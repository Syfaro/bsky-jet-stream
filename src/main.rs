use std::{io::Cursor, net::SocketAddr, path::PathBuf, time::Duration};

use async_nats::{HeaderMap, ServerAddr};
use axum::{routing::get, Router};
use clap::Parser;
use futures::StreamExt;
use lazy_static::lazy_static;
use prometheus::{
    register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec, TextEncoder,
};
use serde::{Deserialize, Serialize};
use tap::TapOptional;
use tokio::signal::unix::{signal, SignalKind};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;
use zstd::{dict::DecoderDictionary, Decoder};

const ZSTD_DICTIONARY: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/zstd/dictionary"));

const REPLAY_WINDOW_US: u64 = 5 * 10u64.pow(6);

lazy_static! {
    static ref MESSAGES_COUNTER: IntCounter =
        register_int_counter!("bsky_nats_messages_total", "Number of received messages").unwrap();
    static ref MESSAGE_KIND_COUNTER: IntCounterVec = register_int_counter_vec!(
        "bsky_nats_message_kinds_total",
        "Number of received messages by kind",
        &["kind"]
    )
    .unwrap();
    static ref COMMIT_ACTION_COUNTER: IntCounterVec = register_int_counter_vec!(
        "bsky_nats_repo_commit_actions_total",
        "Number of received commit actions",
        &["action", "collection"]
    )
    .unwrap();
}

#[derive(Debug, Parser)]
struct Config {
    #[clap(
        long,
        env,
        default_value = "wss://jetstream1.us-east.bsky.network/subscribe"
    )]
    jetstream_endpoint: String,

    #[clap(long, env, default_value = "nats://127.0.0.1:4222")]
    nats_url: ServerAddr,
    #[clap(long, env, conflicts_with_all = ["nats_credentials", "nats_credentials_file"])]
    nats_nkey: Option<String>,
    #[clap(long, env, conflicts_with_all = ["nats_nkey", "nats_credentials_file"])]
    nats_credentials: Option<String>,
    #[clap(long, env, conflicts_with_all = ["nats_nkey", "nats_credentials"])]
    nats_credentials_file: Option<PathBuf>,

    #[clap(long, env, default_value = "bsky")]
    nats_bucket_name: String,
    #[clap(long, env, default_value = "jetstream-cursor")]
    nats_cursor_key: String,
    #[clap(long, env, default_value = "bsky-ingest")]
    nats_stream_name: String,

    #[clap(long, env, default_value = "127.0.0.1:8080")]
    metrics_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JetstreamEvent {
    did: String,
    time_us: u64,
    #[serde(flatten)]
    data: JetstreamEventData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JetstreamEventData {
    Commit { commit: JetstreamCommit },
    Identity { identity: JetstreamIdentity },
    Account { account: JetstreamAccount },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JetstreamCommit {
    rev: String,
    collection: String,
    rkey: String,
    #[serde(flatten)]
    data: JetstreamCommitData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum JetstreamCommitData {
    Create {
        record: serde_json::Value,
        cid: String,
    },
    Update {
        record: serde_json::Value,
        cid: String,
    },
    Delete,
}

impl JetstreamCommit {
    fn action(&self) -> &'static str {
        match &self.data {
            JetstreamCommitData::Create { .. } => "create",
            JetstreamCommitData::Update { .. } => "update",
            JetstreamCommitData::Delete => "delete",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JetstreamIdentity {
    did: String,
    handle: Option<String>,
    seq: u64,
    time: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JetstreamAccount {
    active: bool,
    did: String,
    seq: u64,
    time: chrono::DateTime<chrono::Utc>,
    status: Option<AccountStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum AccountStatus {
    Deactivated,
    Deleted,
    Suspended,
    TakenDown,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    let token = CancellationToken::new();

    let connect_options = if let Some(nats_nkey) = &config.nats_nkey {
        async_nats::ConnectOptions::with_nkey(nats_nkey.to_owned())
    } else if let Some(nats_credentials) = &config.nats_credentials {
        async_nats::ConnectOptions::with_credentials(nats_credentials)?
    } else if let Some(nats_credentials_file) = &config.nats_credentials_file {
        async_nats::ConnectOptions::with_credentials_file(nats_credentials_file).await?
    } else {
        async_nats::ConnectOptions::default()
    };

    let client = connect_options.connect(&config.nats_url).await?;
    let js = async_nats::jetstream::new(client);

    if js.get_key_value(&config.nats_bucket_name).await.is_err() {
        eyre::bail!("missing nats key value bucket: {}", config.nats_bucket_name);
    }

    if js.get_stream(&config.nats_stream_name).await.is_err() {
        eyre::bail!("missing nats stream: {}", config.nats_stream_name);
    }

    let metrics_handle = tokio::spawn(metrics_server(token.clone(), config.metrics_addr));
    let process_handle = tokio::spawn(process(token.clone(), js, config));

    let mut sig = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("got ctrl+c, shutting down");
            token.cancel();
        }

        _ = sig.recv() => {
            info!("got terminate, shutting down");
            token.cancel();
        }
    }

    debug!("waiting for tasks to finish");
    metrics_handle.await??;
    process_handle.await?;

    info!("goodbye!");

    Ok(())
}

async fn metrics_server(token: CancellationToken, host: SocketAddr) -> eyre::Result<()> {
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route(
            "/metrics",
            get(|| async {
                let encoder = TextEncoder::new();
                let metric_families = prometheus::gather();

                encoder
                    .encode_to_string(&metric_families)
                    .expect("prometheus encoder encode should always work")
            }),
        );

    let listener = tokio::net::TcpListener::bind(host).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(token.cancelled_owned())
        .await?;

    Ok(())
}

async fn process(token: CancellationToken, js: async_nats::jetstream::Context, config: Config) {
    loop {
        tokio::select! {
            biased;

            _ = token.cancelled() => {
                info!("shutting down");
                break;
            }

            res = start_jetstream(token.clone(), &config, js.clone()) => {
                match res {
                    Ok(()) => warn!("jetstream returned early, retrying"),
                    Err(err) => error!("jetstream error: {err}"),
                };

                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn start_jetstream(
    token: CancellationToken,
    config: &Config,
    js: async_nats::jetstream::Context,
) -> eyre::Result<()> {
    info!("initializing jetstream connection");

    let dict = DecoderDictionary::copy(ZSTD_DICTIONARY);

    let bucket = js.get_key_value(&config.nats_bucket_name).await?;

    let cursor = if let Some(cursor) = bucket.get(&config.nats_cursor_key).await? {
        String::from_utf8(cursor.to_vec())
            .ok()
            .and_then(|str| str.parse::<u64>().ok())
            .tap_some(|cursor| debug!(cursor, "got stored cursor"))
            .map(|cursor| cursor.saturating_sub(REPLAY_WINDOW_US))
            .unwrap_or(0)
    } else {
        0
    };
    debug!(cursor, "set cursor");

    let url = Url::parse_with_params(
        &config.jetstream_endpoint,
        &[
            ("compress", "true".to_string()),
            ("cursor", cursor.to_string()),
        ],
    )?;
    debug!(%url, "built final connection url");

    let (mut stream, _) = connect_async(url).await?;
    info!("connected to stream");

    let mut pos = cursor;

    let mut ticker = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!(pos, "cancelled, committing cursor position and stopping");
                bucket.put(&config.nats_cursor_key, pos.to_string().into()).await?;
                break;
            }

            _ = ticker.tick() => {
                debug!(pos, "committing cursor position");
                bucket.put(&config.nats_cursor_key, pos.to_string().into()).await?;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(message)) => {
                        MESSAGES_COUNTER.inc();

                        match message {
                            Message::Binary(data) => {
                                let message = transform_message(&config.nats_stream_name, &dict, data)?;

                                pos = message.time_us;

                                if let Some((subject, headers, data)) = message.data {
                                    js.publish_with_headers(subject, headers, data.into()).await?.await?;
                                }
                            }

                            message => {
                                warn!("got unexpected message type: {message:?}");
                            }
                        }
                    }

                    Some(Err(err)) => {
                        error!("stream error: {err}");
                        break;
                    }

                    None => {
                        warn!("stream ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

struct TransformedMessage {
    time_us: u64,
    data: Option<(String, HeaderMap, Vec<u8>)>,
}

#[instrument(skip_all)]
fn transform_message(
    stream_name: &str,
    dict: &DecoderDictionary<'_>,
    data: Vec<u8>,
) -> eyre::Result<TransformedMessage> {
    let cursor = Cursor::new(data);
    let decoder = Decoder::with_prepared_dictionary(cursor, dict)?;
    let event = serde_json::from_reader::<_, JetstreamEvent>(decoder)?;

    trace!(time_us = event.time_us, "got event: {event:?}");

    let mut headers = HeaderMap::new();
    headers.insert("Nats-Expected-Stream", stream_name);
    headers.insert("Nats-Msg-Id", event.time_us.to_string());

    let (subject, data): (String, Vec<u8>) = match event.data {
        JetstreamEventData::Commit { commit } => {
            let action = commit.action();

            MESSAGE_KIND_COUNTER.with_label_values(&["commit"]).inc();

            COMMIT_ACTION_COUNTER
                .with_label_values(&[action, &commit.collection])
                .inc();

            let value = match &commit.data {
                JetstreamCommitData::Create { record, .. }
                | JetstreamCommitData::Update { record, .. } => serde_json::to_value(record)?,
                JetstreamCommitData::Delete => serde_json::Value::Null,
            };

            let payload = serde_json::json!({
                "repo": event.did,
                "path": format!("{}/{}", commit.collection, commit.rkey),
                "action": action,
                "data": value,
            });

            (
                format!("bsky.ingest.commit.{action}.{}", commit.collection),
                serde_json::to_vec(&payload)?,
            )
        }
        JetstreamEventData::Identity { identity } => {
            MESSAGE_KIND_COUNTER.with_label_values(&["identity"]).inc();

            (
                "bsky.ingest.identity".into(),
                serde_json::to_vec(&identity)?,
            )
        }
        JetstreamEventData::Account { account } => {
            MESSAGE_KIND_COUNTER.with_label_values(&["account"]).inc();

            ("bsky.ingest.account".into(), serde_json::to_vec(&account)?)
        }
    };

    Ok(TransformedMessage {
        time_us: event.time_us,
        data: Some((subject, headers, data)),
    })
}
