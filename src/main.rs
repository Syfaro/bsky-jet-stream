use std::{
    future::IntoFuture,
    io::{Cursor, Read},
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use async_nats::{HeaderMap, ServerAddr};
use axum::{routing::get, Router};
use clap::Parser;
use futures::StreamExt;
use lazy_static::lazy_static;
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, IntCounter, IntCounterVec,
    IntGauge, TextEncoder,
};
use serde::{Deserialize, Serialize};
use tap::TapOptional;
use tokio::{
    net::TcpStream,
    signal::unix::{signal, SignalKind},
    time::interval,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url;
use zstd::{dict::DecoderDictionary, Decoder};

const ZSTD_DICTIONARY: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/zstd/dictionary"));

const REPLAY_WINDOW_US: u64 = 5 * 10u64.pow(6);

lazy_static! {
    static ref RECEIVED_SIZE_COUNTER: IntCounter =
        register_int_counter!("bsky_nats_size_total", "Number of relevant bytes received.")
            .unwrap();
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
    static ref LAST_TIMESTAMP: IntGauge = register_int_gauge!(
        "bsky_nats_last_time_miliseconds",
        "Last received timestamp."
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
struct JetstreamEvent<'a> {
    did: String,
    time_us: u64,
    kind: JetstreamEventKind,
    #[serde(borrow)]
    commit: Option<JetstreamCommit<'a>>,
    identity: Option<&'a serde_json::value::RawValue>,
    account: Option<&'a serde_json::value::RawValue>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum JetstreamEventKind {
    Commit,
    Identity,
    Account,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct JetstreamCommit<'a> {
    rev: String,
    operation: JetstreamCommitOperation,
    collection: String,
    rkey: String,
    #[serde(borrow)]
    record: Option<&'a serde_json::value::RawValue>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JetstreamCommitOperation {
    Create,
    Update,
    Delete,
}

impl JetstreamCommit<'_> {
    fn action(&self) -> &'static str {
        match &self.operation {
            JetstreamCommitOperation::Create => "create",
            JetstreamCommitOperation::Update => "update",
            JetstreamCommitOperation::Delete => "delete",
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct CommitPayload<'a> {
    repo: String,
    rev: String,
    path: String,
    action: String,
    #[serde(borrow)]
    data: Option<&'a serde_json::value::RawValue>,
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

    info!("waiting for tasks to finish");
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

    let (stream, _) = connect_async(url).await?;
    info!("connected to stream");

    let mut pos = cursor;

    let mut commit_ticker = interval(Duration::from_secs(10));

    let mut perf_ticker = interval(Duration::from_secs(1));
    let mut start = Instant::now();
    let mut ops = 0usize;

    let (bytes_tx, bytes_rx) =
        tokio::sync::mpsc::channel::<Result<Vec<u8>, tokio_tungstenite::tungstenite::Error>>(200);

    let (messages_tx, messages_rx) =
        tokio::sync::mpsc::channel::<eyre::Result<TransformedMessage>>(200);
    let messages_stream = ReceiverStream::new(messages_rx);
    let chunked_messages_stream =
        tokio_stream::StreamExt::chunks_timeout(messages_stream, 100, Duration::from_secs(1));
    tokio::pin!(chunked_messages_stream);

    let stream_handle = stream_sender(token, stream, bytes_tx).await;
    let transform_handle = transform_thread(config.nats_stream_name.clone(), bytes_rx, messages_tx);

    loop {
        tokio::select! {
            _ = commit_ticker.tick() => {
                info!(pos, "committing cursor position");
                bucket.put(&config.nats_cursor_key, pos.to_string().into()).await?;
            }

            _ = perf_ticker.tick() => {
                let now = Instant::now();
                let elapsed = now - start;

                let ops_per_s = ops as f64 / elapsed.as_secs_f64();
                debug!(ops_per_s, "calculated operations");

                start = now;
                ops = 0;
            }

            messages = chunked_messages_stream.next() => {
                match messages {
                    Some(messages) => {
                        let len = messages.len();
                        trace!(len, "got chunk of messages");

                        MESSAGES_COUNTER.inc_by(len as u64);
                        ops += len;

                        handle_message_chunk(&js, &mut pos, messages).await?;

                        LAST_TIMESTAMP.set((pos / 1000).try_into().unwrap());
                    }

                    None => {
                        warn!("messages stream ended");
                        break;
                    }
                }
            }
        }
    }

    stream_handle.await?;
    transform_handle
        .join()
        .expect("transform handle should join");

    info!(pos, "loop finished, saving cursor position");
    bucket
        .put(&config.nats_cursor_key, pos.to_string().into())
        .await?;

    Ok(())
}

async fn handle_message_chunk(
    js: &async_nats::jetstream::Context,
    pos: &mut u64,
    messages: Vec<eyre::Result<TransformedMessage>>,
) -> eyre::Result<()> {
    let messages: Vec<TransformedMessage> = messages
        .into_iter()
        .collect::<eyre::Result<Vec<TransformedMessage>>>()?;

    let mut futs = Vec::with_capacity(messages.len());
    for message in messages {
        if let Some((subject, headers, data)) = message.data {
            let fut = js
                .publish_with_headers(subject, headers, data.into())
                .await?
                .into_future();
            futs.push(fut);
        }

        if message.time_us > *pos {
            *pos = message.time_us;
        }
    }

    futures::future::try_join_all(futs).await?;

    Ok(())
}

async fn stream_sender(
    token: CancellationToken,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    tx: tokio::sync::mpsc::Sender<Result<Vec<u8>, tokio_tungstenite::tungstenite::Error>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let stream = stream.take_until(token.cancelled());
        tokio::pin!(stream);

        while let Some(message) = stream.next().await {
            match message {
                Ok(Message::Binary(data)) => {
                    RECEIVED_SIZE_COUNTER.inc_by(data.len() as u64);

                    tx.send(Ok(data)).await.expect("send should always succeed");
                }

                Ok(message) => {
                    warn!("got unexpected message type: {message:?}");
                }

                Err(err) => {
                    tx.send(Err(err)).await.expect("send should always succeed");
                    break;
                }
            }
        }

        warn!("websocket stream ended");
    })
}

fn transform_thread(
    stream_name: String,
    mut rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, tokio_tungstenite::tungstenite::Error>>,
    tx: tokio::sync::mpsc::Sender<eyre::Result<TransformedMessage>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let dict = DecoderDictionary::copy(ZSTD_DICTIONARY);

        while let Some(msg) = rx.blocking_recv() {
            match msg {
                Ok(bytes) => {
                    if let Err(err) =
                        tx.blocking_send(transform_message(&stream_name, &dict, bytes))
                    {
                        error!("could not send transformed message: {err}");
                        break;
                    }
                }

                Err(err) => {
                    error!("got websocket error: {err}");
                    break;
                }
            }
        }

        warn!("transform thread ended");
    })
}

struct TransformedMessage {
    time_us: u64,
    data: Option<(String, HeaderMap, Vec<u8>)>,
}

#[instrument(skip_all, fields(time_us, subject))]
fn transform_message(
    stream_name: &str,
    dict: &DecoderDictionary<'_>,
    data: Vec<u8>,
) -> eyre::Result<TransformedMessage> {
    let cursor = Cursor::new(data);
    let mut decoder = Decoder::with_prepared_dictionary(cursor, dict)?;

    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;

    let event: JetstreamEvent = serde_json::from_slice(&buf)?;

    tracing::Span::current().record("time_us", event.time_us);
    trace!("got event");

    let mut headers = HeaderMap::new();
    headers.insert("Nats-Expected-Stream", stream_name);
    headers.insert("Nats-Msg-Id", event.time_us.to_string());

    let (subject, data): (String, Vec<u8>) = match event.kind {
        JetstreamEventKind::Commit => {
            let Some(commit) = event.commit else {
                warn!("commit event without commit data");
                return Ok(TransformedMessage {
                    time_us: event.time_us,
                    data: None,
                });
            };

            let action = commit.action();

            MESSAGE_KIND_COUNTER.with_label_values(&["commit"]).inc();

            COMMIT_ACTION_COUNTER
                .with_label_values(&[action, &commit.collection])
                .inc();

            trace!("data: {:?}", commit.record);

            let payload = serde_json::to_vec(&CommitPayload {
                repo: event.did,
                rev: commit.rev,
                path: format!("{}/{}", commit.collection, commit.rkey),
                action: action.to_string(),
                data: commit.record,
            })?;

            (
                format!("bsky.ingest.commit.{action}.{}", commit.collection),
                payload,
            )
        }
        JetstreamEventKind::Identity => {
            MESSAGE_KIND_COUNTER.with_label_values(&["identity"]).inc();

            (
                "bsky.ingest.identity".into(),
                serde_json::to_vec(&event.identity)?,
            )
        }
        JetstreamEventKind::Account => {
            MESSAGE_KIND_COUNTER.with_label_values(&["account"]).inc();

            (
                "bsky.ingest.account".into(),
                serde_json::to_vec(&event.account)?,
            )
        }
    };

    tracing::Span::current().record("subject", &subject);
    trace!("finished processing event");

    Ok(TransformedMessage {
        time_us: event.time_us,
        data: Some((subject, headers, data)),
    })
}
