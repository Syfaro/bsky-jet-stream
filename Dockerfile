FROM ubuntu:22.04
EXPOSE 8080
COPY ./bsky-jet-stream /bin/bsky-jet-stream
CMD ["/bin/bsky-jet-stream"]
