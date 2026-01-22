FROM debian:trixie-slim AS app
ARG target=release

RUN apt-get update && apt-get install -y openssl

WORKDIR /app
COPY ./target/$target/server /app/proxy-server

ENTRYPOINT ["/app/proxy-server"]
EXPOSE 13306