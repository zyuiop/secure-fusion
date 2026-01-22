FROM rust:1.90.0 as build
LABEL authors="zyuiop"
RUN apt-get update && apt-get install -y libssl-dev cmake

WORKDIR /build
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release

FROM debian:trixie-slim AS app

RUN apt-get update && apt-get install -y openssl

WORKDIR /app
COPY --from=build /build/target/release/server /app/proxy-server

ENTRYPOINT ["/app/proxy-server"]
EXPOSE 13306