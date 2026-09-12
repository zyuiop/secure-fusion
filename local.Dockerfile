FROM archlinux:base AS app
ARG target=release

RUN pacman -Syu --noconfirm openssl

WORKDIR /app
COPY ./target/$target/server /app/proxy-server

ENTRYPOINT ["/app/proxy-server"]
EXPOSE 13306