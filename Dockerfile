FROM rust:1.31.0 as build

RUN USER=root cargo new --bin /build_app
WORKDIR /build_app

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

RUN cargo build --release && \
  rm -rf src

COPY ./ ./

RUN find ./target/release | grep blockpipe | xargs rm -rf && \
  cargo build --release

FROM ubuntu:18.04

RUN apt-get update && \
  apt-get install -y libpq-dev libssl-dev && \
  touch .env

WORKDIR /app

COPY --from=build /build_app/target/release/blockpipe ./

ENTRYPOINT ["/app/blockpipe"]
