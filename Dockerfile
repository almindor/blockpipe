FROM rust:1.31.0-slim as build

RUN USER=root cargo new --bin /app
WORKDIR /app

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

RUN cargo build --release && \
  rm -rf src

COPY ./ ./

RUN rm -rf ./target/release/blockpipe && \
  cargo build --release

FROM ubuntu:16.04

RUN apt-get update && \
  apt-get install -y libpq-dev && \
  touch .env

COPY --from=build /app/target/release/blockpipe ./

ENTRYPOINT ["/blockpipe"]
