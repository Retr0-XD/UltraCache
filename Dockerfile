FROM rust:1.75 AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN useradd -r -u 10001 ultracache
COPY --from=builder /app/target/release/ultracache /usr/local/bin/ultracache

EXPOSE 6379
USER 10001

ENTRYPOINT ["ultracache"]
