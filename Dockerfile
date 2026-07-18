# Build stage
FROM rust:1.85.0 AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN useradd -r -u 10001 ultracache
COPY --from=builder /app/target/release/ultracache /usr/local/bin/ultracache

EXPOSE 6379
USER 10001

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/6379 && printf "PING\r\n" >&3 && grep -q "+PONG" <&3' || exit 1

ENTRYPOINT ["ultracache"]
