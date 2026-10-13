FROM rust:1-slim-bookworm AS build
WORKDIR /app

# Build the dependencies first so they're cached when only the source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src src
COPY ui ui
COPY config.example.toml ./
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/points-miner /usr/local/bin/points-miner

# config.toml, miner.db, web_token and cookies/ all live in /data.
VOLUME /data
WORKDIR /data
ENV POINTS_MINER_WEB_HOST=0.0.0.0
EXPOSE 5000
# The miner shuts down cleanly (and prints its session report) on Ctrl+C.
STOPSIGNAL SIGINT
CMD ["points-miner", "/data/config.toml"]
