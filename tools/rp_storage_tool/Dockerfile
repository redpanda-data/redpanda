FROM rust:1.79.0-slim-bullseye AS builder

# If you are building on a workstation and copying the binary to a remote machine, and your
# workstation doesn't happen to run the same linux distro as the remote machine, it is
# useful to build a statically linked binary that will work on any distro.
# That's what the next 2 lines are for
RUN apt update && apt install -y musl-tools
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /src
COPY . /src/
RUN cargo build --release --target=x86_64-unknown-linux-musl

FROM rust:1.79.0-slim-bullseye

WORKDIR /usr/local/bin
COPY --from=builder /src/target/release/rp-storage-tool .

ENTRYPOINT ["rp-storage-tool"]
CMD ["--help"]
