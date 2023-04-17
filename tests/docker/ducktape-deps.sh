#!/bin/bash

set -e

function install_java_client_deps() {
  apt update
  apt install -y \
    build-essential \
    default-jdk \
    git \
    maven \
    cmake \
    curl

  install_protobuf_compiler
}

function install_system_deps() {
  apt-get update
  apt-get install -qq \
    bind9-utils \
    bind9-dnsutils \
    bsdmainutils \
    dmidecode \
    krb5-admin-server \
    krb5-kdc \
    krb5-user \
    iproute2 \
    iptables \
    libatomic1 \
    libyajl-dev \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    net-tools \
    lsof \
    pciutils \
    nodejs \
    npm \
    openssh-server \
    netcat-openbsd \
    sudo \
    llvm \
    python3-pip \
    libzstd-dev \
    pkg-config
}

function install_omb() {
  git -C /opt clone https://github.com/redpanda-data/openmessaging-benchmark.git
  cd /opt/openmessaging-benchmark
  git reset --hard 2674d62ca2b6fd7f22536e924c0df8a8fa21350d
  mvn clean package -DskipTests
}

function install_kafka_tools() {
  for ver in "2.3.1" "2.4.1" "2.5.0" "2.7.0" "3.0.0"; do
    mkdir -p "/opt/kafka-${ver}"
    chmod a+rw "/opt/kafka-${ver}"
    curl -s "$KAFKA_MIRROR/kafka_2.12-${ver}.tgz" | tar xz --strip-components=1 -C "/opt/kafka-${ver}"
  done
  ln -s /opt/kafka-3.0.0/ /opt/kafka-dev
}

function install_librdkafka() {
  mkdir /opt/librdkafka
  curl -SL "https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.0.2.tar.gz" | tar -xz --strip-components=1 -C /opt/librdkafka
  cd /opt/librdkafka
  ./configure
  make -j$(nproc)
  make install
  cd /opt/librdkafka/tests
  make build -j$(nproc)
}

function install_kcat() {
  mkdir /tmp/kcat
  curl -SL "https://github.com/edenhill/kcat/archive/1.7.0.tar.gz" | tar -xz --strip-components=1 -C /tmp/kcat
  cd /tmp/kcat
  ./configure
  make -j$(nproc)
  make install
  ldconfig
}

function install_golang() {
  mkdir -p /usr/local/go/
  if [ $(uname -m) = "aarch64" ]; then
    export ARCHID="arm64"
  else
    export ARCHID="amd64"
  fi
  curl -sSLf --retry 3 --retry-connrefused --retry-delay 2 "https://golang.org/dl/go1.19.2.linux-${ARCHID}.tar.gz" | tar -xz -C /usr/local/go/ --strip 1
}

function install_kaf() {
  go install github.com/birdayz/kaf/cmd/kaf@v0.2.3
  mv /root/go/bin/kaf /usr/local/bin/
}

# Alias so that vtools can be updated asynchronously.
function install_client_swarm() {
  install_rust_tools "$1"
}

function install_rust_tools() {
  dir="$1"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  export PATH="$dir/.cargo/bin:${PATH}"
  pushd /tmp

  git clone https://github.com/redpanda-data/client-swarm.git
  pushd client-swarm
  git reset --hard 63b4cd558203cdd79a69a0893c7435104c10f428
  cargo build --release
  cp target/release/client-swarm /usr/local/bin
  popd
  rm -rf client-swarm

  git clone https://github.com/jcsp/segment_toy.git
  pushd segment_toy
  git reset --hard 3a4b8b37bf1fddd5ff767a13c13e3947476485a8
  cargo build --release
  cp target/release/segments /usr/local/bin
  popd
  rm -rf segment_toy

  popd
  rm -rf $dir/.cargo
  rm -rf $dir/.rustup
}

function install_sarama_examples() {
  git -C /opt clone -b v1.32.0 --single-branch https://github.com/Shopify/sarama.git
  cd /opt/sarama/examples/interceptors
  go mod tidy
  go build
  cd /opt/sarama/examples/http_server
  go mod tidy
  go build
  cd /opt/sarama/examples/consumergroup
  go mod tidy
  go build
  cd /opt/sarama/examples/sasl_scram_client
  go mod tidy
  go build
}

function install_franz_bench() {
  git -C /opt clone -b v1.5.0 --single-branch https://github.com/twmb/franz-go.git
  cd /opt/franz-go
  cd /opt/franz-go/examples/bench
  go mod tidy
  go build
}

function install_kcl() {
  go install github.com/twmb/kcl@v0.8.0
  mv /root/go/bin/kcl /usr/local/bin/
}

function install_kgo_verifier() {
  git -C /opt clone https://github.com/redpanda-data/kgo-verifier.git
  cd /opt/kgo-verifier
  git reset --hard 5b1b6b6b802a30962963cca8b364bee148fee515
  go mod tidy
  make
}

function install_addr2line() {
  mkdir -p /opt/scripts
  curl https://raw.githubusercontent.com/redpanda-data/seastar/2a9504b3238cba4150be59353bf8d0b3a01fe39c/scripts/addr2line.py -o /opt/scripts/addr2line.py
  curl https://raw.githubusercontent.com/redpanda-data/seastar/2a9504b3238cba4150be59353bf8d0b3a01fe39c/scripts/seastar-addr2line -o /opt/scripts/seastar-addr2line
  chmod +x /opt/scripts/seastar-addr2line
}

function install_kafka_streams_examples() {
  git -C /opt clone https://github.com/redpanda-data/kafka-streams-examples.git
  cd /opt/kafka-streams-examples
  git reset --hard 913d08c8351c74ee454b79f8e0c1f48ca9b562a5
  mvn -DskipTests=true clean package
}

function install_arroyo() {
  git -C /opt clone -b 2.5.0 --depth=1 https://github.com/getsentry/arroyo.git
  cd /opt/arroyo
  make install
  python3 -m pip install --force --no-cache-dir -e /opt/arroyo
}

function install_java_test_clients() {
  mvn clean package --batch-mode --file /opt/redpanda-tests/java/e2e-verifiers --define buildDir=/opt/e2e-verifiers
  mvn clean package --batch-mode --file /opt/redpanda-tests/java/verifiers --define buildDir=/opt/verifiers
  mvn clean package --batch-mode --file /opt/redpanda-tests/java/kafka-serde --define buildDir=/opt/kafka-serde
}

function install_go_test_clients() {
  cd /opt/redpanda-tests/go/sarama/produce_test
  go mod tidy
  go build

  cd /opt/redpanda-tests/go/go-kafka-serde
  GOPATH=${HOME}/go make clean all
}

function install_protobuf_compiler() {
  mkdir /tmp/protobuf
  curl -SL "https://vectorized-public.s3.amazonaws.com/dependencies/protobuf-cpp-3.21.8.tar.gz" | tar --no-same-owner -xz --strip-components=1 -C /tmp/protobuf
  cd /tmp/protobuf
  cmake -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=/usr
  make -j$(nproc)
  make install
}

$@
