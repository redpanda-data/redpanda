#!/bin/bash

set -e
set -x

function install_java_client_deps() {
  apt update
  apt install -y \
    build-essential \
    default-jdk \
    git \
    maven
}

function install_system_deps() {
  apt-get update
  apt-get install -qq \
    bind9-utils \
    bind9-dnsutils \
    bsdmainutils \
    curl \
    dmidecode \
    cmake \
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
    python3-pip
}

function install_omb() {
  git -C /opt clone https://github.com/redpanda-data/openmessaging-benchmark.git
  cd /opt/openmessaging-benchmark
  git reset --hard 2674d62ca2b6fd7f22536e924c0df8a8fa21350d
  mvn clean package -DskipTests
}

$@
