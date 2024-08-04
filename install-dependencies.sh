#!/bin/bash
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
set -e

echo "installing redpanda toolchain"

if [[ $EUID -ne 0 ]]; then
  echo "This script should be run as root."
  exit 1
fi
if [ -f "/etc/os-release" ]; then
  . /etc/os-release
elif [ -f "/etc/arch-release" ]; then
  export ID=arch
else
  echo "/etc/os-release missing."
  exit 1
fi

deb_deps=(
  cargo
  ccache
  clang
  clang-tidy
  cmake
  git
  golang
  libarrow-dev
  libboost-all-dev
  libc-ares-dev
  libgssapi-krb5-2
  libkrb5-dev
  liblz4-dev
  libparquet-dev
  libprotobuf-dev
  libprotoc-dev
  libre2-dev
  libsctp-dev
  libsnappy-dev
  libssl-dev
  libxxhash-dev
  libyaml-cpp-dev
  libzstd-dev
  lld
  ninja-build
  openssl
  protobuf-compiler
  python3
  python3-jinja2
  python3-jsonschema
  ragel
  systemtap-sdt-dev
  valgrind
  xfslibs-dev
)
fedora_deps=(
  boost-devel
  c-ares-devel
  cargo
  ccache
  clang
  clang-tools-extra
  cmake
  compiler-rt
  git
  golang
  hwloc-devel
  krb5-devel
  libarrow-devel
  libxml2-devel
  libzstd-devel
  lksctp-tools-devel
  lld
  llvm
  lz4-devel
  lz4-static
  ninja-build
  numactl-devel
  openssl
  openssl-devel
  parquet-libs-devel
  procps
  protobuf-devel
  python3
  python3-jinja2
  python3-jsonschema
  ragel
  re2-devel
  rust
  snappy-devel
  systemtap-sdt-devel
  valgrind-devel
  xfsprogs-devel
  xxhash-devel
  yaml-cpp-devel
  zlib-devel
)
arch_deps=(
  ccache
  clang
  curl
  git
  go
  lld
  llvm
  openssl
  pkg-config
  procps
  python-jinja
  python-virtualenv
  rapidjson
  rust
  snappy
  unzip
  which
  xxhash
  xz
  zip
  zstd
)

case "$ID" in
  ubuntu | debian | pop)
    export DEBIAN_FRONTEND=noninteractive
    apt update
    apt install -y -V ca-certificates lsb-release wget
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt update
    apt-get install -y "${deb_deps[@]}"
    if [[ $CLEAN_PKG_CACHE == true ]]; then
      rm -rf /var/lib/apt/lists/*
    fi
    ;;
  fedora)
    dnf install -y "${fedora_deps[@]}"
    if [[ $CLEAN_PKG_CACHE == true ]]; then
      dnf clean all
    fi
    ;;
  arch | manjaro)
    pacman -Sy --needed --noconfirm "${arch_deps[@]}"
    if [[ $CLEAN_PKG_CACHE == true ]]; then
      pacman -Sc
    fi
    ;;
  *)
    echo "Please help us make the script better by sending patches with your OS $ID"
    exit 1
    ;;
esac
