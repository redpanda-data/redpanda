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
  cmake
  ninja-build
  clang
  lld
  git
  libboost-all-dev
  libc-ares-dev
  libcrypto++-dev
  liblz4-dev
  gnutls-dev
  libsctp-dev
  libyaml-cpp-dev
  ragel
  valgrind
  libsnappy-dev
  libabsl-dev
  libxxhash-dev
  libzstd-dev
  libprotobuf-dev
  libprotoc-dev
  protobuf-compiler
  python3-jsonschema
  python3-jinja2
  xfslibs-dev
  libre2-dev
  systemtap-sdt-dev
  libkrb5-dev
  libgssapi-krb5-2
  golang
  python3
)
fedora_deps=(
  cargo
  rust
  cmake
  ninja-build
  clang
  compiler-rt
  llvm
  lld
  git
  boost-devel
  c-ares-devel
  cryptopp-devel
  lz4-devel
  gnutls-devel
  hwloc-devel
  lksctp-tools-devel
  numactl-devel
  yaml-cpp-devel
  ragel-devel
  valgrind-devel
  xfsprogs-devel
  systemtap-sdt-devel
  snappy-devel
  abseil-cpp-devel
  zlib-devel
  xxhash-devel
  libzstd-devel
  protobuf-devel
  libxml2-devel
  re2-devel
  krb5-devel
  python3-jsonschema
  python3-jinja2
  golang
  python3
  procps
)
arch_deps=(
  rust
  ccache
  clang
  curl
  git
  go
  zstd
  llvm
  lld
  pkg-config
  procps
  python-jinja
  python-virtualenv
  rapidjson
  snappy
  which
  xxhash
  xz
  zip
  unzip
)

case "$ID" in
  ubuntu | debian | pop)
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${deb_deps[@]}"
    ;;
  fedora)
    dnf install -y "${fedora_deps[@]}"
    ;;
  arch | manjaro)
    pacman -Sy --needed --noconfirm "${arch_deps[@]}"
    ;;
  *)
    echo "Please help us make the script better by sending patches with your OS $ID"
    exit 1
    ;;
esac
