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
  clang
  cmake
  git
  gnutls-dev
  golang
  libabsl-dev
  libboost-all-dev
  libc-ares-dev
  libcrypto++-dev
  libgssapi-krb5-2
  libkrb5-dev
  liblz4-dev
  libprotobuf-dev
  libprotoc-dev
  libre2-dev
  libsctp-dev
  libsnappy-dev
  libxxhash-dev
  libyaml-cpp-dev
  libzstd-dev
  lld
  ninja-build
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
  abseil-cpp-devel
  boost-devel
  c-ares-devel
  cargo
  clang
  cmake
  compiler-rt
  cryptopp-devel
  git
  gnutls-devel
  golang
  hwloc-devel
  krb5-devel
  libxml2-devel
  libzstd-devel
  lksctp-tools-devel
  lld
  llvm
  lz4-devel
  ninja-build
  numactl-devel
  procps
  protobuf-devel
  python3
  python3-jinja2
  python3-jsonschema
  ragel-devel
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
