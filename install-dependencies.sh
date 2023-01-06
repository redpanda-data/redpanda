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
  ccache
  clang
  curl
  git
  golang-go
  libkrb5-dev
  libgssapi-krb5-2
  libsnappy-dev
  libxxhash-dev
  libzstd-dev
  llvm
  lld
  pkg-config
  procps
  python3-jinja2
  python3-venv
  rapidjson-dev
  zip
  unzip
  libre2-dev
)
fedora_deps=(
  ccache
  clang
  curl
  git
  golang
  krb5-libs
  krb5-devel
  libzstd-devel
  libzstd-static
  llvm
  lld
  pkg-config
  procps
  python3-jinja2
  python3-virtualenv
  rapidjson-devel
  snappy-devel
  which
  xxhash-devel
  xz
  zip
  unzip
  re2-devel
)
arch_deps=(
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
# needed for unit tests
sysctl -w fs.aio-max-nr=10485760 || true
curl -1sLf "https://raw.githubusercontent.com/redpanda-data/seastar/master/install-dependencies.sh" | bash
