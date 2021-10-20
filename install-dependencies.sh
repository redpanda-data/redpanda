#!/bin/bash
# Copyright 2020 Vectorized, Inc.
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
  xz
  zip
)
fedora_deps=(
  ccache
  clang
  curl
  git
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
  xxhash-devel
  xz
  zip
)

case "$ID" in
  ubuntu | debian)
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${deb_deps[@]}"
    ;;
  fedora)
    dnf install -y "${fedora_deps[@]}"
    ;;
  *)
    echo "Please help us make the script better by sending patches with your OS $ID"
    exit 1
    ;;
esac
# needed for unit tests
sysctl -w fs.aio-max-nr=10485760 || true
curl -1sLf "https://raw.githubusercontent.com/vectorizedio/seastar/master/install-dependencies.sh" | bash

set -e

mkdir -p /opt/
rm -rf /opt/depot_tools
cd /opt
git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
cd /opt/depot_tools
git checkout ecdc362593cfee1ade115ead7be6c3e96b2e7384
mkdir /opt/depot_tools/installed-gn

# Download gn (git_revision:e0c476ffc83dc10897cb90b45c03ae2539352c5c)
case $(uname -i) in
  x86_64)
    link_for_gn="https://chrome-infra-packages.appspot.com/dl/gn/gn/linux-amd64/+/COENCtFXQmybPdz3KQBjmuSaCK4qdzmzwuCJHQKxovEC"
    ;;
  *)
    link_for_gn="https://chrome-infra-packages.appspot.com/dl/gn/gn/linux-arm64/+/QckR7eHEDkvAVMi-h0_7w4D38fWeo-wdfI7Nfxy3jfEC"
    ;;
esac

curl -L $link_for_gn --output /opt/depot_tools/gn_zip
unzip -d /opt/depot_tools/installed-gn/ /opt/depot_tools/gn_zip
chmod -R 777 /opt/depot_tools
