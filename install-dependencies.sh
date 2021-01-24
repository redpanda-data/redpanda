#!/bin/bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

echo "installing seastar dependencies"

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
  curl
  libzstd-dev
  libsnappy-dev
  rapidjson-dev
  libxxhash-dev
  python3-venv
  python3-jinja2
)
fedora_deps=(
  curl
  libzstd-static
  libzstd-devel
  snappy-devel
  rapidjson-devel
  xxhash-devel
  python3-virtualenv
  python3-jinja2
)

case "$ID" in
  ubuntu | debian)
    apt-get install -y "${deb_deps[@]}"
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
sysctl -w fs.aio-max-nr=10485760
curl -1sLf "https://raw.githubusercontent.com/vectorizedio/seastar/master/install-dependencies.sh" | bash
