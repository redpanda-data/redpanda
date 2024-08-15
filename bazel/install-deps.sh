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
  autoconf
  automake
  autopoint
  bison
  git
  libtool
  make
  pkgconf
  ragel
  valgrind
  xfslibs-dev
)

fedora_deps=(
  autoconf
  automake
  bison
  gettext-devel
  git
  libtool
  perl
  ragel
  valgrind-devel
  xfsprogs-devel
  xorg-x11-util-macros
)

case "$ID" in
  ubuntu | debian | pop)
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${deb_deps[@]}"
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
  *)
    echo "Please help us make the script better by sending patches with your OS $ID"
    exit 1
    ;;
esac
