#!/usr/bin/env bash
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

set -e

for ip in "$@"; do
  iptables -A INPUT -s $ip -j DROP
  iptables -A OUTPUT -d $ip -j DROP
done
