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

/opt/redpanda/bin/kvelldb --ip 0.0.0.0 --workdir /mnt/kvell --node-id $1 --port 33145 --httpport 9092 --peers $2,$3:33145 --peers $4,$5:33145 --cpus 1 2>&1
