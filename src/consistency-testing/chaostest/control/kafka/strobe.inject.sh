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

echo "inject" >>/home/ubuntu/strobe.inject.log

curl 127.0.0.1:9094/inject?delta_ms=$1\&period_ms=$2
