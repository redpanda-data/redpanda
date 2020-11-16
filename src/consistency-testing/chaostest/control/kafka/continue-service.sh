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

ps aux | egrep [Q]uorumPeerMain | awk '{print $2}' | xargs -r kill -CONT
ps aux | egrep [k]afka.Kafka | awk '{print $2}' | xargs -r kill -CONT
