/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "raft/types.h"
#include "random/simple_time_jitter.h"

namespace raft {

using timeout_jitter
  = simple_time_jitter<raft::clock_type, raft::duration_type>;

} // namespace raft
