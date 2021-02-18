/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/topic_updates_dispatcher.h"
#include "raft/mux_state_machine.h"

namespace cluster {

// single instance
using controller_stm = raft::mux_state_machine<topic_updates_dispatcher>;

static constexpr ss::shard_id controller_stm_shard = 0;

} // namespace cluster