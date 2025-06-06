/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "raft/consensus.h"
#include "raft/types.h"

namespace raft::testing_details {
class consensus_accessor {
public:
    static follower_stats& follower_stats(ss::lw_shared_ptr<consensus> c) {
        return c->_fstats;
    }

    static void dispatch_recovery(
      ss::lw_shared_ptr<consensus> c, follower_index_metadata& f) {
        c->dispatch_recovery(f);
    }
};
} // namespace raft::testing_details
