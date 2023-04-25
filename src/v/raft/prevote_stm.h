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

#include "outcome.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "raft/vote_stm.h"
#include "rpc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>

#include <absl/container/flat_hash_map.h>

#include <variant>
#include <vector>

namespace raft {

// based on raft dissertation 9.6 - preventing disruptions when a server
// rejoins the cluster. We increment candidate's term only after we learn
// that a majority on the cluster are willing to vote for it (their log is
// up to date or behind the candidate's log)
class prevote_stm {
public:
    prevote_stm(consensus*);
    ~prevote_stm();

    ss::future<bool> prevote(bool leadership_transfer);
    ss::future<> wait();

private:
    struct vmeta {
        bool _is_ok = false;
        bool _is_failed = false;
        bool _is_pending = true;
    };

    ss::future<bool> do_prevote();
    ss::future<> dispatch_prevote(vnode);
    ss::future<result<vote_reply>> do_dispatch_prevote(vnode);
    ss::future<> process_reply(vnode n, ss::future<result<vote_reply>> f);
    ss::future<> process_replies();
    // args
    consensus* _ptr;
    // make sure to always make a copy; never move() this struct
    vote_request _req;
    bool _success = false;
    // for sequentiality/progress
    ssx::semaphore _sem;
    std::optional<raft::group_configuration> _config;
    rpc::timeout_spec _prevote_timeout;
    // for safety to wait for all bg ops
    ss::gate _vote_bg;
    absl::flat_hash_map<vnode, vmeta> _replies;
    ctx_log _ctxlog;
};

} // namespace raft
