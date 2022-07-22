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

#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/fwd.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "storage/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

namespace raft {

/// A single-shot class. Utility method with state
/// Use with a lw_shared_ptr like so:
/// auto ptr = ss::make_lw_shared<replicate_entries_stm>(..);
/// return ptr->apply()
///            .then([ptr]{
///                 // wait in background.
///                (void)ptr->wait().finally([ptr]{});
///            });
///
/// Replicate STM implements following algorithm
///
///  Stop conditions:
///    1) leader commit_index > offset of an entry that was replicated
///       to the leader log (SUCCESS)
///    2) term has changed (FAILURE)
///    3) current node is not the leader (FAILURE)
///
///  Algorithm steps:
///    1) Append entry to leader log without flush
///    2) Dispatch append entries RPC calls to followers and in parallel flush
///       entries to leader disk
///    3) Wait for (1),(2) or (3)
///    4) When
///       ->(1) reply with success
///       ->(2) or (3) check if entry with given offset and term exists in log
///         if entry exists reply with success, reply with false otherwise
///
///
///   Wait is realized with condition variable that is only notified when commit
///   index change
///
///
///
///                            N1 (Leader)        +
///                            +-------+          |
///                        +-->| Flush |--------->+     OK
///                        |   +-------+          |     +----(1)----> SUCCESS
///                        |                      |     |
///      N1 (Leader)       |   N2                 |     |
/// +-------------------+  |   +--------+-------+ |     |
/// |Replicate |Append  |--+-->+ Append | Flush |-+---->+
/// +-------------------+  |   +--------+-------+ |     |
///                     |  |                      |     |
///                     |  |   N3                 |     |
///                     |  |   +--------+-------+ |     +-(2)-(3)---> FAILURE
///                     |  +-->| Append | Flush |-+     ERR
///                     |      +--------+-------+ |
///                     |                         +
///                     v               Wait for (1) or (2)
///         Store entry offset & term

class replicate_entries_stm {
public:
    using units_t = std::vector<ssx::semaphore_units>;
    replicate_entries_stm(
      consensus*,
      append_entries_request,
      absl::flat_hash_map<vnode, follower_req_seq>);
    ~replicate_entries_stm();

    /// caller have to pass semaphore units, the apply call will do the
    /// fine grained locking on behalf of the caller
    ss::future<result<replicate_result>>
      apply(std::vector<ssx::semaphore_units>);

    /**
     * Waits for majority of replicas to successfully execute dispatched
     * append_entries_request
     */
    ss::future<result<replicate_result>> wait_for_majority();

    /**
     * Waits for all related background future to finish - required to be called
     * before destorying the stm
     */
    ss::future<> wait_for_shutdown();

private:
    ss::future<append_entries_request> share_request();

    ss::future<> dispatch_one(vnode);
    ss::future<result<append_entries_reply>> dispatch_single_retry(vnode);
    ss::future<result<append_entries_reply>> flush_log();

    ss::future<result<append_entries_reply>>
      send_append_entries_request(vnode, append_entries_request);
    result<replicate_result> process_result(model::offset, model::term_id);
    bool should_skip_follower_request(vnode);
    clock_type::time_point append_entries_timeout();
    /// This append will happen under the lock
    ss::future<result<storage::append_result>> append_to_self();

    result<replicate_result> build_replicate_result() const;

    consensus* _ptr;
    /// we keep a copy around until we finish the retries
    std::unique_ptr<append_entries_request> _req;
    absl::flat_hash_map<vnode, follower_req_seq> _followers_seq;
    ssx::semaphore _share_sem;
    ssx::semaphore _dispatch_sem{0, "raft/repl-dispatch"};
    ss::gate _req_bg;
    ctx_log _ctxlog;
    model::offset _dirty_offset;
    model::offset _initial_committed_offset;
    ss::lw_shared_ptr<std::vector<ssx::semaphore_units>> _units;
    std::optional<result<storage::append_result>> _append_result;
    uint16_t _requests_count = 0;
};

} // namespace raft
