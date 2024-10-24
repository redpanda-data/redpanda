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

#include "base/outcome.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/fwd.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "ssx/semaphore.h"
#include "storage/types.h"
#include "utils/mutex.h"

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
///                (void)ptr->wait_for_majority().finally([ptr]{});
///            });
///
/// Replicate STM implements following algorithm
///
///    1) Append entry to leader log without flush
///    ->   wait for (1) to finish successfully.
///    2) Dispatch append entries RPC calls to followers and in parallel.
///    Entries are flushed if write caching is disabled or the caller
///    decides to force flush.
///    3) Rely on the replication monitor to wait until the entries are
///    successfully replicated or truncated. See replication_monitor for
///    details.
///
///
///                            N1 (Leader)        +
///                            +-------+          |
///                        +-->| Flush |--------->+
///                        |   +-------+          |              +->SUCCESS
///                        |                      |            OK|
///      N1 (Leader)       |   N2                 |              |
/// +-------------------+  |   +--------+-------+ |     +---------------------+
/// |Replicate |Append  |--+-->+ Append | Flush |-+---->| replication_monitor |
/// +-------------------+  |   +--------+-------+ |     +---------------------+
///                     |  |                      |              |
///                     |  |   N3                 |              |
///                     |  |   +--------+-------+ |              +--> FAILURE
///                     |  +-->| Append | Flush |-+
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
    ss::future<model::record_batch_reader> share_batches();

    ss::future<> dispatch_one(vnode);
    ss::future<> dispatch_remote_append_entries(vnode);
    ss::future<> flush_log();

    ss::future<result<append_entries_reply>>
      send_append_entries_request(vnode, model::record_batch_reader);

    result<replicate_result>
      process_result(raft::errc, model::offset, model::term_id);
    bool should_skip_follower_request(vnode);
    clock_type::time_point append_entries_timeout();
    /// This append will happen under the lock
    ss::future<result<storage::append_result>> append_to_self();

    result<replicate_result> build_replicate_result() const;

    consensus* _ptr;
    /// we keep a copy around until we finish the retries
    protocol_metadata _meta;
    flush_after_append _is_flush_required;
    std::optional<model::record_batch_reader> _batches;
    absl::flat_hash_map<vnode, follower_req_seq> _followers_seq;
    absl::flat_hash_map<vnode, consensus::inflight_appends_guard>
      _inflight_appends;
    mutex _share_mutex{"replicate_entries_stm::share"};
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
