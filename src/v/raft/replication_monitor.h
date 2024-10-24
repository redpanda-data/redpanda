/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "storage/types.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

namespace raft {

enum class errc : int16_t;
class consensus;

/**
 * This is a raft internal utility to wait for offsets to be
 * committed/replicated/truncated. This is not intended for use
 * by other subsystems, for this usecase check event_manager.
 *
 * This class is hooked up with consensus for the following event
 * updates
 *   1. offset committed
 *   2. offset majority replicated (without flush)
 *
 * Internally this class maintains a list of waiters for these events.
 * The main work loop that notifies these waiters runs out side of
 * consensus op lock. The waiters are also notified of the truncation
 * event if the desired offset is truncated in case of a leadership change.
 */
class replication_monitor {
public:
    explicit replication_monitor(consensus* r);
    ~replication_monitor() = default;
    replication_monitor(replication_monitor&) = delete;
    replication_monitor(replication_monitor&&) = delete;
    replication_monitor& operator=(replication_monitor&) = delete;
    replication_monitor& operator=(replication_monitor&&) = delete;

    ss::future<> stop();

    /**
     * Future resolved when the locally appended result is raft
     * committed/truncated.
     */
    ss::future<errc> wait_until_committed(storage::append_result);

    /**
     * Future resolved when the locally appended result is replicated on a
     * majority/truncated.
     */
    ss::future<errc> wait_until_majority_replicated(storage::append_result);

    // Event hooks from consensus.
    void notify_replicated();
    void notify_committed();

private:
    enum wait_type : int8_t { commit, majority_replication };
    struct waiter {
        waiter(
          replication_monitor*, storage::append_result, wait_type) noexcept;
        ~waiter() noexcept;
        ss::promise<errc> done;
        replication_monitor* monitor;
        storage::append_result append_info;
        wait_type type;
    };

    ss::future<> do_notify_replicated();
    ss::future<> do_notify_committed();

    ss::future<errc> do_wait_until(storage::append_result, wait_type);

    bool is_append_replicated(const storage::append_result&) const;
    std::optional<errc>
    is_append_committed_or_truncated(const storage::append_result&) const;

    consensus* _raft;
    // Key is the base offset of the append results. So all the waiters
    // are sorted by the base offsets of the results.
    using waiters_type
      = absl::btree_multimap<model::offset, ss::lw_shared_ptr<waiter>>;
    waiters_type _waiters;
    ss::gate _gate;

    ss::condition_variable _replicated_event_cv;
    ss::condition_variable _committed_event_cv;
    // Tracks the number of waiters of type majority_replication in
    // the waiters map. This is an optimization to avoid looping
    // through all waiters in the map if there are no waiters
    // of the matching type. majority replication index may move
    // faster than committed index, so if there are no waiters
    // for majority replication, it is wasteful to do the extra work.
    size_t _pending_majority_replication_waiters{0};

public:
    friend std::ostream& operator<<(std::ostream&, const replication_monitor&);
    friend std::ostream& operator<<(std::ostream&, const waiter&);
    friend std::ostream& operator<<(std::ostream&, const wait_type&);
};

} // namespace raft
