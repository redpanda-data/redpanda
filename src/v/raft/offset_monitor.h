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
#include "model/timeout_clock.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

#include <absl/container/btree_map.h>

namespace raft {

/**
 * Offset monitor.
 *
 * Utility for manging waiters based on a threshold offset. Supports multiple
 * waiters on the same offset, as well as timeout and abort source methods of
 * aborting a wait.
 */
class offset_monitor {
public:
    /**
     * Exisiting waiters receive wait_aborted exception.
     */
    void stop();

    /**
     * Wait until at least the given offset has been notified, a timeout
     * occurs, or an abort has been requested.
     */
    ss::future<> wait(
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    /**
     * Returns true if there are no waiters.
     */
    bool empty() const { return _waiters.empty(); }

    /**
     * Notify waiters of an offset.
     */
    void notify(model::offset);

private:
    struct waiter {
        offset_monitor* mon;
        ss::promise<> done;
        ss::timer<model::timeout_clock> timer;
        ss::abort_source::subscription sub;

        waiter(
          offset_monitor*,
          model::timeout_clock::time_point,
          std::optional<std::reference_wrapper<ss::abort_source>>);

        void handle_abort(bool is_timeout);
    };

    friend waiter;

    using waiters_type
      = absl::btree_multimap<model::offset, std::unique_ptr<waiter>>;

    waiters_type _waiters;
    model::offset _last_applied;
};

} // namespace raft
