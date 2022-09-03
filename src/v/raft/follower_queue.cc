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

#include "raft/follower_queue.h"

#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>

namespace raft {

follower_queue::follower_queue(uint32_t max_concurrent_append_entries)
  : _max_concurrent_append_entries(max_concurrent_append_entries)
  , _sem(std::make_unique<ssx::semaphore>(
      _max_concurrent_append_entries, "raft/follow")) {}

ss::future<ssx::semaphore_units> follower_queue::get_append_entries_unit() {
    co_return co_await ss::get_units(*_sem, 1);
}

} // namespace raft
