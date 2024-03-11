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
#include "base/vassert.h"
#include "ssx/semaphore.h"

namespace raft {

class follower_queue {
public:
    explicit follower_queue(uint32_t);

    follower_queue(follower_queue&&) noexcept = default;
    follower_queue(const follower_queue&) = delete;
    follower_queue& operator=(follower_queue&&) = default;
    follower_queue& operator=(const follower_queue&) = delete;
    ~follower_queue() {
        vassert(is_idle(), "can not remove not idle follower queue");
    }

    ss::future<ssx::semaphore_units> get_append_entries_unit();

    ss::future<> stop();

    bool is_idle() const {
        return _sem->waiters() == 0
               && _sem->available_units() == _max_concurrent_append_entries;
    }

private:
    /**
     * TODO: consider using queue depth control to automatically adjust number
     * of concurrent requests per follower. In general it should be fine to make
     * this static since scaling throughput is usually achieved by increasing
     * partition count. We may need to increase number of concurrent requests
     * per partition to increase throughput when partition number is low and we
     * are fine with increasing latency.
     *
     * Things to consider:
     * - per shard concurrency controll
     * - token-bucket based throughput limitter
     */
    uint32_t _max_concurrent_append_entries;
    std::unique_ptr<ssx::semaphore> _sem;
};

} // namespace raft
