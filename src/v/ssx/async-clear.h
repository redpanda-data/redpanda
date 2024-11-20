/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "container/chunked_hash_map.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace ssx {

/**
 * For sufficiently large containers where the element destructors
 * do some work, it is problematic to spend a long time clearing
 * the container without yielding to the scheduler.
 *
 * This function yields every so often while erasing all elements
 * in a container.
 *
 * The type is specific to absl::flat_hash_map to avoid accidentially
 * using this function on types where repeatedly erasing from the start is
 * very expensive, like std::vector.
 */
template<typename Key, typename Value, typename Hash, typename EqualTo>
ss::future<>
async_clear(chunked_hash_map<Key, Value, Hash, EqualTo>& container) {
    // Below threshold_size, just call clear().
    // Otherwise yield to the scheduler every `threshold_size` elements
    constexpr size_t threshold_size = 100;

    if (container.size() < threshold_size) {
        container.clear();
        co_return;
    }

    size_t i = 0;
    auto it = container.begin();
    while (it != container.end()) {
        it = container.erase(it);

        if (++i % threshold_size == 0) {
            co_await ss::coroutine::maybe_yield();
            // incase the iterator got invaliated between scheduling
            // points.
            it = container.begin();
        }
    }
    vassert(
      container.empty(), "Container is non empty, size: {}", container.size());
}

} // namespace ssx
