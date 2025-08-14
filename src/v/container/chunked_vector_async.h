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

#include "container/chunked_vector.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

// Async helpers for chunked_vector.
// Not in the main header to avoid pulling in all of seastar if you just need
// chunked_vector.

/**
 * A futurized version of std::fill optimized for fragmented vector. It is
 * futurized to allow for large vectors to be filled without incurring reactor
 * stalls. It is optimized by circumventing the indexing indirection incurred by
 * using the fragmented vector interface directly.
 */
template<typename T>
inline seastar::future<>
chunked_vector_fill_async(chunked_vector<T>& vec, const T& value) {
    auto remaining = vec._size;
    for (auto& frag : vec._frags) {
        const auto n = std::min(frag.size(), remaining);
        if (n == 0) {
            break;
        }
        std::fill_n(frag.begin(), n, value);
        remaining -= n;
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vassert(
      remaining == 0,
      "fragmented vector inconsistency filling remaining {} size {} cap {} "
      "nfrags {}",
      remaining,
      vec._size,
      vec._capacity,
      vec._frags.size());
}

/**
 * A futurized version of chunked_vector::clear that allows clearing a large
 * vector without incurring a reactor stall.
 */
template<typename T>
inline seastar::future<> chunked_vector_clear_async(chunked_vector<T>& vec) {
    while (!vec._frags.empty()) {
        vec._frags.pop_back();
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vec.clear();
}
