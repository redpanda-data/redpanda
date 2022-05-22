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

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <absl/container/flat_hash_map.h>

template<typename Container>
concept IterableContainer
  = requires(Container c, typename Container::iterator i) {
    { c.begin() } -> std::same_as<typename Container::iterator>;
    { c.end() } -> std::same_as<typename Container::iterator>;
    { ++i } -> std::convertible_to<typename Container::iterator>;
};

template<typename Container>
concept RangeErasableContainer = requires(
  Container c, typename Container::iterator i, typename Container::iterator j) {
    { c.erase(i, j) } -> std::same_as<typename Container::iterator>;
};

template<typename Container>
concept SizableContainer = requires(Container c) {
    { c.size() } -> std::convertible_to<size_t>;
};

template<typename Container>
concept ClearableContainer = requires(Container c) {
    c.clear();
};

/// Clears the given container while yielding to seastar after every
/// element is erased. This is used to avoid reactor stall warnings
/// when deleting containers with a large number of objects.
///
/// \param c a container to be erased.
template<typename K, typename V>
ss::future<>
async_clear(absl::flat_hash_map<K, V>& c, const size_t threshold_size = 1000) {
    // Avoid excessive erases on small containers.
    if (c.size() <= threshold_size) {
        c.clear();
        co_return;
    }

    auto current = c.begin();

    while (current != c.end()) {
        // Note: abseil containers not compatible with std containers
        // in all other overloads of erase.
        current = c.erase(current, ++current);
        co_await ss::coroutine::maybe_yield();
    }
}
