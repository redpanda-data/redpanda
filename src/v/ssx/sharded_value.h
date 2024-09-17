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
#include "ssx/aligned.h"

#include <seastar/core/smp.hh>

#include <vector>

namespace ssx {

/// sharded_value is similar to ss::sharded<T> in that it has a local copy of
/// the wrapped type T on each shard. sharded_value is simpler because it
/// doesn't have the start(), stop() mechanics, so it is useful for use cases
/// where you just need a local copy of the value and not a full sharded
/// service.
template<typename T>
class sharded_value {
public:
    explicit sharded_value(T value)
      : _state(ss::smp::count, ssx::aligned<T>{value}) {}
    ~sharded_value() noexcept = default;

    sharded_value(sharded_value&& other) noexcept = default;
    sharded_value& operator=(sharded_value&&) noexcept = default;

    sharded_value(const sharded_value&) = delete;
    sharded_value& operator=(const sharded_value&) = delete;

    /// get a reference to the local instance
    const T& local() const { return _state[ss::this_shard_id()]; }

    /// get a reference to the local instance
    T& local() { return _state[ss::this_shard_id()]; }

private:
    std::vector<ssx::aligned<T>> _state;
};

} // namespace ssx
