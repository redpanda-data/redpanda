// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

#include <optional>

namespace config {

/// A config value captured at startup that remains fixed until explicitly
/// reset. Stored in thread_local storage on each shard for fast local access.
///
/// This is useful for config values that:
/// - Must be consistent throughout a service's lifetime
/// - Should only change on restart (even if the underlying config can change)
///
/// Usage:
///   using my_feature = startup_config<bool, struct my_feature_tag>;
///
///   // In start():
///   co_await my_feature::initialize(config::shard_local_cfg().my_feature());
///
///   // Anywhere:
///   if (my_feature::get()) { ... }
///
///   // In stop(), when stop()/start() cycles are possible:
///   co_await my_feature::reset();
///
template<typename T, typename Tag>
class startup_config {
public:
    /// Initialize the value on all shards. Must be called before get().
    static ss::future<> initialize(T value) {
        return ss::smp::invoke_on_all([value] { set_local(value); });
    }

    /// Set the value on the current shard only. Useful for testing.
    static void set_local(T value) {
        vassert(!_value.has_value(), "startup_config already initialized");
        _value = value;
    }

    /// Get the value. Asserts if not initialized.
    static T get() {
        vassert(_value.has_value(), "startup_config not initialized");
        return *_value;
    }

    /// Check if initialized on this shard.
    static bool is_initialized() { return _value.has_value(); }

    /// Reset on all shards, allowing re-initialization on restart.
    static ss::future<> reset() {
        return ss::smp::invoke_on_all([] { reset_local(); });
    }

    /// Reset on the current shard only. Useful for testing.
    static void reset_local() {
        vassert(_value.has_value(), "startup_config not initialized");
        _value.reset();
    }

private:
    static inline thread_local std::optional<T> _value;
};

} // namespace config
