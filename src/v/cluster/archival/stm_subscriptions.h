// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <cstdint>

namespace archival {

/// A manager for archival stm state changes subscriptions.
class stm_subscriptions {
public:
    using id_t = named_type<int32_t, struct id_tag>;
    using cb_t = ss::noncopyable_function<void() noexcept>;

    /// Subscribe to state changes. The callback will be called every time the
    /// state of the STM changes. The subscription ID is returned and must be
    /// used to unsubscribe. It is unique for the lifetime of the STM.
    ///
    /// If you need to store subscriptions for multiple STMs in the same
    /// container, augment the id with additional information like to
    /// distinguish between different STMs.
    [[nodiscard("return value must be used to unsubscribe")]] id_t
    subscribe(cb_t f) {
        auto id = _next_sub_id++;
        _subscriptions.emplace(id, std::move(f));
        return id;
    }

    /// Unsubscribe from state changes if subscription id exists. If the
    /// subscription id does not exist, the method is a no-op.
    void unsubscribe(id_t id) { _subscriptions.erase(id); }

    /// Notify all subscribers.
    void notify() {
        for (auto& [_, f] : _subscriptions) {
            f();
        }
    }

private:
    absl::flat_hash_map<id_t, cb_t> _subscriptions;
    id_t _next_sub_id{0};
};

}; // namespace archival
