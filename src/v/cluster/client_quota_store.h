// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/client_quota_serde.h"
#include "cluster/controller_snapshot.h"
#include "container/fragmented_vector.h"

#include <absl/algorithm/container.h>
#include <absl/container/node_hash_map.h>

namespace cluster::client_quota {

class store final {
public:
    using container_type = absl::node_hash_map<entity_key, entity_value>;
    using range_container_type
      = chunked_vector<std::pair<entity_key, entity_value>>;
    using range_callback_type
      = std::function<bool(const std::pair<entity_key, entity_value>&)>;

    /// Constructs an empty store
    store() = default;

    /// Constructs a store based on a controller snapshot
    explicit store(const controller_snapshot_parts::client_quotas_t& snap)
      : _quotas{snap.quotas} {};

    /// Upserts the given quota at the given entity key
    /// All quota types are overwritten with the given entity_value, so on alter
    /// operations we need to read the current state of the quota and merge it
    /// with the alterations
    void set_quota(const entity_key&, const entity_value&);

    /// Removes the configured quota at the given entity key
    void remove_quota(const entity_key&);

    /// Returns the configured quotas for the given entity key if it exists
    std::optional<entity_value> get_quota(const entity_key&) const;

    /// Returns a list of quotas that match the given predicate
    store::range_container_type range(range_callback_type&&) const;

    /// Returns the number of quotas stored in the store
    container_type::size_type size() const;

    /// Removes all client quotas in the store
    void clear();

    /// Returns a copy of all the client quotas in the store
    const container_type& all_quotas() const;

    /// Applies the given alter controller command to the store
    void apply_delta(const alter_delta_cmd_data&);

    static constexpr auto entity_part_filter =
      [](
        const std::pair<entity_key, entity_value>& kv,
        const entity_key::part& target_part) {
          return absl::c_any_of(
            kv.first.parts, [&target_part](const entity_key::part& key_part) {
                return key_part == target_part;
            });
      };

    // TODO: provide an observer mechanism so that quota_manager can listen to
    // quota changes and update its state accordingly

private:
    container_type _quotas;
};

} // namespace cluster::client_quota
