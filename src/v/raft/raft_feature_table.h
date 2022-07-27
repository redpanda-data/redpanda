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

#include <bitset>
#include <type_traits>
namespace raft {
#pragma once

// raft specific features

enum class raft_feature {
    improved_config_change = 0,
};
/**
 *  Simple class aggregating information about raft features, it will be used by
 * `raft::consensus` instances to decide if a particular feature is enabled.
 *
 * This class will provide a way to use feature manager without depending on
 * `cluster` module to access the feature table directly.
 *
 */
class raft_feature_table {
public:
    bool is_feature_active(raft_feature f) const {
        return _active_features.test(
          static_cast<std::underlying_type_t<raft_feature>>(f));
    }

    void set_feature_active(raft_feature f) {
        _active_features.set(
          static_cast<std::underlying_type_t<raft_feature>>(f));
    };

private:
    std::bitset<32> _active_features;
};

} // namespace raft
