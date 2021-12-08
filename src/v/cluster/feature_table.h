/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"

#include <string_view>

class cluster_version;

namespace cluster {

enum class feature {
    central_config = 0x1,
};

std::string_view to_string_view(feature);

using feature_list = std::vector<feature>;

/**
 * To enable all shards to efficiently check enablement of features
 * in their hot paths, the cluster logical version and features
 * are copied onto each shard.
 *
 * Instances of this class are updated by feature_manager.
 */
class feature_table {
public:
    cluster_version get_active_version() const noexcept {
        return _active_version;
    }

    feature_list get_active_features() const;

    /**
     * Query whether a feature is active, i.e. whether functionality
     * depending on this feature should be allowed to run.
     *
     * Keep this small and simple to be used in hot paths that need to check
     * for feature enablement.
     */
    bool is_active(feature f) const noexcept {
        return (uint64_t(f) & _active_features_mask) != 0;
    }

private:
    // Only for use by our friends feature backend & manager
    void set_active_version(cluster_version);

    cluster_version _active_version{invalid_version};

    // Bitmask only used at runtime: if we run out of bits for features
    // just use a bigger one.  Do not serialize this as a bitmask anywhere.
    uint64_t _active_features_mask{0};

    // feature_manager is a friend so that they can initialize
    // the active version on single-node first start.
    friend class feature_manager;

    // feature_backend is a friend for routine updates when
    // applying raft0 log events.
    friend class feature_backend;
};

} // namespace cluster