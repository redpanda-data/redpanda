/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "feature_table.h"

#include "cluster/logger.h"
#include "cluster/types.h"

namespace cluster {

std::string_view to_string_view(feature f) {
    switch (f) {
    case feature::central_config:
        return "central_config";
    }
    __builtin_unreachable();
}

feature_list feature_table::get_active_features() const {
    if (_active_version == invalid_version) {
        // The active version will be invalid_version when
        // the first version of redpanda with feature_manager
        // first runs (all nodes must check in before active_version
        // gets updated to a valid version for the first time)
        vlog(
          clusterlog.debug,
          "Feature manager not yet initialized, returning no features");
        return {};
    }

    if (_active_version < cluster_version{1}) {
        // 1 was the earliest version number, and invalid_version was
        // handled above.  This an unexpected situation.
        vlog(
          clusterlog.warn,
          "Invalid logical version {}, returning no features",
          _active_version);
        return {};
    } else {
        // A single branch for now, this will become a check of _active_version
        // with different features per version when we add another version
        return {
          feature::central_config,
        };
    }
}

void feature_table::set_active_version(cluster_version v) {
    _active_version = v;

    // Update mask for fast is_active() lookup
    _active_features_mask = 0x0;
    for (const auto& f : get_active_features()) {
        _active_features_mask |= uint64_t(f);
    }
}

} // namespace cluster