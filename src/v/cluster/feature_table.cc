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

    // Check all pending waiters and kick any that are ready.  Do exhaustive
    // pass: feature updates are rare and waiter count is small, order of the
    // number of subsystems.
    auto waiters = std::exchange(_waiters, {});
    for (auto& wi : waiters) {
        if (is_active(wi->f)) {
            vlog(
              clusterlog.trace,
              "Triggering waiter for feature {}",
              to_string_view(wi->f));
            wi->p.set_value();
        } else {
            vlog(
              clusterlog.trace,
              "Not ready for waiter (version={}, waiter wants feature {})",
              _active_version,
              to_string_view(wi->f));
            _waiters.push_back(std::move(wi));
        }
    }
}

/**
 * Wait until this feature becomes available, or the abort
 * source fires.  If the abort source fires, the future
 * will be an exceptional future.
 */
ss::future<> feature_table::await_feature(feature f, ss::abort_source& as) {
    if (is_active(f)) {
        vlog(clusterlog.trace, "Feature {} already active", to_string_view(f));
        return ss::now();
    } else {
        vlog(clusterlog.trace, "Waiting for feature {}", to_string_view(f));
        auto item = std::make_unique<wait_item>(f);
        auto fut = item->p.get_future();

        auto sub_opt = as.subscribe(
          [item_ptr = item->weak_from_this()]() noexcept {
              if (item_ptr) {
                  vlog(
                    clusterlog.trace,
                    "Abort source fired for waiter on {}",
                    to_string_view(item_ptr->f));
                  item_ptr->p.set_exception(ss::abort_requested_exception());
              }
          });

        if (!sub_opt) {
            // Abort source already fired!
            return ss::make_exception_future<>(ss::abort_requested_exception());
        } else {
            item->abort_sub = std::move(*sub_opt);
        }

        _waiters.emplace_back(std::move(item));
        return fut;
    }
}

} // namespace cluster