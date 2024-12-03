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

#include "feature_table_snapshot.h"

#include "base/vlog.h"
#include "features/feature_table.h"
#include "features/logger.h"

namespace features {

feature_table_snapshot feature_table_snapshot::from(const feature_table& ft) {
    feature_table_snapshot fts;

    fts.states.reserve(feature_schema.size());

    fts.version = ft.get_active_version();
    fts.license = ft.get_configured_license();
    for (const auto& state : ft._feature_state) {
        auto& name = state.spec.name;
        fts.states.push_back(feature_state_snapshot{
          .name = ss::sstring(name), .state = state._state});
    }
    fts.applied_offset = ft.get_applied_offset();
    fts.original_version = ft.get_original_version();

    return fts;
}

void feature_table_snapshot::apply(feature_table& ft) const {
    vlog(
      featureslog.debug,
      "applying snapshot with applied offset {}",
      applied_offset);

    ft.set_active_version(version);
    ft._license = license;

    for (auto& cur_state : ft._feature_state) {
        const auto& spec = cur_state.spec;
        auto snap_state_iter = std::find_if(
          states.begin(),
          states.end(),
          [&spec](const feature_state_snapshot& s) {
              return s.name == spec.name;
          });
        if (snap_state_iter == states.end()) {
            // The feature table refers to a feature name that the snapshot
            // doesn't mention: this is normal on upgrade.
            if (spec.require_version <= version) {
                // The feature was introduced no later than the agreed version,
                // which is no later than the version of the broker that took
                // the snapshot. So it only can be missing because it has been
                // retired and thus deemed active.
                cur_state._state = feature_state::state::active;
            } else {
                // Otherwise the feature was introduced after the agreed
                // version, so it can only be disabled before we reach it.
                cur_state._state = feature_state::state::unavailable;
            }
            vlog(
              featureslog.debug,
              "No state for feature '{}' in snapshot v{}, upgrade in progress? "
              "Assuming the feature state is {}",
              spec.name,
              version,
              cur_state._state);
        } else {
            if (
              spec.require_version
                <= feature_table::get_earliest_logical_version()
              && spec.available_rule == feature_spec::available_policy::always
              && snap_state_iter->state < feature_state::state::active) {
                // It is possible in theory that the snapshot is so old that
                // applying it will result in disabling features that we enabled
                // by fast-forwarding the table to earliest_logical_version on
                // the first start. We want to avoid that.
                continue;
            }

            cur_state._state = snap_state_iter->state;
        }
    }

    ft.set_applied_offset(applied_offset);
    ft.set_original_version(original_version);

    ft.on_update();
}

bytes feature_table_snapshot::kvstore_key() {
    return iobuf_to_bytes(serde::to_iobuf(ss::sstring("feature_table")));
}

} // namespace features
