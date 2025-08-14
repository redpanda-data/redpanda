/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/remove_snapshots_action.h"

#include "base/vlog.h"
#include "container/chunked_hash_map.h"
#include "iceberg/logger.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_update.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

namespace iceberg {

namespace {

std::optional<ss::sstring>
get_property(const table_metadata& table, std::string_view prop) {
    if (!table.properties.has_value()) {
        return std::nullopt;
    }
    auto it = table.properties->find(ss::sstring{prop});
    if (it == table.properties->end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<long> get_long_property(
  const prefix_logger& logger,
  const table_metadata& table,
  std::string_view prop) {
    auto p_str = get_property(table, prop);
    if (!p_str.has_value()) {
        return std::nullopt;
    }
    try {
        return std::stol(*p_str);
    } catch (...) {
        vlog(logger.warn, "Invalid long for property '{}': '{}'", prop, *p_str);
        return std::nullopt;
    }
}
long table_max_snapshot_age_ms(
  const prefix_logger& logger, const table_metadata& table) {
    return get_long_property(logger, table, max_snapshot_age_ms_prop)
      .value_or(remove_snapshots_action::default_max_snapshot_age_ms);
}
long table_max_snapshot_ref_age_ms(
  const prefix_logger& logger, const table_metadata& table) {
    return get_long_property(logger, table, max_ref_age_ms_prop)
      .value_or(remove_snapshots_action::default_max_ref_age_ms);
}
long table_min_snapshots_retained(
  const prefix_logger& logger, const table_metadata& table) {
    // NOTE: the Java implementation uses an int32 for this property, but it
    // seems harmless to use a bigger type.
    return get_long_property(logger, table, min_snapshots_to_keep_prop)
      .value_or(remove_snapshots_action::default_min_snapshots_retained);
}

// Arguments for collecting ancestors of a given snapshot.
struct collection_reqs {
    collection_reqs(size_t min_snaps, model::timestamp::type min_timestamp)
      : min_snapshots_to_keep(min_snaps)
      , min_timestamp_to_keep_ms(min_timestamp) {}
    size_t min_snapshots_to_keep;
    model::timestamp::type min_timestamp_to_keep_ms;
};

// Returns the snapshot IDs of all ancestors of the given snapshot, keeping
// snapshots such that both `min_snapshots_to_keep` and
// `min_timestamp_to_keep_ms` are satisfied, if provided.
chunked_hash_set<snapshot_id> collect_snap_ancestors(
  const table_metadata& table,
  snapshot_id snap_id,
  std::optional<collection_reqs> collect_reqs) {
    chunked_hash_set<snapshot_id> ret;
    if (!table.snapshots.has_value() || table.snapshots->empty()) {
        return ret;
    }
    auto next_snap = std::make_optional<snapshot_id>(snap_id);
    auto table_snaps = table.get_snapshots_by_id();
    while (next_snap.has_value()) {
        auto snap_it = table_snaps.find(next_snap.value());
        if (snap_it == table_snaps.end()) {
            break;
        }
        auto snap_ts = snap_it->second.timestamp_ms;
        if (collect_reqs.has_value()) {
            auto has_enough_snaps = ret.size()
                                    >= collect_reqs->min_snapshots_to_keep;
            auto snap_too_old = snap_ts.value()
                                < collect_reqs->min_timestamp_to_keep_ms;
            if (has_enough_snaps && snap_too_old) {
                // We've satisfied the count minimum requirement, and all
                // further ancestors are older than the oldest to keep. No more
                // snapshots to keep.
                break;
            }
        }
        ret.emplace(*next_snap);
        next_snap = snap_it->second.parent_snapshot_id;
    }
    return ret;
}

// Collect the ancestors of all retained branch references that need to be
// retained per the branch's retention settings.
void collect_retained_branch_ancestors(
  const table_metadata& table,
  const chunked_hash_map<ss::sstring, snapshot_reference>& retained_refs,
  model::timestamp now,
  long table_max_snap_age_ms,
  size_t table_min_snapshots_to_keep,
  chunked_hash_set<snapshot_id>* retained_snaps) {
    for (const auto& [ref_name, ref] : retained_refs) {
        if (ref.type == snapshot_ref_type::branch) {
            auto snap_min_timestamp_to_keep_ms
              = now.value()
                - ref.max_snapshot_age_ms.value_or(table_max_snap_age_ms);
            auto snap_min_snapshots_to_keep
              = ref.min_snapshots_to_keep.value_or(table_min_snapshots_to_keep);
            auto branch_ancestors_to_retain = collect_snap_ancestors(
              table,
              ref.snapshot_id,
              std::make_optional<collection_reqs>(
                snap_min_snapshots_to_keep, snap_min_timestamp_to_keep_ms));
            retained_snaps->insert(
              branch_ancestors_to_retain.begin(),
              branch_ancestors_to_retain.end());
        }
    }
}

// Consider only the snapshots that aren't ancestors of any snapshot reference,
// and retain them only if they are new enough.
void collect_retained_unreferenced_snaps(
  const prefix_logger& logger,
  const table_metadata& table,
  const chunked_hash_map<ss::sstring, snapshot_reference>& retained_refs,
  model::timestamp now,
  long table_max_snap_age_ms,
  chunked_hash_set<snapshot_id>* retained_snaps) {
    chunked_hash_set<snapshot_id> referenced_snaps;
    for (const auto& [ref_name, ref] : retained_refs) {
        if (ref.type == snapshot_ref_type::branch) {
            auto branch_ancestors = collect_snap_ancestors(
              table, ref.snapshot_id, /*collect_reqs=*/std::nullopt);
            referenced_snaps.insert(
              branch_ancestors.begin(), branch_ancestors.end());
        } else {
            referenced_snaps.emplace(ref.snapshot_id);
        }
    }
    vlog(logger.debug, "{} snapshots are referenced", referenced_snaps.size());
    size_t count = 0;
    auto table_min_timestamp_to_keep_ms = now.value() - table_max_snap_age_ms;
    for (const auto& s : *table.snapshots) {
        if (referenced_snaps.contains(s.id)) {
            // Not an unreferenced snapshot.
            continue;
        }
        auto snap_too_old = s.timestamp_ms.value()
                            < table_min_timestamp_to_keep_ms;
        if (snap_too_old) {
            continue;
        }
        retained_snaps->emplace(s.id);
        ++count;
    }
    vlog(logger.debug, "Retaining {} unreferenced snapshots", count);
}

} // namespace

remove_snapshots_action::remove_snapshots_action(
  const table_metadata& table, model::timestamp now)
  : table_(table)
  , now_(now)
  , logger_(
      log,
      fmt::format(
        "[table uuid: {}, location: {}]", table_.table_uuid, table_.location)) {
}
void remove_snapshots_action::compute_removed_snapshots(
  chunked_vector<snapshot_id>* snaps_to_remove,
  chunked_vector<ss::sstring>* refs_to_remove) const {
    if (!table_.snapshots.has_value() || table_.snapshots->empty()) {
        // Technically we could proceed to remove references still, but just
        // no-op instead. This mirrors the behavior in Apache Iceberg.
        // https://github.com/apache/iceberg/blob/df547908a9500ec5b886cfeb64ea5bf10ebde84f/core/src/main/java/org/apache/iceberg/RemoveSnapshots.java#L181-L183
        return;
    }
    auto table_max_snap_age_ms = table_max_snapshot_age_ms(logger_, table_);
    auto table_max_ref_age_ms = table_max_snapshot_ref_age_ms(logger_, table_);
    auto table_min_snapshots_to_keep = table_min_snapshots_retained(
      logger_, table_);

    const auto& table_snaps = table_.snapshots.value();
    chunked_hash_set<snapshot_id> retained_snaps;
    chunked_hash_map<ss::sstring, snapshot_reference> retained_refs;
    const auto& cur_snap_id = table_.current_snapshot_id;
    if (table_.refs.has_value()) {
        // Iterate through the snapshot references and collect those that need
        // to be retained.
        const auto should_keep_ref = [this, &table_snaps, table_max_ref_age_ms](
                                       const ss::sstring& ref_name,
                                       const snapshot_reference& ref) -> bool {
            if (ref_name == "main") {
                return true;
            }
            auto ref_snap_id = ref.snapshot_id;
            auto snap_it = std::ranges::find(
              table_snaps, ref_snap_id, &snapshot::id);
            if (snap_it == table_snaps.end()) {
                // Invalid ref, should not be retained.
                return false;
            }
            const auto& ref_snap = *snap_it;
            // Keep only the references whose age is lower than the max age.
            return now_.value() - ref_snap.timestamp_ms.value()
                   <= ref.max_ref_age_ms.value_or(table_max_ref_age_ms);
        };
        // Collect references to retain. We'll use this list to determine the
        // full set of snapshots to retain below.
        for (const auto& [ref_name, ref] : *table_.refs) {
            if (should_keep_ref(ref_name, ref)) {
                retained_refs.emplace(ref_name, ref);
                retained_snaps.emplace(ref.snapshot_id);
            }
        }
    } else if (cur_snap_id.has_value()) {
        // Even if there is no explicit references, the 'main' branch always
        // refers to the current snapshot id. So for the sake of accounting
        // what to retain, treat the current snapshot as a branch.
        retained_refs.emplace(
          "main",
          snapshot_reference{
            .snapshot_id = *cur_snap_id,
            .type = snapshot_ref_type::branch,
          });
    }

    collect_retained_branch_ancestors(
      table_,
      retained_refs,
      now_,
      table_max_snap_age_ms,
      table_min_snapshots_to_keep,
      &retained_snaps);
    collect_retained_unreferenced_snaps(
      logger_,
      table_,
      retained_refs,
      now_,
      table_max_snap_age_ms,
      &retained_snaps);

    // The set to remove is everything that isn't being retained.
    for (const auto& s : table_snaps) {
        if (!retained_snaps.contains(s.id)) {
            snaps_to_remove->emplace_back(s.id);
        }
    }
    if (table_.refs.has_value()) {
        for (const auto& [ref_name, ref] : *table_.refs) {
            if (!retained_refs.contains(ref_name)) {
                refs_to_remove->emplace_back(ref_name);
            }
        }
    }
}

ss::future<action::action_outcome> remove_snapshots_action::build_updates() && {
    updates_and_reqs ret;
    if (!table_.snapshots.has_value() || table_.snapshots->empty()) {
        vlog(logger_.debug, "No snapshots, snapshot removal returning early");
        co_return ret;
    }

    chunked_vector<snapshot_id> snaps_to_remove;
    chunked_vector<ss::sstring> refs_to_remove;
    compute_removed_snapshots(&snaps_to_remove, &refs_to_remove);
    vlog(
      logger_.debug,
      "Computed {} snapshots and {} refs to remove",
      snaps_to_remove.size(),
      refs_to_remove.size());
    if (snaps_to_remove.empty() && refs_to_remove.empty()) {
        co_return ret;
    }

    // NOTE: the Java implementation only allows a single snapshot per update,
    // despite the spec allowing multiple. To be maximally supportive of the
    // ecosystem, we will serialize one snapshot per update.
    // https://github.com/apache/iceberg/blob/3e6da2e5437ffb3f643275927e5580cb9620256b/core/src/main/java/org/apache/iceberg/MetadataUpdateParser.java#L550-L553
    for (auto& s : snaps_to_remove) {
        ret.updates.emplace_back(table_update::remove_snapshots{{s}});
    }
    for (auto& r : refs_to_remove) {
        ret.updates.emplace_back(
          table_update::remove_snapshot_ref{.ref_name = std::move(r)});
    }

    ret.requirements.emplace_back(table_requirement::assert_ref_snapshot_id{
      .ref = "main",
      .snapshot_id = table_.current_snapshot_id,
    });
    co_return ret;
}

} // namespace iceberg
