/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_vector.h"
#include "iceberg/action.h"
#include "iceberg/table_metadata.h"
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

#include <limits>

namespace iceberg {

using namespace std::chrono_literals;

static constexpr std::string_view max_snapshot_age_ms_prop
  = "history.expire.max-snapshot-age-ms";
static constexpr std::string_view min_snapshots_to_keep_prop
  = "history.expire.min-snapshots-to-keep";
static constexpr std::string_view max_ref_age_ms_prop
  = "history.expire.max-ref-age-ms";

// Action to remove a set of snapshots from the table metadata. Snapshot
// retention is tied to snapshot references, which may refer to individual
// snapshots (tags) or may refer to a tree of snapshots (branches).
//
// Snapshot references may be removed based on a max reference age configured
// for the entire table. For snapshot references that are retained:
// - Ancestor snapshots of branches are retained up to a given minimum count
//   and a maximum age that may be configured per branch.
// - Snapshots that are tagged are kept for a configurable amount of time
//   (default is indefinitely).
//
// For snapshots that are not referenced or are not an ancestor snapshot of a
// branch, the snapshot is only kept if it is younger than a max expiry age
// which is configurable per table.
//
// NOTE: this does not perform IO to purge the corresponding manifest lists or
// the underlying manifests and data files. Callers are expected to do so upon
// committing this action and reloading the resulting table metadata.
class remove_snapshots_action : public action {
public:
    static constexpr long default_max_snapshot_age_ms = 5 * 24 * 60 * 60 * 1000;
    static constexpr long default_min_snapshots_retained = 1;
    // Default behavior is that references are kept forever.
    static constexpr long default_max_ref_age_ms
      = std::numeric_limits<long>::max();

    remove_snapshots_action(const table_metadata& table, model::timestamp now);
    ~remove_snapshots_action() override = default;

    // Computes the set of snapshots and snapshot references to remove from the
    // table.
    void compute_removed_snapshots(
      chunked_vector<snapshot_id>* snaps_to_remove,
      chunked_vector<ss::sstring>* refs_to_remove) const;

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    const table_metadata& table_;
    const model::timestamp now_;
    const prefix_logger logger_;
};

} // namespace iceberg
