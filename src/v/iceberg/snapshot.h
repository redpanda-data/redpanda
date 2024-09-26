// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "model/timestamp.h"

#include <absl/container/btree_map.h>

namespace iceberg {

enum class snapshot_operation {
    // Files were only added in the given snapshot.
    append,

    // Files were added or removed but the contents of the table did not
    // logically change (e.g. metadata compaction, relocating files, etc).
    replace,

    // Files were added and removed.
    overwrite,

    // Files were only removed.
    delete_data,
};

// A string map that summarizes the snapshot changes.
// Iceberg may store extra information per snapshot to speed up various
// operations, e.g. by skipping over processing of snapshots that haven't added
// new data.
struct snapshot_summary {
    // The operation that this snapshot represents.
    snapshot_operation operation;

    // All other properties of the snapshot, besides 'operation'.
    // NOTE: these aren't necessarily important to Redpanda's Iceberg write
    // path, but are still important to serialize, as they may be used for
    // optimization by Iceberg readers.
    absl::btree_map<ss::sstring, ss::sstring> other;

    friend bool operator==(const snapshot_summary&, const snapshot_summary&)
      = default;
};

// Represents a point-in-time snapshot of an Iceberg table. Each snapshot
// points to a manifest list object along with various metadata relevant to the
// snapshot. These snapshots are expected to be serialized into JSON as a part
// of the table metadata in accordance with the Iceberg spec.
struct snapshot {
    snapshot_id id;

    // The snapshot of that preceeds this snapshot.
    // May be null if there is no parent snapshot, e.g. this is the first
    // snapshot for a table.
    std::optional<snapshot_id> parent_snapshot_id;

    // Monotonically increasing counter that tracks the order of changes in the
    // table.
    sequence_number sequence_number;

    // Timestamp at which this snapshot was created.
    model::timestamp timestamp_ms;

    // Summarizes the operation and various properties of this snapshot.
    snapshot_summary summary;

    // Location of a manifest list for this snapshot that tracks manifest files
    // and additional metadata.
    ss::sstring manifest_list_path;

    // Current schema at the time the snapshot was created.
    std::optional<schema::id_t> schema_id;

    friend bool operator==(const snapshot&, const snapshot&) = default;
};

// The type of snapshot reference.
enum class snapshot_ref_type {
    // A label for an individual snapshot.
    tag,

    // A mutable named reference. Can be updated by committing a new snapshot
    // as a branch's referened snapshot.
    branch,
};

// Represents a named reference to a snapshot.
// NOTE: the name of the reference is tracked outside of the reference itself,
// e.g. in table metadata.
struct snapshot_reference {
    snapshot_id snapshot_id;
    snapshot_ref_type type;

    // For snapshot references except the 'main' branch, a positive number for
    // the max age of the snapshot reference to keep while expiring snapshots.
    std::optional<int64_t> max_ref_age_ms;

    // For branch type only, a positive number for the max age of snapshots to
    // keep when expiring, including the latest snapshot.
    std::optional<int64_t> max_snapshot_age_ms;

    // For branch type only, a positive number for the minimum number of
    // snapshots to keep in a branch while expiring snapshots.
    std::optional<int32_t> min_snapshots_to_keep;

    friend bool operator==(const snapshot_reference&, const snapshot_reference&)
      = default;
};

} // namespace iceberg
