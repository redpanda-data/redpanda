// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition.h"
#include "iceberg/snapshot.h"
#include "iceberg/transform.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <optional>

namespace iceberg {

enum class sort_direction {
    asc,
    desc,
};

enum class null_order {
    nulls_first,
    nulls_last,
};

struct sort_field {
    transform transform;
    chunked_vector<nested_field::id_t> source_ids;
    sort_direction direction;
    null_order null_order;

    friend bool operator==(const sort_field&, const sort_field&) = default;
};

struct sort_order {
    using id_t = named_type<int, struct sort_order_tag>;
    static constexpr id_t unsorted_id = id_t{0};

    id_t order_id;
    chunked_vector<sort_field> fields;

    friend bool operator==(const sort_order&, const sort_order&) = default;
};

// V2 metadata for an Iceberg table, as defined by the Iceberg spec.
struct table_metadata {
    format_version format_version;
    uuid_t table_uuid;
    ss::sstring location;
    sequence_number last_sequence_number;
    model::timestamp last_updated_ms;
    nested_field::id_t last_column_id;
    chunked_vector<schema> schemas;
    schema::id_t current_schema_id;
    chunked_vector<partition_spec> partition_specs;
    partition_spec::id_t default_spec_id;
    partition_field::id_t last_partition_id;

    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> properties;
    std::optional<snapshot_id> current_snapshot_id;
    std::optional<chunked_vector<snapshot>> snapshots;

    // TODO: (optional) snapshot-log
    // TODO: (optional) metadata-log

    chunked_vector<sort_order> sort_orders;
    sort_order::id_t default_sort_order_id;

    // A map of snapshot references. The map keys are the unique snapshot
    // reference names in the table, and the map values are snapshot reference
    // objects. There is always a main branch reference pointing to the
    // current-snapshot-id even if the refs map is null.
    std::optional<chunked_hash_map<ss::sstring, snapshot_reference>> refs;

    // TODO: (optional) statistics
    // TODO: (optional) partition_statistics

    friend bool operator==(const table_metadata&, const table_metadata&)
      = default;
};

} // namespace iceberg
