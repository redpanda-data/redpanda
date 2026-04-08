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
#include "iceberg/datatypes.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition.h"
#include "iceberg/snapshot.h"
#include "iceberg/transform.h"
#include "iceberg/uri.h"
#include "utils/absl_sstring_hash.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <optional>

namespace iceberg {

using table_properties_t
  = chunked_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq>;

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

    sort_field copy() const;

    friend bool operator==(const sort_field&, const sort_field&) = default;
};

struct sort_order {
    using id_t = named_type<int, struct sort_order_tag>;
    static constexpr id_t unsorted_id = id_t{0};

    id_t order_id;
    chunked_vector<sort_field> fields;

    sort_order copy() const;

    friend bool operator==(const sort_order&, const sort_order&) = default;
};

// V2 metadata for an Iceberg table, as defined by the Iceberg spec.
struct table_metadata {
    format_version format_version;
    uuid_t table_uuid;
    uri location;
    sequence_number last_sequence_number;
    model::timestamp last_updated_ms;
    nested_field::id_t last_column_id;
    chunked_vector<schema> schemas;
    schema::id_t current_schema_id;
    chunked_vector<partition_spec> partition_specs;
    partition_spec::id_t default_spec_id;
    partition_field::id_t last_partition_id;

    std::optional<table_properties_t> properties;
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

    const partition_spec* get_partition_spec(partition_spec::id_t id) const {
        auto it = std::ranges::find(
          partition_specs, id, &partition_spec::spec_id);
        return it != partition_specs.end() ? &*it : nullptr;
    }

    const schema* get_schema(schema::id_t id) const {
        auto it = std::ranges::find(schemas, id, &schema::schema_id);
        return it != schemas.end() ? &*it : nullptr;
    }

    // Search for a schema that matches the provided type. Start from the end of
    // the list to short circuit on the common case (desired type is current,
    // latest schema)
    const schema* get_equivalent_schema(const struct_type& type) const;

    table_metadata copy() const;

    // TODO: consider making this a lazy data member if it gets used by many
    // callers for the same metadata.
    chunked_hash_map<snapshot_id, snapshot> get_snapshots_by_id() const {
        chunked_hash_map<snapshot_id, snapshot> snaps_by_id;
        if (!snapshots.has_value()) {
            return snaps_by_id;
        }
        for (const auto& s : *snapshots) {
            snaps_by_id.emplace(s.id, s);
        }
        return snaps_by_id;
    }

    friend bool
    operator==(const table_metadata&, const table_metadata&) = default;
};

} // namespace iceberg
