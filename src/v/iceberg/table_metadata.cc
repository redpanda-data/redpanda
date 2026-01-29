/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/table_metadata.h"

#include "iceberg/compatibility_utils.h"

namespace iceberg {

sort_field sort_field::copy() const {
    return sort_field{
      .transform = transform,
      .source_ids = source_ids.copy(),
      .direction = direction,
      .null_order = null_order,
    };
}

sort_order sort_order::copy() const {
    chunked_vector<sort_field> ret_fields;
    ret_fields.reserve(fields.size());
    for (const auto& field : fields) {
        ret_fields.push_back(field.copy());
    }
    return {
      .order_id = order_id,
      .fields = std::move(ret_fields),
    };
}

const schema*
table_metadata::get_equivalent_schema(const struct_type& type) const {
    auto schemas_reversed = std::ranges::reverse_view(schemas);
    auto it = std::ranges::find_if(
      schemas_reversed,
      [&type](const iceberg::struct_type& source) {
          return iceberg::schemas_equivalent(source, type);
      },
      &iceberg::schema::schema_struct);
    return it != schemas_reversed.end() ? &*it : nullptr;
}

table_metadata table_metadata::copy() const {
    chunked_vector<schema> ret_schemas;
    ret_schemas.reserve(schemas.size());
    for (const auto& s : schemas) {
        ret_schemas.push_back(s.copy());
    }
    chunked_vector<partition_spec> ret_partition_specs;
    ret_partition_specs.reserve(partition_specs.size());
    for (const auto& ps : partition_specs) {
        ret_partition_specs.push_back(ps.copy());
    }
    std::optional<table_properties_t> ret_properties;
    if (properties.has_value()) {
        ret_properties.emplace();
        ret_properties->reserve(properties->size());
        for (const auto& [k, v] : *properties) {
            ret_properties->emplace(k, v);
        }
    }
    std::optional<chunked_vector<snapshot>> ret_snapshots;
    if (snapshots.has_value()) {
        ret_snapshots.emplace();
        ret_snapshots->reserve(snapshots->size());
        for (const auto& snap : *snapshots) {
            ret_snapshots->push_back(snap.copy());
        }
    }
    chunked_vector<sort_order> ret_sort_orders;
    ret_sort_orders.reserve(sort_orders.size());
    for (const auto& order : sort_orders) {
        ret_sort_orders.push_back(order.copy());
    }
    std::optional<chunked_hash_map<ss::sstring, snapshot_reference>> ret_refs;
    if (refs.has_value()) {
        ret_refs.emplace();
        ret_refs->reserve(refs->size());
        for (const auto& [k, v] : *refs) {
            ret_refs->emplace(k, v);
        }
    }

    return {
      .format_version = format_version,
      .table_uuid = table_uuid,
      .location = location,
      .last_sequence_number = last_sequence_number,
      .last_updated_ms = last_updated_ms,
      .last_column_id = last_column_id,
      .schemas = std::move(ret_schemas),
      .current_schema_id = current_schema_id,
      .partition_specs = std::move(ret_partition_specs),
      .default_spec_id = default_spec_id,
      .last_partition_id = last_partition_id,
      .properties = std::move(ret_properties),
      .current_snapshot_id = current_snapshot_id,
      .snapshots = std::move(ret_snapshots),
      .sort_orders = std::move(ret_sort_orders),
      .default_sort_order_id = default_sort_order_id,
      .refs = std::move(ret_refs),
    };
}
} // namespace iceberg
