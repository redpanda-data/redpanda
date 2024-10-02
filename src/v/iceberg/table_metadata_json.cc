// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/table_metadata_json.h"

#include "iceberg/json_utils.h"
#include "iceberg/partition_json.h"
#include "iceberg/schema_json.h"
#include "iceberg/snapshot.h"
#include "iceberg/snapshot_json.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transform_json.h"
#include "json/document.h"
#include "strings/string_switch.h"

namespace iceberg {

namespace {

constexpr int format_version_to_int(format_version f) {
    switch (f) {
    case format_version::v1:
        return 1;
    case format_version::v2:
        return 2;
    }
}

format_version format_version_from_int(int f) {
    if (f == 1) {
        return format_version::v1;
    }
    if (f == 2) {
        return format_version::v2;
    }
    throw std::invalid_argument(fmt::format("Unknown format version: {}", f));
}

constexpr std::string_view sort_direction_to_str(sort_direction s) {
    switch (s) {
    case sort_direction::asc:
        return "asc";
    case sort_direction::desc:
        return "desc";
    }
}

sort_direction sort_direction_from_str(std::string_view s) {
    return string_switch<sort_direction>(s)
      .match(sort_direction_to_str(sort_direction::asc), sort_direction::asc)
      .match(sort_direction_to_str(sort_direction::desc), sort_direction::desc);
}

constexpr std::string_view null_order_to_str(null_order o) {
    switch (o) {
    case null_order::nulls_first:
        return "nulls-first";
    case null_order::nulls_last:
        return "nulls-last";
    }
}

null_order null_order_from_str(std::string_view s) {
    return string_switch<null_order>(s)
      .match(
        null_order_to_str(null_order::nulls_first), null_order::nulls_first)
      .match(null_order_to_str(null_order::nulls_last), null_order::nulls_last);
}

} // namespace

sort_field parse_sort_field(const json::Value& v) {
    const auto& transform_str = parse_required_str(v, "transform");
    chunked_vector<nested_field::id_t> source_ids;
    for (const auto& id_json : parse_required_array(v, "source-ids")) {
        if (!id_json.IsInt()) {
            throw std::invalid_argument(fmt::format(
              "Expected source-ids to be ints: {}", id_json.GetType()));
        }
        source_ids.emplace_back(nested_field::id_t{id_json.GetInt()});
    }
    auto direction = parse_required_str(v, "direction");
    auto null_order = parse_required_str(v, "null-order");
    return sort_field{
      .transform = transform_from_str(transform_str),
      .source_ids = std::move(source_ids),
      .direction = sort_direction_from_str(direction),
      .null_order = null_order_from_str(null_order),
    };
}

sort_order parse_sort_order(const json::Value& v) {
    auto order_id = parse_required_i32(v, "order-id");
    chunked_vector<sort_field> fields;
    for (const auto& f : parse_required_array(v, "fields")) {
        fields.emplace_back(parse_sort_field(f));
    }
    return sort_order{
      .order_id = sort_order::id_t{order_id},
      .fields = std::move(fields),
    };
}

table_metadata parse_table_meta(const json::Value& v) {
    auto format_version = parse_required_i32(v, "format-version");
    auto table_uuid = parse_required_str(v, "table-uuid");
    auto location = parse_required_str(v, "location");
    auto last_sequence_number = parse_required_i64(v, "last-sequence-number");
    auto last_updated_ms = parse_required_i64(v, "last-updated-ms");
    auto last_column_id = parse_required_i32(v, "last-column-id");
    const auto& schemas_json = parse_required_array(v, "schemas");
    chunked_vector<schema> schemas;
    for (const auto& s : schemas_json) {
        schemas.emplace_back(parse_schema(s));
    }
    auto current_schema_id = parse_required_i32(v, "current-schema-id");

    chunked_vector<partition_spec> partition_specs;
    auto partition_specs_json = parse_required_array(v, "partition-specs");
    for (const auto& s : partition_specs_json) {
        partition_specs.emplace_back(parse_partition_spec(s));
    }
    auto default_spec_id = parse_required_i32(v, "default-spec-id");
    auto last_partition_id = parse_required_i32(v, "last-partition-id");

    auto properties_json = parse_optional_object(v, "properties");
    std::optional<chunked_hash_map<ss::sstring, ss::sstring>> properties;
    if (properties_json.has_value()) {
        properties.emplace();
        for (const auto& m : properties_json.value()) {
            if (!m.name.IsString() || !m.value.IsString()) {
                throw std::invalid_argument(fmt::format(
                  "Expected 'properties' field to be a string map: {} => {}",
                  m.name.GetType(),
                  m.value.GetType()));
            }
            const auto& val_str = m.value.GetString();
            properties->emplace(m.name.GetString(), val_str);
        }
    }
    std::optional<snapshot_id> current_snapshot_id;
    auto current_snapshot_id_opt = parse_optional_i64(v, "current-snapshot-id");
    if (current_snapshot_id_opt.has_value()) {
        current_snapshot_id.emplace(current_snapshot_id_opt.value());
    }

    auto snapshots_json = parse_optional_array(v, "snapshots");
    std::optional<chunked_vector<snapshot>> snapshots;
    if (snapshots_json.has_value()) {
        snapshots.emplace();
        for (const auto& s : snapshots_json.value()) {
            snapshots->emplace_back(parse_snapshot(s));
        }
    }
    chunked_vector<sort_order> sort_orders;
    auto sort_orders_json = parse_required_array(v, "sort-orders");
    for (const auto& s : sort_orders_json) {
        sort_orders.emplace_back(parse_sort_order(s));
    }

    auto refs_json = parse_optional_object(v, "refs");
    std::optional<chunked_hash_map<ss::sstring, snapshot_reference>> refs;
    if (refs_json.has_value()) {
        refs.emplace();
        for (const auto& r : refs_json.value()) {
            if (!r.name.IsString()) {
                throw std::invalid_argument(fmt::format(
                  "Expected 'refs' field to be string-object map: {} => {}",
                  r.name.GetType(),
                  r.value.GetType()));
            }
            auto ref = parse_snapshot_ref(r.value);
            refs->emplace(r.name.GetString(), ref);
        }
    }

    auto default_sort_order_id = parse_required_i32(v, "default-sort-order-id");
    return table_metadata{
      .format_version = format_version_from_int(format_version),
      .table_uuid = uuid_t::from_string(table_uuid),
      .location = location,
      .last_sequence_number = sequence_number{last_sequence_number},
      .last_updated_ms = model::timestamp{last_updated_ms},
      .last_column_id = nested_field::id_t{last_column_id},
      .schemas = std::move(schemas),
      .current_schema_id = schema::id_t{current_schema_id},
      .partition_specs = std::move(partition_specs),
      .default_spec_id = partition_spec::id_t{default_spec_id},
      .last_partition_id = partition_field::id_t{last_partition_id},
      .properties = std::move(properties),
      .current_snapshot_id = current_snapshot_id,
      .snapshots = std::move(snapshots),
      .sort_orders = std::move(sort_orders),
      .default_sort_order_id = sort_order::id_t{default_sort_order_id},
      .refs = std::move(refs),
    };
}

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::sort_field& f) {
    w.StartObject();
    w.Key("transform");
    w.String(iceberg::transform_to_str(f.transform));
    w.Key("source-ids");
    w.StartArray();
    for (const auto& id : f.source_ids) {
        w.Int(id());
    }
    w.EndArray();
    w.Key("direction");
    w.String(ss::sstring(iceberg::sort_direction_to_str(f.direction)));
    w.Key("null-order");
    w.String(ss::sstring(iceberg::null_order_to_str(f.null_order)));
    w.EndObject();
}

void rjson_serialize(iceberg::json_writer& w, const iceberg::sort_order& m) {
    w.StartObject();
    w.Key("order-id");
    w.Int(m.order_id());
    w.Key("fields");
    w.StartArray();
    for (const auto& f : m.fields) {
        rjson_serialize(w, f);
    }
    w.EndArray();
    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::table_metadata& m) {
    w.StartObject();
    w.Key("format-version");
    w.Int(iceberg::format_version_to_int(m.format_version));
    w.Key("table-uuid");
    w.String(fmt::to_string(m.table_uuid));
    w.Key("location");
    w.String(m.location);
    w.Key("last-sequence-number");
    w.Int64(m.last_sequence_number());
    w.Key("last-updated-ms");
    w.Int64(m.last_updated_ms.value());
    w.Key("last-column-id");
    w.Int64(m.last_column_id());
    w.Key("schemas");
    w.StartArray();
    for (const auto& s : m.schemas) {
        rjson_serialize(w, s);
    }
    w.EndArray();
    w.Key("current-schema-id");
    w.Int(m.current_schema_id());
    w.Key("partition-specs");
    w.StartArray();
    for (const auto& s : m.partition_specs) {
        rjson_serialize(w, s);
    }
    w.EndArray();
    w.Key("default-spec-id");
    w.Int(m.default_spec_id());
    w.Key("last-partition-id");
    w.Int(m.last_partition_id());

    if (m.properties.has_value()) {
        w.Key("properties");
        w.StartObject();
        for (const auto& [k, v] : m.properties.value()) {
            w.Key(k);
            w.String(v);
        }
        w.EndObject();
    }

    if (m.current_snapshot_id.has_value()) {
        w.Key("current-snapshot-id");
        w.Int64(m.current_snapshot_id.value()());
    }

    if (m.snapshots.has_value()) {
        w.Key("snapshots");
        w.StartArray();
        for (const auto& s : m.snapshots.value()) {
            rjson_serialize(w, s);
        }
        w.EndArray();
    }

    w.Key("sort-orders");
    w.StartArray();
    for (const auto& s : m.sort_orders) {
        rjson_serialize(w, s);
    }
    w.EndArray();
    w.Key("default-sort-order-id");
    w.Int(m.default_sort_order_id());

    if (m.refs.has_value()) {
        w.Key("refs");
        w.StartObject();
        for (const auto& [name, ref] : m.refs.value()) {
            w.Key(name);
            rjson_serialize(w, ref);
        }
        w.EndObject();
    }
    w.EndObject();
}

} // namespace json
