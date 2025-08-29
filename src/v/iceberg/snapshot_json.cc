/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/snapshot_json.h"

#include "iceberg/json_utils.h"
#include "iceberg/snapshot.h"
#include "json/document.h"
#include "model/timestamp.h"
#include "strings/string_switch.h"

#include <absl/container/btree_map.h>

namespace iceberg {

namespace {

constexpr std::string_view operation_to_str(snapshot_operation o) {
    using enum snapshot_operation;
    switch (o) {
    case append:
        return "append";
    case replace:
        return "replace";
    case overwrite:
        return "overwrite";
    case delete_data:
        return "delete";
    }
}

snapshot_operation operation_from_str(std::string_view operation_str) {
    using enum snapshot_operation;
    return string_switch<snapshot_operation>(operation_str)
      .match(operation_to_str(append), append)
      .match(operation_to_str(replace), replace)
      .match(operation_to_str(overwrite), overwrite)
      .match(operation_to_str(delete_data), delete_data);
}

constexpr std::string_view ref_type_to_str(snapshot_ref_type t) {
    using enum snapshot_ref_type;
    switch (t) {
    case tag:
        return "tag";
    case branch:
        return "branch";
    }
}

snapshot_ref_type ref_type_from_str(std::string_view ref_type_str) {
    using enum snapshot_ref_type;
    return string_switch<snapshot_ref_type>(ref_type_str)
      .match(ref_type_to_str(tag), tag)
      .match(ref_type_to_str(branch), branch);
}

} // namespace

snapshot parse_snapshot(const json::Value& v) {
    auto id = parse_required_i64(v, "snapshot-id");
    std::optional<snapshot_id> parent_id;
    auto parent_id_opt = parse_optional_i64(v, "parent-snapshot-id");
    if (parent_id_opt.has_value()) {
        parent_id.emplace(parent_id_opt.value());
    }
    auto seq_num = parse_required_i64(v, "sequence-number");
    auto timestamp_ms = parse_required_i64(v, "timestamp-ms");
    auto manifest_list_path = parse_required_str(v, "manifest-list");
    auto schema_id_opt = parse_optional_i32(v, "schema-id");
    std::optional<schema::id_t> schema_id;
    if (schema_id_opt.has_value()) {
        schema_id.emplace(schema_id_opt.value());
    }
    const auto& summary_json = parse_required(v, "summary");
    if (!summary_json.IsObject()) {
        throw std::invalid_argument(fmt::format(
          "Expected JSON object to parse field 'summary', found: {}",
          summary_json.GetType()));
    }
    std::optional<snapshot_operation> operation;
    absl::btree_map<ss::sstring, ss::sstring> other_map;
    for (const auto& m : summary_json.GetObject()) {
        if (!m.name.IsString() || !m.value.IsString()) {
            throw std::invalid_argument(fmt::format(
              "Expected 'summary' field to be a string map, found: {} => {}",
              m.name.GetType(),
              m.value.GetType()));
        }
        const auto& val_str = m.value.GetString();
        // Pull out the 'operation' field specifically, as it's a required
        // field of the 'summary' map.
        if (m.name == "operation") {
            operation = operation_from_str(val_str);
            continue;
        }
        // Any other fields land in the 'other' map.
        other_map.emplace(m.name.GetString(), val_str);
    }
    if (!operation.has_value()) {
        throw std::invalid_argument(
          "Expected 'summary' field to have 'operation' member");
    }
    snapshot_summary summary{
      .operation = operation.value(),
      .other = std::move(other_map),
    };
    const auto maybe_parse_metric =
      [&summary](std::string_view metric_name, std::optional<int64_t>* out) {
          // Look for the given metric in the map and assign `out` if it
          // exists. If so, the metric is removed from the map.
          auto it = summary.other.find(ss::sstring(metric_name));
          if (it == summary.other.end()) {
              return;
          }
          try {
              auto m = std::stol(it->second);
              summary.other.erase(it);
              *out = m;
          } catch (...) {
              // Ignore surprising values.
          }
      };
    maybe_parse_metric("added-data-files", &summary.added_data_files);
    maybe_parse_metric("total-data-files", &summary.total_data_files);
    maybe_parse_metric("added-records", &summary.added_records);
    maybe_parse_metric("total-records", &summary.total_records);
    maybe_parse_metric("added-files-size", &summary.added_files_size);
    maybe_parse_metric("total-files-size", &summary.total_files_size);
    auto operation_str = parse_required_str(summary_json, "operation");
    return snapshot{
      .id = snapshot_id{id},
      .parent_snapshot_id = parent_id,
      .sequence_number = sequence_number{seq_num},
      .timestamp_ms = model::timestamp{timestamp_ms},
      .summary = std::move(summary),
      .manifest_list_path = uri(manifest_list_path),
      .schema_id = schema_id,
    };
}

snapshot_reference parse_snapshot_ref(const json::Value& v) {
    auto id = parse_required_i64(v, "snapshot-id");
    auto type_str = parse_required_str(v, "type");
    auto type = ref_type_from_str(type_str);
    auto max_ref_age_ms = parse_optional_i64(v, "max-ref-age-ms");
    auto max_snapshot_age_ms = parse_optional_i64(v, "max-snapshot-age-ms");
    auto min_snapshots_to_keep = parse_optional_i32(v, "min-snapshots-to-keep");
    return snapshot_reference{
      .snapshot_id = snapshot_id{id},
      .type = type,
      .max_ref_age_ms = max_ref_age_ms,
      .max_snapshot_age_ms = max_snapshot_age_ms,
      .min_snapshots_to_keep = min_snapshots_to_keep,
    };
}

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::snapshot& s) {
    w.StartObject();
    w.Key("snapshot-id");
    w.Int64(s.id());
    if (s.parent_snapshot_id.has_value()) {
        w.Key("parent-snapshot-id");
        w.Int64(s.parent_snapshot_id.value()());
    }
    w.Key("sequence-number");
    w.Int64(s.sequence_number());
    w.Key("timestamp-ms");
    w.Int64(s.timestamp_ms.value());
    w.Key("manifest-list");
    w.String(s.manifest_list_path());
    if (s.schema_id.has_value()) {
        w.Key("schema-id");
        w.Int(s.schema_id.value()());
    }
    w.Key("summary");
    w.StartObject();
    w.Key("operation");
    w.String(ss::sstring(iceberg::operation_to_str(s.summary.operation)));
    for (const auto& [k, v] : s.summary.other) {
        w.Key(k);
        w.String(v);
    }
    const auto maybe_serialize_metric =
      [&w](std::string_view metric_name, const std::optional<int64_t>& metric) {
          if (!metric.has_value()) {
              return;
          }
          w.Key(ss::sstring(metric_name));
          w.String(fmt::to_string(*metric));

          // TODO: is it important to make sure we don't double-serialize if
          // it's in `other`?
      };
    maybe_serialize_metric("added-data-files", s.summary.added_data_files);
    maybe_serialize_metric("total-data-files", s.summary.total_data_files);
    maybe_serialize_metric("added-records", s.summary.added_records);
    maybe_serialize_metric("total-records", s.summary.total_records);
    maybe_serialize_metric("added-files-size", s.summary.added_files_size);
    maybe_serialize_metric("total-files-size", s.summary.total_files_size);
    w.EndObject();

    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::snapshot_reference& s) {
    w.StartObject();
    serialize_snapshot_reference_properties(w, s);
    w.EndObject();
}

void serialize_snapshot_reference_properties(
  iceberg::json_writer& w, const iceberg::snapshot_reference& s) {
    w.Key("snapshot-id");
    w.Int64(s.snapshot_id());
    w.Key("type");
    w.String(ss::sstring(iceberg::ref_type_to_str(s.type)));
    if (s.max_ref_age_ms.has_value()) {
        w.Key("max-ref-age-ms");
        w.Int64(s.max_ref_age_ms.value());
    }
    if (s.max_snapshot_age_ms.has_value()) {
        w.Key("max-snapshot-age-ms");
        w.Int64(s.max_snapshot_age_ms.value());
    }
    if (s.min_snapshots_to_keep.has_value()) {
        w.Key("min-snapshots-to-keep");
        w.Int(s.min_snapshots_to_keep.value());
    }
}

} // namespace json
