// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/partition_json.h"

#include "iceberg/json_utils.h"
#include "iceberg/partition.h"
#include "iceberg/transform_json.h"
#include "json/document.h"

#include <stdexcept>

namespace iceberg {

partition_field parse_partition_field(const json::Value& v) {
    auto source_id = parse_required_i32(v, "source-id");
    auto field_id = parse_required_i32(v, "field-id");
    auto name = parse_required_str(v, "name");
    auto transform_str = parse_required_str(v, "transform");
    auto transform = transform_from_str(transform_str);
    return partition_field{
      .source_id = nested_field::id_t{source_id},
      .field_id = partition_field::id_t{field_id},
      .name = name,
      .transform = transform,
    };
}

partition_spec parse_partition_spec(const json::Value& v) {
    auto spec_id = parse_required_i32(v, "spec-id");
    auto fields_array = parse_required_array(v, "fields");
    chunked_vector<partition_field> fs;
    fs.reserve(fields_array.Size());
    for (const auto& f : fields_array) {
        fs.emplace_back(parse_partition_field(f));
    }
    return partition_spec{
      .spec_id = partition_spec::id_t{spec_id},
      .fields = std::move(fs),
    };
}

} // namespace iceberg

namespace json {

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::partition_field& m) {
    w.StartObject();
    w.Key("source-id");
    w.Int(m.source_id());
    w.Key("field-id");
    w.Int(m.field_id());
    w.Key("name");
    w.String(m.name);
    w.Key("transform");
    w.String(transform_to_str(m.transform));
    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::partition_spec& m) {
    w.StartObject();
    w.Key("spec-id");
    w.Int(m.spec_id());
    w.Key("fields");
    w.StartArray();
    for (const auto& f : m.fields) {
        rjson_serialize(w, f);
    }
    w.EndArray();
    w.EndObject();
}

} // namespace json
