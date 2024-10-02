// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/schema_json.h"

#include "iceberg/datatypes.h"
#include "iceberg/datatypes_json.h"
#include "iceberg/json_utils.h"
#include "iceberg/schema.h"
#include "json/document.h"

#include <absl/container/btree_set.h>

namespace iceberg {

schema parse_schema(const json::Value& v) {
    auto type = parse_required_str(v, "type");
    if (type != "struct") {
        throw std::invalid_argument(
          fmt::format("Schema has type '{}' instead of 'struct'", type));
    }
    int32_t schema_id = parse_required_i32(v, "schema-id");
    auto identifier_fids_json = parse_optional(v, "identifier-field-ids");
    absl::btree_set<nested_field::id_t> identifier_fids;
    if (identifier_fids_json.has_value()) {
        if (!identifier_fids_json->get().IsArray()) {
            throw std::invalid_argument(
              fmt::format("Schema has type '{}' instead of 'array'", type));
        }
        for (const auto& id_json : identifier_fids_json->get().GetArray()) {
            if (!id_json.IsInt()) {
                throw std::invalid_argument(fmt::format(
                  "Schema has non-int 'identifier-field-ids' type: {}",
                  id_json.GetType()));
            }
            identifier_fids.emplace(id_json.GetInt());
        }
    }
    auto struct_type = parse_struct(v);
    return schema{
      .schema_struct = std::move(struct_type),
      .schema_id = schema::id_t{schema_id},
      .identifier_field_ids = std::move(identifier_fids),
    };
}

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::schema& s) {
    w.StartObject();
    w.Key("type");
    w.String("struct");

    w.Key("schema-id");
    w.Int(s.schema_id());
    if (!s.identifier_field_ids.empty()) {
        w.Key("identifier-field-ids");
        w.StartArray();
        for (auto id : s.identifier_field_ids) {
            w.Int(id());
        }
        w.EndArray();
    }
    w.Key("fields");
    w.StartArray();
    for (const auto& f : s.schema_struct.fields) {
        rjson_serialize(w, *f);
    }
    w.EndArray();
    w.EndObject();
}

} // namespace json
