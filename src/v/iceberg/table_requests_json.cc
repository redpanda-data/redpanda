/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/table_requests_json.h"

#include "iceberg/partition_json.h"
#include "iceberg/schema_json.h"
#include "iceberg/table_requirement_json.h"
#include "iceberg/table_update_json.h"

namespace json {

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::create_table_request& r) {
    w.StartObject();

    w.Key("name");
    w.String(r.name);

    w.Key("schema");
    rjson_serialize(w, r.schema);

    if (r.location.has_value()) {
        w.Key("location");
        w.String(r.location.value());
    }

    if (r.partition_spec.has_value()) {
        w.Key("partition-spec");
        rjson_serialize(w, r.partition_spec.value());
    }

    if (r.stage_create.has_value()) {
        w.Key("stage-create");
        w.Bool(r.stage_create.value());
    }

    if (r.properties.has_value()) {
        w.Key("properties");
        w.StartObject();
        for (const auto& [k, v] : r.properties.value()) {
            w.Key(k);
            w.String(v);
        }
        w.EndObject();
    }

    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::table_identifier& r) {
    w.StartObject();
    w.Key("name");
    w.String(r.table);
    w.Key("namespace");
    w.StartArray();
    for (const auto& n : r.ns) {
        w.String(n);
    }
    w.EndArray();
    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::commit_table_request& req) {
    w.StartObject();
    w.Key("identifier");
    rjson_serialize(w, req.identifier);
    w.Key("requirements");
    w.StartArray();
    for (const auto& requirement : req.requirements) {
        rjson_serialize(w, requirement);
    }
    w.EndArray();
    w.Key("updates");
    w.StartArray();
    for (const auto& update : req.updates) {
        rjson_serialize(w, update);
    }
    w.EndArray();
    w.EndObject();
}

} // namespace json
