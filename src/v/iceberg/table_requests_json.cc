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

#include "iceberg/json_utils.h"
#include "iceberg/partition_json.h"
#include "iceberg/schema_json.h"
#include "iceberg/table_metadata_json.h"
#include "iceberg/table_requirement_json.h"
#include "iceberg/table_update_json.h"
namespace iceberg {
load_table_result parse_load_table_result(const json::Value& value) {
    load_table_result ret{
      .metadata = parse_table_meta(parse_required(value, "metadata")),
    };
    ret.metadata_location = parse_optional_str(value, "metadata-location");
    ret.config = parse_optional_string_map(value, "config");

    auto storage_credentials = parse_optional_array(
      value, "storage-credentials");
    if (storage_credentials) {
        ret.storage_credentials.emplace();
        for (auto& sc_value : *storage_credentials) {
            iceberg::storage_credentials sc;
            sc.prefix = parse_required_str(sc_value, "prefix");
            sc.config = parse_required_string_map(sc_value, "config");

            ret.storage_credentials->push_back(std::move(sc));
        }
    }

    return ret;
}

commit_table_response parse_commit_table_response(const json::Value& value) {
    return commit_table_response{
      .metadata_location = parse_required_str(value, "metadata-location"),
      .table_metadata = parse_table_meta(parse_required(value, "metadata")),
    };
}

create_namespace_response
parse_create_namespace_response(const json::Value& value) {
    create_namespace_response ret;
    auto ns_json_array = parse_required_array(value, "namespace");
    ret.ns.reserve(ns_json_array.Size());
    for (const auto& ns_json : ns_json_array) {
        if (!ns_json.IsString()) {
            throw std::invalid_argument(fmt::format(
              "Expected 'namespace' to be string array, element is {}",
              ns_json.GetType()));
        }
        ret.ns.emplace_back(ns_json.GetString());
    }
    ret.properties = parse_optional_string_map(value, "properties");
    return ret;
}

} // namespace iceberg
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

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::create_namespace_request& req) {
    w.StartObject();
    w.Key("namespace");
    w.StartArray();
    for (const auto& n : req.ns) {
        w.String(n);
    }
    w.EndArray();
    if (req.properties.has_value()) {
        w.Key("properties");
        w.StartObject();
        for (const auto& [k, v] : *req.properties) {
            w.Key(k);
            w.String(v);
        }
        w.EndObject();
    }
    w.EndObject();
}

} // namespace json
