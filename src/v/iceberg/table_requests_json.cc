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

} // namespace json
