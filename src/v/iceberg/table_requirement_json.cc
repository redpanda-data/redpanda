/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/table_requirement_json.h"

namespace {

struct requirement_json_serializing_visitor {
    void operator()(const iceberg::table_requirement::assert_create&) {
        serialize_type("assert-create");
    }

    void operator()(
      const iceberg::table_requirement::assert_current_schema_id& req) {
        serialize_type("assert-current-schema-id");
        w.Key("current-schema-id");
        w.Int(req.current_schema_id);
    }
    void
    operator()(const iceberg::table_requirement::assert_ref_snapshot_id& req) {
        serialize_type("assert-ref-snapshot-id");
        w.Key("ref");
        w.String(req.ref);
        if (req.snapshot_id) {
            w.Key("snapshot-id");
            w.Int64(req.snapshot_id.value());
        }
    }

    void operator()(const iceberg::table_requirement::assert_table_uuid& req) {
        serialize_type("assert-table-uuid");
        w.Key("uuid");
        w.String(fmt::to_string(req.uuid));
    }

    void operator()(
      const iceberg::table_requirement::last_assigned_field_match& req) {
        serialize_type("assert-last-assigned-field-id");
        w.Key("last-assigned-field-id");
        w.Int(req.last_assigned_field_id);
    }
    void operator()(
      const iceberg::table_requirement::assert_last_assigned_partition_id&
        req) {
        serialize_type("assert-last-assigned-partition-id");
        w.Key("last-assigned-partition-id");
        w.Int(req.last_assigned_partition_id);
    }

    void serialize_type(std::string_view type) {
        w.Key("type");
        w.String(type.data());
    }

    iceberg::json_writer& w;
};

} // namespace

namespace json {
void rjson_serialize(
  iceberg::json_writer& w, const iceberg::table_requirement::requirement& req) {
    w.StartObject();
    std::visit(requirement_json_serializing_visitor{w}, req);
    w.EndObject();
}
} // namespace json
