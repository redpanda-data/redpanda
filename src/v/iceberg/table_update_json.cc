/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/table_update_json.h"

#include "iceberg/partition_json.h"
#include "iceberg/schema_json.h"
#include "iceberg/snapshot_json.h"

namespace {
struct table_update_json_serializing_visitor {
    void operator()(const iceberg::table_update::add_schema& update) {
        serialize_action("add-schema");
        w.Key("schema");
        json::rjson_serialize(w, update.schema);
        if (update.last_column_id) {
            w.Key("last-column-id");
            w.Int(update.last_column_id.value());
        }
    }
    void operator()(const iceberg::table_update::set_current_schema& update) {
        serialize_action("set-current-schema");
        w.Key("schema-id");
        w.Int(update.schema_id);
    }
    void operator()(const iceberg::table_update::add_spec& update) {
        serialize_action("add-spec");
        w.Key("spec");
        json::rjson_serialize(w, update.spec);
    }
    void operator()(const iceberg::table_update::add_snapshot& update) {
        serialize_action("add-snapshot");
        w.Key("snapshot");
        json::rjson_serialize(w, update.snapshot);
    }
    void operator()(const iceberg::table_update::remove_snapshots& update) {
        serialize_action("remove-snapshots");
        w.Key("snapshot-ids");
        w.StartArray();
        for (const auto& id : update.snapshot_ids) {
            w.Int64(id);
        }
        w.EndArray();
    }
    void operator()(const iceberg::table_update::set_snapshot_ref& update) {
        serialize_action("set-snapshot-ref");
        w.Key("ref-name");
        w.String(update.ref_name);
        json::serialize_snapshot_reference_properties(w, update.ref);
    }

    void serialize_action(std::string_view type) {
        w.Key("action");
        w.String(type.data());
    }

    iceberg::json_writer& w;
};

} // namespace
namespace json {
void rjson_serialize(
  iceberg::json_writer& w, const iceberg::table_update::update& req) {
    w.StartObject();
    std::visit(table_update_json_serializing_visitor{.w = w}, req);
    w.EndObject();
}
} // namespace json
