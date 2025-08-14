/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/table_update_applier.h"

#include "base/vlog.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "iceberg/logger.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"

namespace iceberg::table_update {

namespace {

struct update_applying_visitor {
    explicit update_applying_visitor(table_metadata& meta)
      : meta(meta) {}
    table_metadata& meta;

    outcome operator()(const add_schema& update) {
        auto sid = update.schema.schema_id;
        auto s = std::ranges::find(meta.schemas, sid, &schema::schema_id);
        if (s != meta.schemas.end()) {
            vlog(log.error, "Schema {} already exists", sid);
            return outcome::unexpected_state;
        }
        if (update.last_column_id.has_value()) {
            auto new_last_col_id = update.last_column_id.value();
            // NOTE: we can drop fields, so new_last_col_id could be less
            // than meta.last_column_id (e.g. if some field was dropped and
            // no new fields were added). we should still enforce that
            // meta.last_column_id is always increasing monotonically to avoid
            // reusing column IDs on subsequent schema updates
            meta.last_column_id = std::max(
              meta.last_column_id, new_last_col_id);
        }
        meta.schemas.emplace_back(update.schema.copy());
        return outcome::success;
    }
    outcome operator()(const set_current_schema& update) {
        auto sid = update.schema_id;
        if (sid() == set_current_schema::last_added) {
            // -1 indicates that we should set the schema to the latest one.
            if (meta.schemas.empty()) {
                vlog(log.error, "Can't set -1 when there are no schemas");
                return outcome::unexpected_state;
            }
            auto max_id = meta.schemas.front().schema_id;
            for (const auto& s : meta.schemas) {
                max_id = std::max(max_id, s.schema_id);
            }
            meta.current_schema_id = max_id;
            return outcome::success;
        }
        auto s = std::ranges::find(meta.schemas, sid, &schema::schema_id);
        if (s == meta.schemas.end()) {
            vlog(log.error, "Schema {} doesn't exist", sid);
            return outcome::unexpected_state;
        }
        meta.current_schema_id = update.schema_id;
        return outcome::success;
    }
    outcome operator()(const add_spec& update) {
        auto sid = update.spec.spec_id;
        auto s = std::ranges::find(
          meta.partition_specs, sid, &partition_spec::spec_id);
        if (s != meta.partition_specs.end()) {
            vlog(log.error, "Partition spec id {} already exists", sid);
            return outcome::unexpected_state;
        }

        meta.partition_specs.emplace_back(update.spec.copy());

        for (const auto& field : update.spec.fields) {
            meta.last_partition_id = std::max(
              meta.last_partition_id, field.field_id);
        }

        return outcome::success;
    }
    outcome operator()(const set_default_spec& update) {
        auto sid = update.spec_id;
        if (sid() == partition_spec::unassigned_id) {
            // -1 indicates that we should set the spec to the latest added one.
            if (meta.partition_specs.empty()) {
                vlog(
                  log.error, "Can't set -1 when there are no partition specs");
                return outcome::unexpected_state;
            }
            auto max_id = meta.partition_specs.front().spec_id;
            for (const auto& s : meta.partition_specs) {
                max_id = std::max(max_id, s.spec_id);
            }
            meta.default_spec_id = max_id;
            return outcome::success;
        }
        auto spec = std::ranges::find(
          meta.partition_specs, sid, &partition_spec::spec_id);
        if (spec == meta.partition_specs.end()) {
            vlog(log.error, "Partition spec {} doesn't exist", sid);
            return outcome::unexpected_state;
        }
        meta.default_spec_id = update.spec_id;
        return outcome::success;
    }
    outcome operator()(const add_snapshot& update) {
        auto sid = update.snapshot.id;
        if (!meta.snapshots.has_value()) {
            meta.snapshots.emplace();
        }
        auto s = std::ranges::find(*meta.snapshots, sid, &snapshot::id);
        if (s != meta.snapshots->end()) {
            vlog(log.error, "Snapshot id {} already exists", sid);
            return outcome::unexpected_state;
        }
        if (update.snapshot.sequence_number <= meta.last_sequence_number) {
            vlog(
              log.error,
              "New sequence number must be higher than last: {} <= {}",
              update.snapshot.sequence_number,
              meta.last_sequence_number);
            return outcome::unexpected_state;
        }

        meta.snapshots->emplace_back(update.snapshot);
        meta.last_sequence_number = update.snapshot.sequence_number;
        return outcome::success;
    }
    outcome operator()(const remove_snapshots& update) {
        if (!meta.snapshots.has_value()) {
            return outcome::success;
        }
        chunked_hash_set<snapshot_id> to_remove;
        for (const auto& id : update.snapshot_ids) {
            to_remove.emplace(id);
        }
        chunked_vector<snapshot> new_list;
        new_list.reserve(meta.snapshots->size());
        for (auto& snap : *meta.snapshots) {
            if (to_remove.contains(snap.id)) {
                continue;
            }
            new_list.emplace_back(std::move(snap));
        }
        meta.snapshots = std::move(new_list);
        // TODO: once we add support for statistics, need to remove them too.
        return outcome::success;
    }
    outcome operator()(const remove_snapshot_ref& update) {
        if (update.ref_name == "main") {
            meta.current_snapshot_id.reset();
            // Intentional fallthrough to remove from the refs container.
        }
        if (!meta.refs.has_value() || meta.refs->empty()) {
            return outcome::success;
        }
        auto ref_it = meta.refs->find(update.ref_name);
        if (ref_it != meta.refs->end()) {
            meta.refs->erase(ref_it);
        }
        return outcome::success;
    }
    outcome operator()(const set_snapshot_ref& update) {
        auto sid = update.ref.snapshot_id;
        if (!meta.snapshots.has_value()) {
            vlog(log.error, "No snapshots exist, looking for {}", sid);
            return outcome::unexpected_state;
        }
        auto s = std::ranges::find(*meta.snapshots, sid, &snapshot::id);
        if (s == meta.snapshots->end()) {
            vlog(log.error, "Snapshot id {} doesn't exist", sid);
            return outcome::unexpected_state;
        }
        if (!meta.refs.has_value()) {
            meta.refs.emplace();
        }
        if (update.ref_name == "main") {
            meta.current_snapshot_id = sid;
            meta.last_updated_ms = model::timestamp::now();
        }
        auto ref_iter = meta.refs->find(update.ref_name);
        if (ref_iter == meta.refs->end()) {
            meta.refs->emplace(update.ref_name, update.ref);
        } else {
            ref_iter->second = update.ref;
        }
        meta.refs->emplace(update.ref_name, update.ref);
        return outcome::success;
    }
};

} // namespace

outcome apply(const update& update, table_metadata& meta) {
    return std::visit(update_applying_visitor{meta}, update);
}

} // namespace iceberg::table_update
