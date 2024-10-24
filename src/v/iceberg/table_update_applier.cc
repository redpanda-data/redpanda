// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/table_update_applier.h"

#include "base/vlog.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
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
            if (new_last_col_id < meta.last_column_id) {
                vlog(
                  log.error,
                  "Expected new last column id to be >= previous last column "
                  "id: {} < {}",
                  new_last_col_id,
                  meta.last_column_id);
                return outcome::unexpected_state;
            }
            meta.last_column_id = new_last_col_id;
        }
        meta.schemas.emplace_back(update.schema.copy());
        return outcome::success;
    }
    outcome operator()(const set_current_schema& update) {
        auto sid = update.schema_id;
        if (sid() == schema::unassigned_id) {
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
