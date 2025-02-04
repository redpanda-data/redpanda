/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/iceberg_file_committer.h"

#include "base/vlog.h"
#include "container/fragmented_vector.h"
#include "datalake/coordinator/commit_offset_metadata.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/location.h"
#include "datalake/logger.h"
#include "datalake/table_id_provider.h"
#include "iceberg/catalog.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/partition_key.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/values.h"
#include "iceberg/values_bytes.h"

#include <optional>

namespace datalake::coordinator {
namespace {

file_committer::errc
log_and_convert_catalog_errc(iceberg::catalog::errc e, std::string_view msg) {
    switch (e) {
        using enum iceberg::catalog::errc;
    case shutting_down:
        vlog(datalake_log.debug, "{}: {}", msg, e);
        return file_committer::errc::shutting_down;
    case timedout:
    case io_error:
    case unexpected_state:
    case already_exists:
    case not_found:
        vlog(datalake_log.warn, "{}: {}", msg, e);
        return file_committer::errc::failed;
    }
}
file_committer::errc
log_and_convert_action_errc(iceberg::action::errc e, std::string_view msg) {
    switch (e) {
        using enum iceberg::action::errc;
    case shutting_down:
        vlog(datalake_log.debug, "{}: {}", msg, e);
        return file_committer::errc::shutting_down;
    case unexpected_state:
    case io_failed:
        vlog(datalake_log.warn, "{}: {}", msg, e);
        return file_committer::errc::failed;
    }
}

constexpr auto commit_meta_prop = "redpanda.commit-metadata";
// Look for the Redpanda commit property in the current snapshot, or the most
// recent ancestor if none.
checked<std::optional<model::offset>, parse_offset_error>
get_iceberg_committed_offset(const iceberg::table_metadata& table) {
    if (!table.current_snapshot_id.has_value()) {
        return std::nullopt;
    }
    if (!table.snapshots.has_value()) {
        return std::nullopt;
    }

    const auto& cur_snap_id = *table.current_snapshot_id;
    auto snap_it = std::ranges::find(
      table.snapshots.value(), cur_snap_id, &iceberg::snapshot::id);
    while (snap_it != table.snapshots->end()) {
        const auto& snap = *snap_it;
        const auto& props = snap.summary.other;
        auto prop_it = props.find(commit_meta_prop);
        if (prop_it != props.end()) {
            auto res = parse_commit_offset_json(prop_it->second);
            if (res.has_error()) {
                return res.error();
            }
            return res.value().offset;
        }

        if (!snap.parent_snapshot_id.has_value()) {
            return std::nullopt;
        }
        snap_it = std::ranges::find(
          table.snapshots.value(),
          *snap.parent_snapshot_id,
          &iceberg::snapshot::id);
    }
    return std::nullopt;
}

checked<iceberg::struct_value, file_committer::errc> build_partition_key_struct(
  const model::topic& topic,
  const iceberg::table_metadata& table,
  const data_file& f,
  const iceberg::schema& schema) {
    auto pspec_id = iceberg::partition_spec::id_t{f.partition_spec_id};
    auto partition_spec = table.get_partition_spec(pspec_id);
    if (!partition_spec) {
        vlog(
          datalake_log.error,
          "can't find partition spec {} in table for topic {}",
          pspec_id,
          topic);
        return file_committer::errc::failed;
    }

    if (partition_spec->fields.size() != f.partition_key.size()) {
        vlog(
          datalake_log.error,
          "[topic: {}, file: {}] partition key size {} doesn't "
          "match spec size {} (spec id: {})",
          topic,
          f.remote_path,
          f.partition_key.size(),
          partition_spec->fields.size(),
          pspec_id);
        return file_committer::errc::failed;
    }

    auto key_type = iceberg::partition_key_type::create(
      *partition_spec, schema);

    iceberg::struct_value pk;

    for (size_t i = 0; i < partition_spec->fields.size(); ++i) {
        const auto& field_type = key_type.type.fields.at(i);
        const auto& field_bytes = f.partition_key.at(i);
        if (field_bytes) {
            try {
                pk.fields.push_back(iceberg::value_from_bytes(
                  field_type->type, field_bytes.value()));
            } catch (const std::invalid_argument& e) {
                vlog(
                  datalake_log.error,
                  "[topic: {}, file: {}] failed to parse "
                  "partition key field {} (type: {}): {}",
                  topic,
                  f.remote_path,
                  i,
                  field_type->type,
                  e);
                return file_committer::errc::failed;
            }
        } else {
            pk.fields.push_back(std::nullopt);
        }
    }

    return pk;
}

checked<iceberg::struct_value, file_committer::errc> build_partition_key_struct(
  const model::topic& topic,
  const iceberg::table_metadata& table,
  const data_file& f) {
    if (f.table_schema_id < 0) {
        // File created by a legacy Redpanda version that only
        // supported hourly partitioning, the partition key value is
        // in the hour_deprecated field.
        iceberg::struct_value pk;
        pk.fields.emplace_back(iceberg::int_value(f.hour_deprecated));
        return pk;
    } else {
        auto schema = table.get_schema(
          iceberg::schema::id_t{f.table_schema_id});
        if (!schema) {
            vlog(
              datalake_log.error,
              "can't find schema {} in table for topic {}",
              f.table_schema_id,
              topic);
            return file_committer::errc::failed;
        }

        return build_partition_key_struct(topic, table, f, *schema);
    }
}

checked<iceberg::partition_key, file_committer::errc> build_partition_key(
  const model::topic& topic,
  const iceberg::table_metadata& table,
  const data_file& f) {
    auto pk_res = build_partition_key_struct(topic, table, f);
    if (pk_res.has_error()) {
        return pk_res.error();
    }
    return iceberg::partition_key{
      std::make_unique<iceberg::struct_value>(std::move(pk_res.value()))};
}

/// A table_commit_builder accumulates files to commit to an Iceberg table and
/// manages the commit process.
///
/// A file_committer will usually create one or more `table_commit_builder`
/// instances per topic, and will call process_pending_entry for each pending
/// entry in the topic's state. Once all pending entries have been processed,
/// the file_committer must call commit to commit the files to the Iceberg
/// table.
class table_commit_builder {
public:
    static checked<table_commit_builder, file_committer::errc> create(
      iceberg::table_identifier table_id,
      iceberg::table_metadata&& table,
      scoped_location table_location) {
        auto meta_res = get_iceberg_committed_offset(table);
        if (meta_res.has_error()) {
            vlog(
              datalake_log.warn,
              "Error getting snapshot property '{}' for table {}: {}",
              commit_meta_prop,
              table_id,
              meta_res.error());
            return file_committer::errc::failed;
        }

        return table_commit_builder(
          std::move(table_id),
          std::move(table),
          std::move(table_location),
          meta_res.value());
    }

public:
    checked<std::nullopt_t, file_committer::errc> process_pending_entry(
      const model::topic& topic,
      model::revision_id topic_revision,
      const model::offset added_pending_at,
      const chunked_vector<data_file>& files) {
        if (should_skip_entry(added_pending_at)) {
            // This entry was committed to the Iceberg table already.
            // Intentionally collect the pending commit above so we can
            // replicate the fact that it was committed previously, but
            // don't construct a data_file to send to Iceberg as it is
            // already committed.
            vlog(
              datalake_log.debug,
              "Skipping entry for topic {} revision {} added at "
              "coordinator offset {} because table {} has data including "
              "coordinator offset {}",
              topic,
              topic_revision,
              added_pending_at,
              table_id_,
              table_commit_offset_);
        } else {
            for (const auto& f : files) {
                auto pk = build_partition_key(topic, table_, f);
                if (pk.has_error()) {
                    return pk.error();
                }

                auto file_uri = table_location_
                                  .replace_path(
                                    std::filesystem::path(f.remote_path))
                                  .to_uri();
                vlog(
                  datalake_log.trace,
                  "Adding file {} to Iceberg table {} {}",
                  f.remote_path,
                  table_id_,
                  file_uri());

                // TODO: pass schema_id and pspec_id to merge_append_action
                // (currently it assumes that the files were serialized with the
                // current schema and a single partition spec).
                icb_files_.push_back({
                  .content_type = iceberg::data_file_content_type::data,
                  .file_path = file_uri,
                  .file_format = iceberg::data_file_format::parquet,
                  .partition = std::move(pk.value()),
                  .record_count = f.row_count,
                  .file_size_bytes = f.file_size_bytes,
                });
            }
        }

        new_committed_offset_ = std::max(
          new_committed_offset_,
          std::make_optional<model::offset>(added_pending_at));

        return std::nullopt;
    }

    ss::future<checked<std::nullopt_t, file_committer::errc>> commit(
      const model::topic& topic,
      model::revision_id topic_revision,
      iceberg::catalog& catalog,
      iceberg::manifest_io& io) && {
        if (icb_files_.empty()) {
            // No new files to commit.
            vlog(
              datalake_log.debug,
              "All committed files were deduplicated for topic {} revision {}, "
              "returning without updating Iceberg catalog",
              topic,
              topic_revision);
            co_return std::nullopt;
        }

        vassert(
          new_committed_offset_.has_value(),
          "New Iceberg files implies new commit metadata");
        const auto commit_meta = commit_offset_metadata{
          .offset = *new_committed_offset_,
        };

        vlog(
          datalake_log.debug,
          "Adding {} files to Iceberg table {}",
          icb_files_.size(),
          table_id_);
        iceberg::transaction txn(std::move(table_));
        auto icb_append_res = co_await txn.merge_append(
          io,
          std::move(icb_files_),
          {{commit_meta_prop, to_json_str(commit_meta)}});
        if (icb_append_res.has_error()) {
            co_return log_and_convert_action_errc(
              icb_append_res.error(),
              fmt::format(
                "Iceberg merge append failed for table {}", table_id_));
        }
        auto icb_commit_res = co_await catalog.commit_txn(
          table_id_, std::move(txn));
        if (icb_commit_res.has_error()) {
            co_return log_and_convert_catalog_errc(
              icb_commit_res.error(),
              fmt::format(
                "Iceberg transaction did not commit to table {}", table_id_));
        }

        co_return std::nullopt;
    }

private:
    table_commit_builder(
      iceberg::table_identifier table_id,
      iceberg::table_metadata&& table,
      datalake::scoped_location table_location,
      std::optional<model::offset> table_commit_offset)
      : table_id_(std::move(table_id))
      , table_(std::move(table))
      , table_location_(std::move(table_location))
      , table_commit_offset_(table_commit_offset) {}

private:
    bool should_skip_entry(model::offset added_pending_at) const {
        return table_commit_offset_.has_value()
               && added_pending_at <= *table_commit_offset_;
    }

private:
    iceberg::table_identifier table_id_;
    iceberg::table_metadata table_;
    datalake::scoped_location table_location_;
    std::optional<model::offset> table_commit_offset_;

    // State accumulated.
    chunked_vector<iceberg::data_file> icb_files_;
    std::optional<model::offset> new_committed_offset_;
};

} // namespace

ss::future<
  checked<chunked_vector<mark_files_committed_update>, file_committer::errc>>
iceberg_file_committer::commit_topic_files_to_catalog(
  model::topic topic, const topics_state& state) const {
    vlog(datalake_log.debug, "Beginning commit for topic {}", topic);
    auto tp_it = state.topic_to_state.find(topic);
    if (
      tp_it == state.topic_to_state.end()
      || !tp_it->second.has_pending_entries()) {
        vlog(datalake_log.debug, "Topic {} has no pending entries", topic);
        co_return chunked_vector<mark_files_committed_update>{};
    }
    auto topic_revision = tp_it->second.revision;

    // Main table (may not exist if all records so far were invalid and the
    // schema couldn't be determined):
    std::optional<table_commit_builder> main_table_commit_builder;
    {
        auto main_table_id = table_id_provider::table_id(topic);
        auto main_table_res = co_await catalog_.load_table(main_table_id);
        if (main_table_res.has_error()) {
            switch (main_table_res.error()) {
            case iceberg::catalog::errc::not_found:
                vlog(
                  datalake_log.debug,
                  "Main table {} not found for committing from topic {}",
                  main_table_id,
                  topic);
                break;
            default:
                co_return log_and_convert_catalog_errc(
                  main_table_res.error(),
                  fmt::format(
                    "Error loading table {} for committing from topic {}",
                    main_table_id,
                    topic));
            }
        } else {
            auto table_location = loc_provider_.make_scoped_location(
              main_table_res.value().location);
            if (!table_location) {
                vlog(
                  datalake_log.error,
                  "Failed to resolve location for table {} {}",
                  main_table_id,
                  main_table_res.value().location);
                co_return file_committer::errc::failed;
            }

            auto main_table_commit_builder_res = table_commit_builder::create(
              std::move(main_table_id),
              std::move(main_table_res.value()),
              std::move(*table_location));
            if (main_table_commit_builder_res.has_error()) {
                co_return main_table_commit_builder_res.error();
            }
            main_table_commit_builder = std::move(
              main_table_commit_builder_res.value());
        }
    }

    // DLQ table (optional):
    std::optional<table_commit_builder> dlq_table_commit_builder;
    {
        // TODO(iceberg-dlq): Load it on demand.
        //  For now, we load it unconditionally because once we start processing
        //  pending entries we are not allowed to suspend/co_await.
        auto dlq_table_id = table_id_provider::dlq_table_id(topic);
        auto dlq_table_res = co_await catalog_.load_table(dlq_table_id);
        if (dlq_table_res.has_error()) {
            switch (dlq_table_res.error()) {
            case iceberg::catalog::errc::not_found:
                vlog(
                  datalake_log.debug,
                  "DLQ table {} not found for committing from topic {}",
                  dlq_table_id,
                  topic);
                // We ignore the DLQ table not found error and continue under
                // the assumption that there are will be no DLQ files to commit.
                // If such a file is found we'll return an error. Ideally we
                // would load the table conditionally but for now we can't
                // suspend/co_await here.
                break;
            default:
                co_return log_and_convert_catalog_errc(
                  dlq_table_res.error(),
                  fmt::format(
                    "Error loading table {} for committing from topic {}",
                    dlq_table_id,
                    topic));
            }
        } else {
            auto table_location = loc_provider_.make_scoped_location(
              dlq_table_res.value().location);
            if (!table_location) {
                vlog(
                  datalake_log.error,
                  "Failed to resolve location for table {} {}",
                  dlq_table_id,
                  dlq_table_res.value().location);
                co_return file_committer::errc::failed;
            }

            auto dlq_table_commit_builder_res = table_commit_builder::create(
              std::move(dlq_table_id),
              std::move(dlq_table_res.value()),
              std::move(*table_location));
            if (dlq_table_commit_builder_res.has_error()) {
                co_return dlq_table_commit_builder_res.error();
            }
            dlq_table_commit_builder = std::move(
              dlq_table_commit_builder_res.value());
        }
    }

    // update the iterator after a scheduling point
    tp_it = state.topic_to_state.find(topic);
    if (
      tp_it == state.topic_to_state.end()
      || tp_it->second.revision != topic_revision) {
        vlog(
          datalake_log.debug,
          "Commit returning early, topic {} state is missing or revision has "
          "changed: current {} vs expected {}",
          topic,
          tp_it->second.revision,
          topic_revision);
        co_return chunked_vector<mark_files_committed_update>{};
    }

    chunked_hash_map<model::partition_id, kafka::offset> pending_commits;
    const auto& tp_state = tp_it->second;
    for (const auto& [pid, p_state] : tp_state.pid_to_pending_files) {
        for (const auto& e : p_state.pending_entries) {
            pending_commits[pid] = e.data.last_offset;

            if (!e.data.files.empty()) {
                if (!main_table_commit_builder) {
                    vlog(
                      datalake_log.error,
                      "Pending files for topic {} revision {} but no main "
                      "table",
                      topic,
                      topic_revision);
                    co_return file_committer::errc::failed;
                }

                auto res = main_table_commit_builder->process_pending_entry(
                  topic, topic_revision, e.added_pending_at, e.data.files);
                if (res.has_error()) {
                    co_return res.error();
                }
            }

            if (!e.data.dlq_files.empty()) {
                if (!dlq_table_commit_builder) {
                    vlog(
                      datalake_log.error,
                      "Pending DLQ files for topic {} revision {} but no DLQ "
                      "table",
                      topic,
                      topic_revision);
                    co_return file_committer::errc::failed;
                }

                auto dlq_res = dlq_table_commit_builder->process_pending_entry(
                  topic, topic_revision, e.added_pending_at, e.data.dlq_files);
                if (dlq_res.has_error()) {
                    co_return dlq_res.error();
                }
            }
        }
    }
    if (pending_commits.empty()) {
        vlog(
          datalake_log.debug,
          "No new data to mark committed for topic {} revision {}",
          topic,
          topic_revision);
        co_return chunked_vector<mark_files_committed_update>{};
    }
    chunked_vector<mark_files_committed_update> updates;
    for (const auto& [pid, committed_offset] : pending_commits) {
        auto tp = model::topic_partition(topic, pid);
        auto update_res = mark_files_committed_update::build(
          state, tp, topic_revision, committed_offset);
        if (update_res.has_error()) {
            vlog(
              datalake_log.warn,
              "Could not build STM update for committing topic {} revision {}: "
              "{}",
              topic,
              topic_revision,
              update_res.error());
            co_return errc::failed;
        }
        updates.emplace_back(std::move(update_res.value()));
    }

    if (dlq_table_commit_builder) {
        auto dlq_commit_res = co_await std::move(*dlq_table_commit_builder)
                                .commit(topic, topic_revision, catalog_, io_);
        if (dlq_commit_res.has_error()) {
            co_return dlq_commit_res.error();
        }
    }

    if (main_table_commit_builder) {
        auto main_table_commit_res
          = co_await std::move(*main_table_commit_builder)
              .commit(topic, topic_revision, catalog_, io_);
        if (main_table_commit_res.has_error()) {
            co_return main_table_commit_res.error();
        }
    }

    co_return updates;
}

ss::future<checked<std::nullopt_t, file_committer::errc>>
iceberg_file_committer::drop_table(
  const iceberg::table_identifier& table_id) const {
    auto drop_res = co_await catalog_.drop_table(table_id, true);
    if (
      drop_res.has_error()
      && drop_res.error() != iceberg::catalog::errc::not_found) {
        co_return log_and_convert_catalog_errc(
          drop_res.error(), fmt::format("Failed to drop {}", table_id));
    }
    co_return std::nullopt;
}
} // namespace datalake::coordinator
