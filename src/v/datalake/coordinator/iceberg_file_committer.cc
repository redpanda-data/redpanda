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
#include "container/chunked_vector.h"
#include "datalake/coordinator/commit_offset_metadata.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/state_update.h"
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
#include "storage/api.h"

#include <exception>
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
constexpr auto commit_tag_name = "redpanda.tag";

// Look for the Redpanda commit property in the current snapshot, or the most
// recent ancestor if none.
checked<std::optional<model::offset>, parse_offset_error>
get_iceberg_committed_offset(
  const iceberg::table_metadata& table, const model::cluster_uuid& cluster) {
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
            const auto& meta = res.value();
            // If there is no cluster UUID in the metadata (it was written from
            // an older version of Redpanda), assume it's from this cluster,
            // given cluster restore events are rare.
            // Otherwise, we should only honor the metadata if it was from this
            // cluster, since it refers to an offset of this cluster's datalake
            // control topic.
            if (!meta.cluster.has_value() || *meta.cluster == cluster) {
                return meta.offset;
            }
            // The metadata wasn't written by this cluster. Keep looking for
            // some metadata that was.
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
                pk.fields.push_back(
                  iceberg::value_from_bytes(
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
      const model::cluster_uuid& cluster,
      iceberg::table_identifier table_id,
      iceberg::table_metadata&& table,
      bool with_tag) {
        auto meta_res = get_iceberg_committed_offset(table, cluster);
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
          cluster,
          std::move(table_id),
          std::move(table),
          meta_res.value(),
          with_tag);
    }

public:
    checked<std::nullopt_t, file_committer::errc> process_pending_entry(
      const model::topic& topic,
      model::revision_id topic_revision,
      const iceberg::manifest_io& io,
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

                iceberg::data_file file{
                  .content_type = iceberg::data_file_content_type::data,
                  .file_path = io.to_uri(std::filesystem::path(f.remote_path)),
                  .file_format = iceberg::data_file_format::parquet,
                  .partition = std::move(pk.value()),
                  .record_count = f.row_count,
                  .file_size_bytes = f.file_size_bytes,
                };
                // For files created by a legacy Redpanda version that only
                // supported hourly partitioning, choose current schema and
                // spec ids.
                auto schema_id = f.table_schema_id >= 0
                                   ? iceberg::schema::id_t{f.table_schema_id}
                                   : table_.current_schema_id;
                auto pspec_id
                  = f.partition_spec_id >= 0
                      ? iceberg::partition_spec::id_t{f.partition_spec_id}
                      : table_.default_spec_id;
                icb_files_.push_back(
                  iceberg::file_to_append{
                    .file = std::move(file),
                    .schema_id = schema_id,
                    .partition_spec_id = pspec_id,
                  });
            }
        }

        new_committed_offset_ = std::max(
          new_committed_offset_,
          std::make_optional<model::offset>(added_pending_at));

        return std::nullopt;
    }

    ss::future<checked<iceberg::table_metadata, file_committer::errc>> commit(
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
            co_return table_.copy();
        }

        vassert(
          new_committed_offset_.has_value(),
          "New Iceberg files implies new commit metadata");
        const auto commit_meta = commit_offset_metadata{
          .offset = *new_committed_offset_,
          .cluster = cluster_,
        };

        vlog(
          datalake_log.debug,
          "Adding {} files to Iceberg table {}",
          icb_files_.size(),
          table_id_);
        // NOTE: a non-expiring tag is added to the new snapshot to ensure that
        // snapshot expiration doesn't clear this snapshot and its commit
        // metadata properties. The tag ensures that we retain them e.g. in
        // case of external table updates and low snapshot retention.
        auto tag_name = with_snapshot_tag_
                          ? std::make_optional<ss::sstring>(commit_tag_name)
                          : std::nullopt;
        auto tag_expiry_ms = with_snapshot_tag_
                               ? std::make_optional<int64_t>(
                                   std::numeric_limits<int64_t>::max())
                               : std::nullopt;
        iceberg::transaction txn(std::move(table_));
        auto icb_append_res = co_await txn.merge_append(
          io,
          std::move(icb_files_),
          {{commit_meta_prop, to_json_str(commit_meta)}},
          std::move(tag_name),
          tag_expiry_ms);
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

        co_return std::move(icb_commit_res.value());
    }

    size_t num_files() const noexcept { return icb_files_.size(); }

private:
    table_commit_builder(
      const model::cluster_uuid& cluster,
      iceberg::table_identifier table_id,
      iceberg::table_metadata&& table,
      std::optional<model::offset> table_commit_offset,
      bool with_tag)
      : cluster_(cluster)
      , table_id_(std::move(table_id))
      , table_(std::move(table))
      , table_commit_offset_(table_commit_offset)
      , with_snapshot_tag_(with_tag) {}

private:
    bool should_skip_entry(model::offset added_pending_at) const {
        return table_commit_offset_.has_value()
               && added_pending_at <= *table_commit_offset_;
    }

private:
    model::cluster_uuid cluster_;
    iceberg::table_identifier table_id_;
    iceberg::table_metadata table_;
    std::optional<model::offset> table_commit_offset_;
    bool with_snapshot_tag_;

    // State accumulated.
    chunked_vector<iceberg::file_to_append> icb_files_;
    std::optional<model::offset> new_committed_offset_;
};

ss::future<checked<model::cluster_uuid, file_committer::errc>>
get_cluster_uuid(storage::api& storage) {
    try {
        auto cluster_uuid_success = co_await storage.wait_for_cluster_uuid();
        if (!cluster_uuid_success) {
            vlog(
              datalake_log.info,
              "Exiting commit after waiting for cluster UUID");
            co_return file_committer::errc::shutting_down;
        }
    } catch (...) {
        vlog(
          datalake_log.error,
          "Exception while waiting for cluster UUID: {}",
          std::current_exception());
        co_return file_committer::errc::failed;
    }
    co_return storage.get_cluster_uuid().value();
}

} // namespace

ss::future<
  checked<chunked_vector<mark_files_committed_update>, file_committer::errc>>
iceberg_file_committer::commit_topic_files_to_catalog(
  model::topic topic, const topics_state& state) const {
    vlog(datalake_log.debug, "Beginning commit for topic {}", topic);
    auto cluster_uuid_res = co_await get_cluster_uuid(storage_);
    if (cluster_uuid_res.has_error()) {
        co_return cluster_uuid_res.error();
    }
    const auto& cluster = cluster_uuid_res.value();

    auto tp_it = state.topic_to_state.find(topic);
    if (
      tp_it == state.topic_to_state.end()
      || !tp_it->second.has_pending_entries()) {
        vlog(datalake_log.debug, "Topic {} has no pending entries", topic);
        co_return chunked_vector<mark_files_committed_update>{};
    }
    // Make a copy up here so we don't have to worry about the state changing
    // underneath us. The STM should be robust enough to detect and reject
    // concurrent changes that result in invalid state updates.
    auto tp_state = tp_it->second.copy();
    auto topic_revision = tp_state.revision;

    // Main table (may not exist if all records so far were invalid and the
    // schema couldn't be determined):
    std::optional<table_commit_builder> main_table_commit_builder;
    if (tp_state.has_pending_main_entries()) {
        auto main_table_id = table_id_provider::table_id(topic);
        auto main_table_res = co_await catalog_.load_table(main_table_id);
        if (main_table_res.has_error()) {
            co_return log_and_convert_catalog_errc(
              main_table_res.error(),
              fmt::format(
                "Error loading table {} for committing from topic {}",
                main_table_id,
                topic));
        }
        auto main_table_commit_builder_res = table_commit_builder::create(
          cluster,
          std::move(main_table_id),
          std::move(main_table_res.value()),
          !disable_snapshot_tags_());
        if (main_table_commit_builder_res.has_error()) {
            co_return main_table_commit_builder_res.error();
        }
        main_table_commit_builder = std::move(
          main_table_commit_builder_res.value());
    }

    // DLQ table (optional):
    std::optional<table_commit_builder> dlq_table_commit_builder;
    if (tp_state.has_pending_dlq_entries()) {
        auto dlq_table_id = table_id_provider::dlq_table_id(topic);
        auto dlq_table_res = co_await catalog_.load_table(dlq_table_id);
        if (dlq_table_res.has_error()) {
            co_return log_and_convert_catalog_errc(
              dlq_table_res.error(),
              fmt::format(
                "Error loading table {} for committing from topic {}",
                dlq_table_id,
                topic));
        }
        auto dlq_table_commit_builder_res = table_commit_builder::create(
          cluster,
          std::move(dlq_table_id),
          std::move(dlq_table_res.value()),
          !disable_snapshot_tags_());
        if (dlq_table_commit_builder_res.has_error()) {
            co_return dlq_table_commit_builder_res.error();
        }
        dlq_table_commit_builder = std::move(
          dlq_table_commit_builder_res.value());
    }

    struct offset_and_bytes {
        kafka::offset last_offset{};
        size_t kafka_bytes_processed{0};
    };

    chunked_hash_map<model::partition_id, offset_and_bytes> pending_commits;
    for (const auto& [pid, p_state] : tp_state.pid_to_pending_files) {
        for (const auto& e : p_state.pending_entries) {
            pending_commits[pid].last_offset = e.data.last_offset;
            pending_commits[pid].kafka_bytes_processed
              += e.data.kafka_bytes_processed;

            if (!e.data.files.empty()) {
                vassert(
                  main_table_commit_builder.has_value(),
                  "Should have main table builder");
                auto res = main_table_commit_builder->process_pending_entry(
                  topic, topic_revision, io_, e.added_pending_at, e.data.files);
                if (res.has_error()) {
                    co_return res.error();
                }
            }

            if (!e.data.dlq_files.empty()) {
                vassert(
                  dlq_table_commit_builder.has_value(),
                  "Should have DLQ table builder");
                auto dlq_res = dlq_table_commit_builder->process_pending_entry(
                  topic,
                  topic_revision,
                  io_,
                  e.added_pending_at,
                  e.data.dlq_files);
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
    updates.reserve(pending_commits.size());
    for (const auto& [pid, entry] : pending_commits) {
        auto tp = model::topic_partition(topic, pid);
        auto update_res = mark_files_committed_update::build(
          state,
          tp,
          topic_revision,
          entry.last_offset,
          entry.kafka_bytes_processed);
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

    const size_t dlq_table_files = dlq_table_commit_builder
                                     ? dlq_table_commit_builder->num_files()
                                     : 0;

    const size_t main_table_files = main_table_commit_builder
                                      ? main_table_commit_builder->num_files()
                                      : 0;

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
        auto table_metadata = std::move(main_table_commit_res.value());
        /**
         * We preserve the snapshot id of the last committed snapshot in the
         * coordinator state. This way we can correlate committed partition
         * offsets with particular Iceberg Table snapshots.
         *
         * We are not interested in the DLQ table snapshots here, as they
         * don't correspond to committed offsets in the main topic.
         */
        const auto current_snapshot_id = table_metadata.current_snapshot_id;
        if (current_snapshot_id) {
            for (auto& update : updates) {
                update.snapshot_id = *current_snapshot_id;
            }
        }
    }

    vlog(
      datalake_log.debug,
      "Committed files to Iceberg catalog for topic {} revision {}. Files for "
      "main table: {}, files for DLQ: {}",
      topic,
      topic_revision,
      main_table_files,
      dlq_table_files);

    co_return updates;
}

ss::future<checked<std::nullopt_t, file_committer::errc>>
iceberg_file_committer::drop_table(
  const iceberg::table_identifier& table_id, purge_data should_purge) const {
    auto load_res = co_await catalog_.load_table(table_id);
    if (load_res.has_error()) {
        if (load_res.error() == iceberg::catalog::errc::not_found) {
            co_return std::nullopt;
        }
        log_and_convert_catalog_errc(
          load_res.error(),
          fmt::format(
            "Failed to load {} before dropping, proceeding to attempt drop "
            "anyway",
            table_id));
    }
    auto drop_res = co_await catalog_.drop_table(
      table_id, should_purge == purge_data::yes);
    if (
      drop_res.has_error()
      && drop_res.error() != iceberg::catalog::errc::not_found) {
        co_return log_and_convert_catalog_errc(
          drop_res.error(), fmt::format("Failed to drop {}", table_id));
    }
    co_return std::nullopt;
}
} // namespace datalake::coordinator
