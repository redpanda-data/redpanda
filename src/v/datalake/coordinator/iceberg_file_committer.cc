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
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
#include "datalake/table_definition.h"
#include "iceberg/catalog.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/partition_key.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"
#include "iceberg/values.h"

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
} // namespace

ss::future<
  checked<chunked_vector<mark_files_committed_update>, file_committer::errc>>
iceberg_file_committer::commit_topic_files_to_catalog(
  model::topic topic, const topics_state& state) const {
    auto tp_it = state.topic_to_state.find(topic);
    if (tp_it == state.topic_to_state.end()) {
        co_return chunked_vector<mark_files_committed_update>{};
    }
    auto table_id = table_id_for_topic(topic);
    chunked_hash_map<model::partition_id, kafka::offset> pending_commits;
    chunked_vector<iceberg::data_file> icb_files;
    const auto& tp_state = tp_it->second;
    for (const auto& [pid, p_state] : tp_state.pid_to_pending_files) {
        for (const auto& e : p_state.pending_entries) {
            pending_commits[pid] = e.last_offset;
            for (const auto& f : e.files) {
                auto pk = std::make_unique<iceberg::struct_value>();
                pk->fields.emplace_back(iceberg::int_value{f.hour});
                icb_files.emplace_back(iceberg::data_file{
                  .content_type = iceberg::data_file_content_type::data,
                  .file_path = f.remote_path,
                  .file_format = iceberg::data_file_format::parquet,
                  .partition = iceberg::partition_key{std::move(pk)},
                  .record_count = f.row_count,
                  .file_size_bytes = f.file_size_bytes,
                });
            }
        }
    }
    chunked_vector<mark_files_committed_update> updates;
    if (pending_commits.empty()) {
        co_return updates;
    }
    for (const auto& [pid, committed_offset] : pending_commits) {
        auto tp = model::topic_partition(topic, pid);
        auto update_res = mark_files_committed_update::build(
          state, tp, committed_offset);
        if (update_res.has_error()) {
            vlog(
              datalake_log.warn,
              "Could not build STM update for committing to {}: {}",
              table_id,
              update_res.error());
            co_return errc::failed;
        }
        updates.emplace_back(std::move(update_res.value()));
    }
    auto table_res = co_await load_or_create_table(table_id);
    if (table_res.has_error()) {
        co_return table_res.error();
    }
    // TODO: deduplicate the files against those already in the table!
    // Or do this in the merge append.

    // TODO: update the table schema if it differs from the input files!

    iceberg::transaction txn(io_, std::move(table_res.value()));
    auto icb_append_res = co_await txn.merge_append(std::move(icb_files));
    if (icb_append_res.has_error()) {
        co_return log_and_convert_action_errc(
          icb_append_res.error(),
          fmt::format("Iceberg merge append failed for table {}", table_id));
    }
    auto icb_commit_res = co_await catalog_.commit_txn(
      table_id, std::move(txn));
    if (icb_commit_res.has_error()) {
        co_return log_and_convert_catalog_errc(
          icb_commit_res.error(),
          fmt::format(
            "Iceberg transaction did not commit to table {}", table_id));
    }
    co_return updates;
}
iceberg::table_identifier
iceberg_file_committer::table_id_for_topic(const model::topic& t) const {
    return iceberg::table_identifier{
      // TODO: namespace as a topic property? Keep it in the table metadata?
      .ns = {"redpanda"},
      .table = t,
    };
}

ss::future<checked<iceberg::table_metadata, file_committer::errc>>
iceberg_file_committer::load_or_create_table(
  const iceberg::table_identifier& table_id) const {
    auto load_res = co_await catalog_.load_table(table_id);
    if (load_res.has_value()) {
        co_return std::move(load_res.value());
    }
    if (load_res.error() != iceberg::catalog::errc::not_found) {
        co_return log_and_convert_catalog_errc(
          load_res.error(),
          fmt::format("Iceberg table {} failed to load", table_id));
    }
    vlog(
      datalake_log.info,
      "Iceberg table {} not found in catalog, creating new table",
      table_id);
    auto create_res = co_await catalog_.create_table(
      table_id, default_schema(), datalake::hour_partition_spec());
    if (create_res.has_value()) {
        co_return std::move(create_res.value());
    }
    if (create_res.error() != iceberg::catalog::errc::already_exists) {
        co_return log_and_convert_catalog_errc(
          create_res.error(),
          fmt::format("Iceberg table {} failed to create", table_id));
    }
    load_res = co_await catalog_.load_table(table_id);
    if (load_res.has_value()) {
        co_return std::move(load_res.value());
    }
    // If it fails to load even after creating, regardless of what it is, just
    // fail the request.
    co_return log_and_convert_catalog_errc(
      load_res.error(),
      fmt::format("Iceberg table {} failed to load after creating", table_id));
}

} // namespace datalake::coordinator
