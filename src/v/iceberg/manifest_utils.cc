/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/manifest_utils.h"

#include "base/vlog.h"
#include "iceberg/compatibility.h"
#include "iceberg/logger.h"
#include "iceberg/manifest.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/values_bytes.h"

namespace iceberg {

namespace {

uri get_metadata_location(const table_metadata& table) {
    static constexpr std::string_view write_metadata_path_prop
      = "write.metadata.path";

    if (table.properties.has_value()) {
        auto it = table.properties->find(write_metadata_path_prop);
        if (it != table.properties->end()) {
            return uri(it->second);
        }
    }

    return uri(fmt::format("{}/metadata", table.location));
}

uri get_manifest_path(
  const table_metadata& table, const uuid_t& commit_uuid, size_t num) {
    auto metadata_location = get_metadata_location(table);
    return uri(
      fmt::format("{}/{}-m{}.avro", metadata_location, commit_uuid, num));
}

action::errc to_action_errc(metadata_io::errc e) {
    switch (e) {
    case metadata_io::errc::failed:
        return action::errc::io_failed;
    case metadata_io::errc::shutting_down:
        return action::errc::shutting_down;
    case metadata_io::errc::invalid_uri:
        return action::errc::unexpected_state;
    case metadata_io::errc::timedout:
        return action::errc::io_failed;
    }
}

manifest_file_content to_file_content(manifest_content_type t) {
    switch (t) {
    case manifest_content_type::data:
        return manifest_file_content::data;
    case manifest_content_type::deletes:
        return manifest_file_content::deletes;
    }
}

void update_partition_summaries(
  const data_file& f, chunked_vector<field_summary_val>& summaries) {
    const auto& pk_val_fields = f.partition.val->fields;
    for (size_t i = 0; i < summaries.size(); ++i) {
        const auto& file_val_field = pk_val_fields[i];
        if (!file_val_field.has_value()) {
            summaries[i].contains_null = true;
            continue;
        }
        const auto& file_prim_val = std::get<primitive_value>(
          file_val_field.value());
        if (!summaries[i].lower_bound.has_value()) {
            summaries[i].lower_bound = make_copy(file_prim_val);
        } else {
            auto& lb = summaries[i].lower_bound.value();
            if (file_prim_val < lb) {
                lb = make_copy(file_prim_val);
            }
        }
        if (!summaries[i].upper_bound.has_value()) {
            summaries[i].upper_bound = make_copy(file_prim_val);
        } else {
            auto& ub = summaries[i].upper_bound.value();
            if (ub < file_prim_val) {
                ub = make_copy(file_prim_val);
            }
        }
    }
}

chunked_vector<field_summary> release_with_bytes(field_summary_val::list_t l) {
    chunked_vector<field_summary> ret;
    ret.reserve(l.size());
    for (auto& v : l) {
        ret.emplace_back(std::move(v).release_with_bytes());
    }
    return ret;
}

void promote_partition_key_type(
  partition_key& pk, const partition_key_type& pk_type) {
    vassert(
      pk.val->fields.size() == pk_type.type.fields.size(),
      "unexpected partition key size: {} (expected: {})",
      pk.val->fields.size(),
      pk_type.type.fields.size());
    for (size_t i = 0; i < pk.val->fields.size(); ++i) {
        auto& field = pk.val->fields[i];
        if (field) {
            const auto& type = std::get<primitive_type>(
              pk_type.type.fields[i]->type);
            field = promote_primitive_value_type(
              std::move(std::get<primitive_value>(*field)), type);
        }
    }
}

} // namespace

field_summary_val::list_t
field_summary_val::empty_summaries(size_t num_fields) {
    field_summary_val::list_t ret;
    ret.reserve(num_fields);
    for (size_t i = 0; i < num_fields; ++i) {
        ret.emplace_back(field_summary_val{});
    }
    return ret;
}

field_summary field_summary_val::release_with_bytes() && {
    std::optional<bytes> lb;
    std::optional<bytes> ub;
    if (lower_bound.has_value()) {
        lb = value_to_bytes(value{std::move(lower_bound).value()});
    }
    if (upper_bound.has_value()) {
        ub = value_to_bytes(value{std::move(upper_bound).value()});
    }
    return field_summary{
      .contains_null = contains_null,
      .contains_nan = contains_nan,
      .lower_bound = std::move(lb),
      .upper_bound = std::move(ub),
    };
}

ss::future<checked<size_t, metadata_io::errc>> upload_as_manifest(
  manifest_io& io,
  const uri& path,
  const schema& schema,
  const partition_spec& pspec,
  manifest_content_type content_type,
  chunked_vector<manifest_entry> entries) {
    vlog(
      log.info,
      "Uploading manifest with {} entries to {}",
      entries.size(),
      path);
    manifest m{
      .metadata = manifest_metadata{
        .schema = schema.copy(),
        .partition_spec = pspec.copy(),
        .format_version = format_version::v2,
        .manifest_content_type = content_type,
      },
      .entries = std::move(entries),
    };
    co_return co_await io.upload_manifest(path, m);
}

ss::future<checked<manifest_file, action::errc>> merge_mfiles(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  manifest_content_type content_type,
  chunked_vector<manifest_file> to_merge,
  chunked_vector<manifest_entry> added_entries,
  std::optional<schema::id_t> max_schema_id_in_added,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx) {
    vlogl(
      log,
      to_merge.empty() ? ss::log_level::debug : ss::log_level::info,
      "Merging {} manifest files and {} added manifest entries",
      to_merge.size(),
      added_entries.size());

    const size_t added_files = added_entries.size();
    size_t added_rows = 0;
    for (const auto& e : added_entries) {
        added_rows += e.data_file.record_count;
    }

    auto merged_entries = std::move(added_entries);
    auto max_schema_id = max_schema_id_in_added.value_or(schema::id_t::min());
    size_t existing_rows = 0;
    size_t existing_files = 0;
    auto min_seq_num = ctx.seq_num;
    for (const auto& mfile : to_merge) {
        auto mfile_res = co_await io.download_manifest(mfile.manifest_path);
        if (mfile_res.has_error()) {
            co_return to_action_errc(mfile_res.error());
        }
        auto m = std::move(mfile_res).value();
        max_schema_id = std::max(max_schema_id, m.metadata.schema.schema_id);
        existing_files += m.entries.size();
        for (auto& e : m.entries) {
            auto f_num_fields = e.data_file.partition.val->fields.size();
            if (f_num_fields != pspec.fields.size()) {
                vlog(
                  log.error,
                  "Partition key for data file {} has {} fields, expected {}",
                  e.data_file.file_path,
                  f_num_fields,
                  pspec.fields.size());
                co_return action::errc::unexpected_state;
            }

            existing_rows += e.data_file.record_count;
            if (e.status == manifest_entry_status::added) {
                e.status = manifest_entry_status::existing;
                e.sequence_number = e.sequence_number.value_or(
                  mfile.seq_number);
                e.file_sequence_number = e.file_sequence_number.value_or(
                  file_sequence_number{mfile.seq_number()});
            }
            if (e.sequence_number.has_value()) {
                min_seq_num = std::min(min_seq_num, e.sequence_number.value());
            }
        }
        std::move(
          m.entries.begin(),
          m.entries.end(),
          std::back_inserter(merged_entries));
    }

    auto schema_id = pspec.spec_id == table.default_spec_id
                       ? table.current_schema_id
                       : max_schema_id;
    const auto* resolved_schema = table.get_schema(schema_id);
    if (!resolved_schema) {
        vlog(log.error, "Table schema {} is missing from metadata", schema_id);
        co_return action::errc::unexpected_state;
    }

    auto pk_type = partition_key_type::create(pspec, *resolved_schema);
    auto partition_summaries = field_summary_val::empty_summaries(
      pspec.fields.size());
    for (auto& e : merged_entries) {
        try {
            promote_partition_key_type(e.data_file.partition, pk_type);
            update_partition_summaries(e.data_file, partition_summaries);
        } catch (const std::exception& ex) {
            vlog(
              log.error,
              "bad partition key for file {}: {}",
              e.data_file.file_path,
              ex);
            co_return action::errc::unexpected_state;
        }
    }

    const auto merged_manifest_path = get_manifest_path(
      table, ctx.commit_uuid, gen_manifest_num());
    const auto mfile_up_res = co_await upload_as_manifest(
      io,
      merged_manifest_path,
      *resolved_schema,
      pspec,
      content_type,
      std::move(merged_entries));
    if (mfile_up_res.has_error()) {
        co_return to_action_errc(mfile_up_res.error());
    }
    manifest_file merged_file{
      .manifest_path = merged_manifest_path,
      .manifest_length = mfile_up_res.value(),
      .partition_spec_id = pspec.spec_id,
      .content = to_file_content(content_type),
      .seq_number = ctx.seq_num,
      .min_seq_number = min_seq_num,
      .added_snapshot_id = ctx.snap_id,
      .added_files_count = added_files,
      .existing_files_count = existing_files,
      .deleted_files_count = 0,
      .added_rows_count = added_rows,
      .existing_rows_count = existing_rows,
      .deleted_rows_count = 0,
      .partitions = release_with_bytes(std::move(partition_summaries)),
    };
    co_return merged_file;
}

ss::future<checked<chunked_vector<manifest_file>, action::errc>>
maybe_merge_mfiles_and_new_entries(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  manifest_content_type content_type,
  size_t min_to_merge,
  chunked_vector<manifest_file> to_merge,
  chunked_vector<manifest_entry> new_entries,
  std::optional<schema::id_t> max_added_schema_id,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx) {
    vlog(
      log.info,
      "Considering {} existing manifest files and {} entries to merge",
      to_merge.size(),
      new_entries.size());
    chunked_vector<manifest_file> ret;
    if (to_merge.size() < min_to_merge) {
        if (!new_entries.empty()) {
            auto new_mfile_res = co_await merge_mfiles(
              io,
              table,
              std::move(gen_manifest_num),
              content_type,
              {},
              std::move(new_entries),
              max_added_schema_id,
              pspec,
              ctx);
            if (new_mfile_res.has_error()) {
                co_return new_mfile_res.error();
            }
            ret.emplace_back(std::move(new_mfile_res.value()));
        }
        std::move(to_merge.begin(), to_merge.end(), std::back_inserter(ret));
        co_return ret;
    }
    auto merged_mfile_res = co_await merge_mfiles(
      io,
      table,
      std::move(gen_manifest_num),
      content_type,
      std::move(to_merge),
      std::move(new_entries),
      max_added_schema_id,
      pspec,
      ctx);
    if (merged_mfile_res.has_error()) {
        co_return merged_mfile_res.error();
    }
    ret.emplace_back(std::move(merged_mfile_res.value()));
    co_return ret;
}

} // namespace iceberg
