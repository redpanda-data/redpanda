/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/row_delta_action.h"

#include "base/units.h"
#include "base/vlog.h"
#include "iceberg/logger.h"
#include "iceberg/manifest_file_packer.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_utils.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_requirement.h"
#include "random/generators.h"

#include <limits>

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

uri get_manifest_list_path(
  const table_metadata& table,
  snapshot_id snap_id,
  const uuid_t& commit_uuid,
  size_t num) {
    auto metadata_location = get_metadata_location(table);
    return uri{fmt::format(
      "{}/snap-{}-{}-{}.avro", metadata_location, snap_id(), commit_uuid, num)};
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

snapshot_id random_snap_id() {
    return snapshot_id{random_generators::get_int<int64_t>(
      0, std::numeric_limits<int64_t>::max())};
}

snapshot_id generate_unused_snap_id(const table_metadata& m) {
    auto sid = random_snap_id();
    if (!m.snapshots.has_value() || m.snapshots->empty()) {
        return sid;
    }
    const auto& snaps = *m.snapshots;
    while (std::ranges::find(snaps, sid, &snapshot::id) != snaps.end()) {
        sid = random_snap_id();
    }
    return sid;
}

// Converts file_to_append entries into manifest_entries and a max schema id,
// then delegates to maybe_merge_mfiles_and_new_entries.
ss::future<checked<chunked_vector<manifest_file>, action::errc>>
maybe_merge_mfiles_and_new_data(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  size_t min_to_merge,
  chunked_vector<manifest_file> to_merge,
  chunked_vector<file_to_append> new_data_files,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx) {
    chunked_vector<manifest_entry> new_data_entries;
    auto max_schema_id_in_added = schema::id_t::min();
    for (auto& f : new_data_files) {
        max_schema_id_in_added = std::max(max_schema_id_in_added, f.schema_id);
        manifest_entry e{
          .status = manifest_entry_status::added,
          .snapshot_id = ctx.snap_id,
          .sequence_number = std::nullopt,
          .file_sequence_number = std::nullopt,
          .data_file = std::move(f.file),
        };
        new_data_entries.emplace_back(std::move(e));
    }
    co_return co_await maybe_merge_mfiles_and_new_entries(
      io,
      table,
      std::move(gen_manifest_num),
      manifest_content_type::data,
      min_to_merge,
      std::move(to_merge),
      std::move(new_data_entries),
      new_data_files.empty() ? std::nullopt
                             : std::make_optional(max_schema_id_in_added),
      pspec,
      ctx);
}

// Distributes manifest files and new data files by partition spec, bin-packs,
// and merges/creates manifests as needed.
ss::future<checked<chunked_vector<manifest_file>, action::errc>>
pack_mlist_and_new_data(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  size_t min_to_merge,
  size_t target_size_bytes,
  const table_snapshot_ctx& ctx,
  chunked_vector<manifest_file> old_data_mfiles,
  chunked_vector<file_to_append> new_data_files) {
    struct per_spec_data {
        chunked_vector<manifest_file> existing_manifests;
        chunked_vector<file_to_append> new_data_files;
    };

    chunked_hash_map<partition_spec::id_t, per_spec_data> spec2data;
    for (auto& m : old_data_mfiles) {
        auto spec_id = m.partition_spec_id;
        spec2data[spec_id].existing_manifests.push_back(std::move(m));
    }
    for (auto& f : new_data_files) {
        auto spec_id = f.partition_spec_id;
        spec2data[spec_id].new_data_files.push_back(std::move(f));
    }

    chunked_vector<manifest_file> new_mfiles;
    for (auto& [spec_id, data] : spec2data) {
        const auto* pspec = table.get_partition_spec(spec_id);
        if (!pspec) {
            vlog(log.error, "partition spec {} not found in metadata", spec_id);
            co_return action::errc::unexpected_state;
        }

        auto num_old_manifests = data.existing_manifests.size();
        auto binned_mfiles = manifest_packer::pack(
          target_size_bytes, std::move(data.existing_manifests));
        vlog(
          log.info,
          "Packed {} manifests into {} bins for partition spec id {}",
          num_old_manifests,
          binned_mfiles.size(),
          spec_id);
        if (binned_mfiles.empty()) {
            binned_mfiles.emplace_back(chunked_vector<manifest_file>{});
        }
        auto merged_bins_res = co_await maybe_merge_mfiles_and_new_data(
          io,
          table,
          [&gen_manifest_num]() { return gen_manifest_num(); },
          min_to_merge,
          std::move(binned_mfiles[0]),
          std::move(data.new_data_files),
          *pspec,
          ctx);
        if (merged_bins_res.has_error()) {
            co_return merged_bins_res.error();
        }
        auto merged_bins = std::move(merged_bins_res.value());
        std::move(
          merged_bins.begin(),
          merged_bins.end(),
          std::back_inserter(new_mfiles));

        for (size_t i = 1; i < binned_mfiles.size(); i++) {
            auto& bin = binned_mfiles[i];
            if (bin.size() == 1) {
                new_mfiles.emplace_back(std::move(bin[0]));
                continue;
            }
            auto merged_bin_res = co_await merge_mfiles(
              io,
              table,
              [&gen_manifest_num]() { return gen_manifest_num(); },
              manifest_content_type::data,
              std::move(bin),
              {},
              std::nullopt,
              *pspec,
              ctx);
            if (merged_bin_res.has_error()) {
                co_return merged_bin_res.error();
            }
            new_mfiles.emplace_back(std::move(merged_bin_res.value()));
        }
    }
    co_return new_mfiles;
}

} // namespace

row_delta_action::row_delta_action(
  manifest_io& io,
  const table_metadata& table,
  chunked_vector<file_to_append> data_files,
  chunked_vector<file_to_delete> delete_files,
  chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props,
  std::optional<ss::sstring> tag_name,
  std::optional<int64_t> tag_expiration_ms)
  : io_(io)
  , table_(table)
  , commit_uuid_(uuid_t::create())
  , new_data_files_(std::move(data_files))
  , new_delete_files_(std::move(delete_files))
  , snapshot_props_(std::move(snapshot_props))
  , tag_name_(std::move(tag_name))
  , tag_expiration_ms_(tag_expiration_ms) {}

ss::future<action::action_outcome> row_delta_action::build_updates() && {
    vlog(
      log.info,
      "Building row delta update for {} data files and {} delete files",
      new_data_files_.size(),
      new_delete_files_.size());

    // Validate data files.
    size_t added_data_records{0};
    size_t added_data_files_size{0};
    for (const auto& f : new_data_files_) {
        if (f.file.partition.val == nullptr) {
            vlog(
              log.error,
              "Metadata for data file {} is missing partition key",
              f.file.file_path);
            co_return action::errc::unexpected_state;
        }
        const auto* pspec = table_.get_partition_spec(f.partition_spec_id);
        if (!pspec) {
            vlog(
              log.error,
              "partition spec {} for file {} not found in metadata",
              f.partition_spec_id,
              f.file.file_path);
            co_return action::errc::unexpected_state;
        }
        auto f_num_fields = f.file.partition.val->fields.size();
        if (f_num_fields != pspec->fields.size()) {
            vlog(
              log.error,
              "Partition key for data file {} has {} fields, expected {}",
              f.file.file_path,
              f_num_fields,
              pspec->fields.size());
            co_return action::errc::unexpected_state;
        }
        added_data_records += f.file.record_count;
        added_data_files_size += f.file.file_size_bytes;
    }
    auto added_data_files_count = new_data_files_.size();

    // Validate delete files. Equality delete files are unpartitioned per
    // the Iceberg spec, so they skip partition field count validation.
    size_t deleted_records{0};
    for (const auto& f : new_delete_files_) {
        bool is_equality_delete = f.file.content_type
                                  == data_file_content_type::equality_deletes;
        if (!is_equality_delete && f.file.partition.val == nullptr) {
            vlog(
              log.error,
              "Metadata for delete file {} is missing partition key",
              f.file.file_path);
            co_return action::errc::unexpected_state;
        }
        const auto* pspec = table_.get_partition_spec(f.partition_spec_id);
        if (!pspec) {
            vlog(
              log.error,
              "partition spec {} for delete file {} not found in metadata",
              f.partition_spec_id,
              f.file.file_path);
            co_return action::errc::unexpected_state;
        }
        auto f_num_fields = f.file.partition.val->fields.size();
        if (f_num_fields != pspec->fields.size()) {
            vlog(
              log.error,
              "Partition key for delete file {} has {} fields, expected "
              "{}",
              f.file.file_path,
              f_num_fields,
              pspec->fields.size());
            co_return action::errc::unexpected_state;
        }
        deleted_records += f.file.record_count;
    }
    auto added_delete_files_count = new_delete_files_.size();

    // Get the manifest list for the current snapshot, if any.
    manifest_list mlist;
    std::optional<snapshot_id> old_snap_id;
    std::optional<snapshot_summary> old_summary;
    if (table_.snapshots.has_value() && !table_.snapshots->empty()) {
        if (!table_.current_snapshot_id.has_value()) {
            vlog(
              log.error,
              "Table's current snapshot id is not set but there are {} "
              "snapshots",
              table_.snapshots->size());
            co_return action::errc::unexpected_state;
        }
        const auto table_cur_snap_id = *table_.current_snapshot_id;
        const auto& snaps = *table_.snapshots;
        auto snap_it = std::ranges::find(
          snaps, table_cur_snap_id, &snapshot::id);
        if (snap_it == snaps.end()) {
            vlog(
              log.error,
              "Table's current snapshot id {} is missing",
              table_cur_snap_id);
            co_return action::errc::unexpected_state;
        }
        auto mlist_res = co_await io_.download_manifest_list(
          snap_it->manifest_list_path);
        if (mlist_res.has_error()) {
            co_return to_action_errc(mlist_res.error());
        }
        mlist = std::move(mlist_res).value();
        old_snap_id = table_cur_snap_id;
        old_summary = snap_it->summary;
    } else if (
      table_.current_snapshot_id.has_value()
      && table_.current_snapshot_id.value() != invalid_snapshot_id) {
        vlog(
          log.error,
          "Table's current snapshot id is set to {} but there are no "
          "snapshots",
          table_.current_snapshot_id.value());
        co_return action::errc::unexpected_state;
    }
    const auto new_seq_num = sequence_number{table_.last_sequence_number() + 1};
    const auto new_snap_id = generate_unused_snap_id(table_);

    const table_snapshot_ctx ctx{
      .commit_uuid = commit_uuid_,
      .snap_id = new_snap_id,
      .seq_num = new_seq_num,
    };

    // Separate existing manifest files into data and delete manifests.
    chunked_vector<manifest_file> old_data_mfiles;
    chunked_vector<manifest_file> old_delete_mfiles;
    for (auto& mf : mlist.files) {
        if (mf.content == manifest_file_content::data) {
            old_data_mfiles.push_back(std::move(mf));
        } else {
            old_delete_mfiles.push_back(std::move(mf));
        }
    }

    // Handle data files using the same pack+merge logic as merge_append.
    chunked_vector<manifest_file> new_mfiles;
    if (!new_data_files_.empty() || !old_data_mfiles.empty()) {
        auto mfiles_res = co_await pack_mlist_and_new_data(
          io_,
          table_,
          [this]() { return generate_manifest_num(); },
          merge_append_action::default_min_to_merge_new_files,
          merge_append_action::default_target_size_bytes,
          ctx,
          std::move(old_data_mfiles),
          std::move(new_data_files_));
        if (mfiles_res.has_error()) {
            co_return mfiles_res.error();
        }
        auto data_mfiles = std::move(mfiles_res.value());
        std::move(
          data_mfiles.begin(),
          data_mfiles.end(),
          std::back_inserter(new_mfiles));
    }

    // Handle delete files: create a new delete manifest per partition spec.
    if (!new_delete_files_.empty()) {
        struct per_spec_delete_data {
            chunked_vector<manifest_entry> entries;
            schema::id_t max_schema_id{schema::id_t::min()};
        };
        chunked_hash_map<partition_spec::id_t, per_spec_delete_data>
          spec2deletes;
        for (auto& f : new_delete_files_) {
            auto spec_id = f.partition_spec_id;
            auto& per_spec = spec2deletes[spec_id];
            per_spec.max_schema_id = std::max(
              per_spec.max_schema_id, f.schema_id);
            manifest_entry e{
              .status = manifest_entry_status::added,
              .snapshot_id = new_snap_id,
              .sequence_number = std::nullopt,
              .file_sequence_number = std::nullopt,
              .data_file = std::move(f.file),
            };
            per_spec.entries.emplace_back(std::move(e));
        }

        for (auto& [spec_id, per_spec] : spec2deletes) {
            const auto* pspec = table_.get_partition_spec(spec_id);
            if (!pspec) {
                vlog(
                  log.error,
                  "partition spec {} not found in metadata",
                  spec_id);
                co_return action::errc::unexpected_state;
            }

            auto mfile_res = co_await merge_mfiles(
              io_,
              table_,
              [this]() { return generate_manifest_num(); },
              manifest_content_type::deletes,
              {},
              std::move(per_spec.entries),
              per_spec.max_schema_id,
              *pspec,
              ctx);
            if (mfile_res.has_error()) {
                co_return mfile_res.error();
            }
            new_mfiles.emplace_back(std::move(mfile_res.value()));
        }
    }

    // Carry forward existing delete manifests.
    std::move(
      old_delete_mfiles.begin(),
      old_delete_mfiles.end(),
      std::back_inserter(new_mfiles));

    manifest_list new_mlist{std::move(new_mfiles)};

    const auto new_mlist_path = get_manifest_list_path(
      table_, new_snap_id, commit_uuid_, 0);

    vlog(
      log.info,
      "Uploading manifest list {} containing {} manifest files",
      new_mlist_path,
      new_mlist.files.size());
    const auto mlist_up_res = co_await io_.upload_manifest_list(
      new_mlist_path, new_mlist);
    if (mlist_up_res.has_error()) {
        co_return to_action_errc(mlist_up_res.error());
    }

    // Determine snapshot operation.
    snapshot_operation op;
    if (added_data_files_count > 0 && added_delete_files_count > 0) {
        op = snapshot_operation::overwrite;
    } else if (added_delete_files_count > 0) {
        op = snapshot_operation::delete_data;
    } else {
        op = snapshot_operation::append;
    }

    snapshot_summary new_summary = {
      .operation = op,
      .added_data_files = static_cast<int64_t>(added_data_files_count),
      .added_records = static_cast<int64_t>(added_data_records),
      .added_files_size = static_cast<int64_t>(added_data_files_size),
      .added_delete_files = static_cast<int64_t>(added_delete_files_count),
      .deleted_records = static_cast<int64_t>(deleted_records),
      .other = {},
    };
    if (old_summary) {
        if (old_summary->total_data_files.has_value()) {
            new_summary.total_data_files = static_cast<int64_t>(
                                             added_data_files_count)
                                           + *old_summary->total_data_files;
        }
        if (old_summary->total_records.has_value()) {
            new_summary.total_records = static_cast<int64_t>(added_data_records)
                                        + *old_summary->total_records;
        }
        if (old_summary->total_files_size.has_value()) {
            new_summary.total_files_size = static_cast<int64_t>(
                                             added_data_files_size)
                                           + *old_summary->total_files_size;
        }
        if (old_summary->total_delete_files.has_value()) {
            new_summary.total_delete_files = static_cast<int64_t>(
                                               added_delete_files_count)
                                             + *old_summary->total_delete_files;
        }
    } else {
        new_summary.total_data_files = static_cast<int64_t>(
          added_data_files_count);
        new_summary.total_records = static_cast<int64_t>(added_data_records);
        new_summary.total_files_size = static_cast<int64_t>(
          added_data_files_size);
        new_summary.total_delete_files = static_cast<int64_t>(
          added_delete_files_count);
    }

    snapshot s{
      .id = new_snap_id,
      .parent_snapshot_id = old_snap_id,
      .sequence_number = new_seq_num,
      .timestamp_ms = model::timestamp::now(),
      .summary = std::move(new_summary),
      .manifest_list_path = new_mlist_path,
      .schema_id = table_.current_schema_id,
    };
    for (auto& [k, v] : snapshot_props_) {
        s.summary.other.emplace(k, v);
    }
    updates_and_reqs ret;
    ret.updates.emplace_back(table_update::add_snapshot{std::move(s)});
    ret.updates.emplace_back(table_update::set_snapshot_ref{
      .ref_name = "main",
      .ref = snapshot_reference{
        .snapshot_id = new_snap_id,
        .type = snapshot_ref_type::branch,
      },
    });
    if (tag_name_.has_value()) {
        ret.updates.emplace_back(table_update::set_snapshot_ref{
          .ref_name = tag_name_.value(),
          .ref = snapshot_reference{
            .snapshot_id = new_snap_id,
            .type = snapshot_ref_type::tag,
            .max_ref_age_ms = tag_expiration_ms_,
          },
        });
    }
    ret.requirements.emplace_back(
      table_requirement::assert_ref_snapshot_id{
        .ref = "main",
        .snapshot_id = old_snap_id,
      });
    co_return ret;
}

} // namespace iceberg
