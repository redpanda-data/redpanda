// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/merge_append_action.h"

#include "base/units.h"
#include "base/vlog.h"
#include "iceberg/logger.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_file_packer.h"
#include "iceberg/manifest_list.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_requirement.h"
#include "iceberg/values_bytes.h"
#include "random/generators.h"

#include <iterator>
#include <limits>

namespace iceberg {

namespace {
manifest_path get_manifest_path(
  const ss::sstring& location, const uuid_t& commit_uuid, size_t num) {
    return manifest_path{
      fmt::format("{}/metadata/{}-m{}.avro", location, commit_uuid, num)};
}
manifest_list_path get_manifest_list_path(
  const ss::sstring& location,
  snapshot_id snap_id,
  const uuid_t& commit_uuid,
  size_t num) {
    return manifest_path{fmt::format(
      "{}/metadata/snap-{}-{}-{}.avro", location, snap_id(), commit_uuid, num)};
}

action::errc to_action_errc(metadata_io::errc e) {
    switch (e) {
    case metadata_io::errc::failed:
        return action::errc::io_failed;
    case metadata_io::errc::shutting_down:
        return action::errc::shutting_down;
    case metadata_io::errc::timedout:
        // NOTE: treat IO timeouts the same as other IO failures.
        // TODO: build out retry logic.
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
    // Repeatedly try to generate a new snapshot id that isn't used already.
    const auto& snaps = *m.snapshots;
    while (std::ranges::find(snaps, sid, &snapshot::id) != snaps.end()) {
        sid = random_snap_id();
    }
    return sid;
}

void update_partition_summaries(
  const data_file& f, chunked_vector<field_summary_val>& summaries) {
    // NOTE: callers should have validated that partition keys of the data
    // files have the same number of fields as the partition key used to
    // construct the summaries.
    const auto& pk_val_fields = f.partition.val->fields;
    for (size_t i = 0; i < summaries.size(); ++i) {
        const auto& file_val_field = pk_val_fields[i];
        if (!file_val_field.has_value()) {
            summaries[i].contains_null = true;
            continue;
        }
        // TODO: contains_nan
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

} // namespace

field_summary_val::list_t
field_summary_val::empty_summaries(const partition_key_type& pk_type) {
    field_summary_val::list_t ret;
    const auto num_fields = pk_type.type.fields.size();
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

ss::future<action::action_outcome> merge_append_action::build_updates() && {
    vlog(
      log.info,
      "Building append update for {} data files",
      new_data_files_.size());
    // Look for the current schema.
    const auto schema_id = table_.current_schema_id;
    auto schema_it = std::ranges::find(
      table_.schemas, schema_id, &schema::schema_id);
    if (schema_it == table_.schemas.end()) {
        vlog(log.error, "Table schema {} is missing from metadata", schema_id);
        co_return action::errc::unexpected_state;
    }
    const auto& schema = *schema_it;

    // Look for the current partition spec.
    const auto& pspecs = table_.partition_specs;
    auto pspec_it = std::ranges::find(
      pspecs, table_.default_spec_id, &partition_spec::spec_id);
    if (pspec_it == pspecs.end()) {
        vlog(
          log.error,
          "Partition spec {} is missing from metadata",
          table_.default_spec_id);
        co_return action::errc::unexpected_state;
    }
    if (pspecs.size() != 1) {
        // TODO: when we support multiple partition specs, we'll need to group
        // them by spec id and write manifest files per spec.
        vlog(
          log.error,
          "Currently exactly one partition spec is supported: {} found",
          pspecs.size());
        co_return action::errc::unexpected_state;
    }

    // Validate our input files that their partition keys look sane.
    const auto& pspec = *pspec_it;
    for (const auto& f : new_data_files_) {
        if (f.partition.val == nullptr) {
            vlog(
              log.error,
              "Metadata for data file {} is missing partition key",
              f.file_path);
            co_return action::errc::unexpected_state;
        }
        auto f_num_fields = f.partition.val->fields.size();
        if (f_num_fields != pspec.fields.size()) {
            vlog(
              log.error,
              "Partition key for data file {} has {} fields, expected {}",
              f.file_path,
              f_num_fields,
              pspec.fields.size());
            co_return action::errc::unexpected_state;
        }
    }

    // Get the manifest list for the current snapshot, if any.
    manifest_list mlist;
    std::optional<snapshot_id> old_snap_id;
    if (table_.snapshots.has_value() && !table_.snapshots->empty()) {
        if (!table_.current_snapshot_id.has_value()) {
            // We have snapshots, but it's unclear which one to base our update
            // off of.
            vlog(
              log.error,
              "Table's current snapshot id is not set but there are {} "
              "snapshots",
              table_.snapshots->size());
            co_return action::errc::unexpected_state;
        }
        // Look for the current snapshot.
        const auto table_cur_snap_id = *table_.current_snapshot_id;
        const auto& snaps = *table_.snapshots;
        auto snap_it = std::ranges::find(
          snaps, table_cur_snap_id, &snapshot::id);
        if (snap_it == snaps.end()) {
            // We have snapshots, but the one we thought we needed to base our
            // update off of is missing.
            vlog(
              log.error,
              "Table's current snapshot id {} is missing",
              table_cur_snap_id);
            co_return action::errc::unexpected_state;
        }
        auto mlist_res = co_await io_.download_manifest_list_uri(
          snap_it->manifest_list_path);
        if (mlist_res.has_error()) {
            co_return to_action_errc(mlist_res.error());
        }
        mlist = std::move(mlist_res).value();
        old_snap_id = table_cur_snap_id;
    } else if (table_.current_snapshot_id.has_value()) {
        vlog(
          log.error,
          "Table's current snapshot id is set to {} but there are no "
          "snapshots",
          table_.current_snapshot_id.value());
        co_return action::errc::unexpected_state;
    }
    const auto new_seq_num = sequence_number{table_.last_sequence_number() + 1};
    const auto new_snap_id = generate_unused_snap_id(table_);

    // TODO: support more than one partition spec by grouping the merged
    // manifests by spec id.
    auto pk_type = partition_key_type::create(pspec, schema);
    const table_snapshot_ctx ctx{
      .commit_uuid = commit_uuid_,
      .schema = schema,
      .pspec = pspec,
      .pk_type = pk_type,
      .snap_id = new_snap_id,
      .seq_num = new_seq_num,
    };

    auto mfiles_res = co_await pack_mlist_and_new_data(
      ctx, std::move(mlist), std::move(new_data_files_));
    if (mfiles_res.has_error()) {
        co_return to_action_errc(mfiles_res.error());
    }
    manifest_list new_mlist{std::move(mfiles_res.value())};

    // NOTE: 0 here is the attempt number for this manifest list. Other Iceberg
    // implementations retry appends on failure and increment an count for
    // naming uniqueness. Retries for us are expected to take the form of an
    // entirely new transaction.
    const auto new_mlist_path = get_manifest_list_path(
      table_.location, new_snap_id, commit_uuid_, 0);

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

    // Return the snapshot metadata.
    snapshot s{
      .id = new_snap_id,
      .parent_snapshot_id = old_snap_id,
      .sequence_number = new_seq_num,
      .timestamp_ms = model::timestamp::now(),
      .summary = {
          .operation = snapshot_operation::append,
          .other = {},
      },
      .manifest_list_path = io_.to_uri(new_mlist_path()),
      .schema_id = schema.schema_id,
    };
    updates_and_reqs ret;
    ret.updates.emplace_back(table_update::add_snapshot{std::move(s)});
    ret.updates.emplace_back(table_update::set_snapshot_ref{
          .ref_name = "main",
          .ref = snapshot_reference{
            .snapshot_id = new_snap_id,
            .type = snapshot_ref_type::branch,
          },
        });
    ret.requirements.emplace_back(table_requirement::assert_ref_snapshot_id{
      .ref = "main",
      .snapshot_id = old_snap_id,
    });
    co_return ret;
}

ss::future<checked<size_t, metadata_io::errc>>
merge_append_action::upload_as_manifest(
  const manifest_path& path,
  const schema& schema,
  const partition_spec& pspec,
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
            .manifest_content_type = manifest_content_type::data,
        },
        .entries = std::move(entries),
    };
    co_return co_await io_.upload_manifest(path, m);
}

ss::future<checked<chunked_vector<manifest_file>, metadata_io::errc>>
merge_append_action::maybe_merge_mfiles_and_new_data(
  chunked_vector<manifest_file> to_merge,
  chunked_vector<data_file> new_data_files,
  const table_snapshot_ctx& ctx) {
    size_t added_rows{0};
    size_t added_files{0};
    vlog(
      log.info,
      "Considering {} existing manifest files and {} data files to merge",
      to_merge.size(),
      new_data_files.size());
    // First construct some manifest entries for the new data files. Regardless
    // of if we upload a brand new manifest or merge with an existing manifest,
    // the new data files will need new entries.
    chunked_vector<manifest_entry> new_data_entries;
    auto partition_summaries = field_summary_val::empty_summaries(ctx.pk_type);
    for (auto& f : new_data_files) {
        update_partition_summaries(f, partition_summaries);
        added_rows += f.record_count;
        manifest_entry e{
          .status = manifest_entry_status::added,
          .snapshot_id = ctx.snap_id,
          .sequence_number = std::nullopt,
          .file_sequence_number = std::nullopt,
          .data_file = std::move(f),
        };
        new_data_entries.emplace_back(std::move(e));
    }
    chunked_vector<manifest_file> ret;
    if (to_merge.size() < default_min_to_merge_new_files) {
        // Upload and return. This bin is too small to merge.
        const auto new_manifest_path = get_manifest_path(
          table_.location, ctx.commit_uuid, generate_manifest_num());
        added_files = new_data_entries.size();
        const auto mfile_up_res = co_await upload_as_manifest(
          new_manifest_path,
          ctx.schema,
          ctx.pspec,
          std::move(new_data_entries));
        if (mfile_up_res.has_error()) {
            co_return mfile_up_res.error();
        }
        // Since this bin was too small to merge, we won't do anything else to
        // its manifests, just add them back to the returned container.
        ret.emplace_back(manifest_file{
          .manifest_path = io_.to_uri(new_manifest_path()),
          .manifest_length = mfile_up_res.value(),
          .partition_spec_id = ctx.pspec.spec_id,
          .content = manifest_file_content::data,
          .seq_number = ctx.seq_num,
          .min_seq_number = ctx.seq_num,
          .added_snapshot_id = ctx.snap_id,
          .added_files_count = added_files,
          .existing_files_count = 0,
          .deleted_files_count = 0,
          .added_rows_count = added_rows,
          .existing_rows_count = 0,
          .deleted_rows_count = 0,
          .partitions = release_with_bytes(std::move(partition_summaries)),
        });
        std::move(to_merge.begin(), to_merge.end(), std::back_inserter(ret));
        co_return ret;
    }
    auto merged_mfile_res = co_await merge_mfiles(
      std::move(to_merge),
      ctx,
      std::move(partition_summaries),
      std::move(new_data_entries),
      added_rows);
    if (merged_mfile_res.has_error()) {
        co_return merged_mfile_res.error();
    }
    ret.emplace_back(std::move(merged_mfile_res.value()));
    co_return ret;
}

ss::future<checked<manifest_file, metadata_io::errc>>
merge_append_action::merge_mfiles(
  chunked_vector<manifest_file> to_merge,
  const table_snapshot_ctx& ctx,
  field_summary_val::list_t added_summaries,
  chunked_vector<manifest_entry> added_entries,
  size_t added_rows) {
    vlog(
      log.info,
      "Merging {} manifest files and {} added manifest entries",
      to_merge.size(),
      added_entries.size());
    auto added_files = added_entries.size();
    auto merged_entries = std::move(added_entries);
    size_t existing_rows = 0;
    size_t existing_files = 0;
    auto min_seq_num = ctx.seq_num;
    auto partition_summaries = added_summaries.empty()
                                 ? field_summary_val::empty_summaries(
                                     ctx.pk_type)
                                 : std::move(added_summaries);
    for (const auto& mfile : to_merge) {
        // Download the manifest file and collect the entries into the merged
        // container.
        auto mfile_res = co_await io_.download_manifest_uri(
          mfile.manifest_path, ctx.pk_type);
        if (mfile_res.has_error()) {
            co_return mfile_res.error();
        }
        auto m = std::move(mfile_res).value();
        existing_files += m.entries.size();
        for (auto& e : m.entries) {
            update_partition_summaries(e.data_file, partition_summaries);
            existing_rows += e.data_file.record_count;
            // Rewrite sequence numbers for previously added entries.
            // These entries refer to files committed prior to this action.
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
    const auto merged_manifest_path = get_manifest_path(
      table_.location, ctx.commit_uuid, generate_manifest_num());
    const auto mfile_up_res = co_await upload_as_manifest(
      merged_manifest_path, ctx.schema, ctx.pspec, std::move(merged_entries));
    if (mfile_up_res.has_error()) {
        co_return mfile_up_res.error();
    }
    manifest_file merged_file{
      .manifest_path = io_.to_uri(merged_manifest_path()),
      .manifest_length = mfile_up_res.value(),
      .partition_spec_id = ctx.pspec.spec_id,
      .content = manifest_file_content::data,
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

ss::future<checked<chunked_vector<manifest_file>, metadata_io::errc>>
merge_append_action::pack_mlist_and_new_data(
  const table_snapshot_ctx& ctx,
  manifest_list old_mlist,
  chunked_vector<data_file> new_data_files) {
    auto num_old_files = old_mlist.files.size();
    auto binned_mfiles = manifest_packer::pack(
      default_target_size_bytes, std::move(old_mlist.files));
    vlog(
      log.info,
      "Packed {} manifests into {} bins",
      num_old_files,
      binned_mfiles.size());
    if (binned_mfiles.empty()) {
        // If we had no manifests at all, at least create an empty bin to add
        // new manifests to below.
        binned_mfiles.emplace_back(chunked_vector<manifest_file>{});
    }
    chunked_vector<manifest_file> new_mfiles;
    // Always add files to the first bin, which by convention will be the
    // latest data. We may not merge existing manifests if the bin is
    // small, but we'll at least add metadata for the new data files.
    auto merged_bins_res = co_await maybe_merge_mfiles_and_new_data(
      std::move(binned_mfiles[0]), std::move(new_data_files), ctx);
    if (merged_bins_res.has_error()) {
        co_return merged_bins_res.error();
    }
    auto merged_bins = std::move(merged_bins_res.value());
    std::move(
      merged_bins.begin(), merged_bins.end(), std::back_inserter(new_mfiles));

    // Merge the rest of the bins.
    for (size_t i = 1; i < binned_mfiles.size(); i++) {
        auto& bin = binned_mfiles[i];
        if (bin.size() == 1) {
            // The bin has only a single manifest so there's nothing to do,
            // just add it as is.
            new_mfiles.emplace_back(std::move(bin[0]));
            continue;
        }
        auto merged_bin_res = co_await merge_mfiles(std::move(bin), ctx);
        if (merged_bin_res.has_error()) {
            co_return merged_bin_res.error();
        }
        new_mfiles.emplace_back(std::move(merged_bin_res.value()));
    }
    co_return new_mfiles;
}

} // namespace iceberg
