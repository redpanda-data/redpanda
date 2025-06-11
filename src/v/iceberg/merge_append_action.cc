/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/merge_append_action.h"

#include "base/units.h"
#include "base/vlog.h"
#include "iceberg/compatibility.h"
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

// Derive the metadata location from table properties.
// Some catalogs require respecting the property `write.metadata.path`,
// but the default location is <table location>/metadata.
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

ss::future<action::action_outcome> merge_append_action::build_updates() && {
    vlog(
      log.info,
      "Building append update for {} data files",
      new_data_files_.size());

    // Validate our input files that their partition keys look sane.
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
        auto mlist_res = co_await io_.download_manifest_list(
          snap_it->manifest_list_path);
        if (mlist_res.has_error()) {
            co_return to_action_errc(mlist_res.error());
        }
        mlist = std::move(mlist_res).value();
        old_snap_id = table_cur_snap_id;
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

    auto mfiles_res = co_await pack_mlist_and_new_data(
      ctx, std::move(mlist), std::move(new_data_files_));
    if (mfiles_res.has_error()) {
        co_return mfiles_res.error();
    }
    manifest_list new_mlist{std::move(mfiles_res.value())};

    // NOTE: 0 here is the attempt number for this manifest list. Other Iceberg
    // implementations retry appends on failure and increment an count for
    // naming uniqueness. Retries for us are expected to take the form of an
    // entirely new transaction.
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
    ret.requirements.emplace_back(table_requirement::assert_ref_snapshot_id{
      .ref = "main",
      .snapshot_id = old_snap_id,
    });
    co_return ret;
}

ss::future<checked<size_t, metadata_io::errc>>
merge_append_action::upload_as_manifest(
  const uri& path,
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

namespace {

// Depending on how schema evolves, type of partition key fields can change
// (e.g. a field type can be promoted from int to long). This means that
// partition values of all data files in the new manifest need to be
// promoted to the common type before creating the manifest.
//
// Preconditions: pk and pk_type have the same size, and the promotion for each
// field is possible.
void promote_partition_key_type(
  partition_key& pk, const partition_key_type& pk_type) {
    // ensured by the callers
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

ss::future<checked<chunked_vector<manifest_file>, action::errc>>
merge_append_action::maybe_merge_mfiles_and_new_data(
  chunked_vector<manifest_file> to_merge,
  chunked_vector<file_to_append> new_data_files,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx) {
    vlog(
      log.info,
      "Considering {} existing manifest files and {} data files to merge",
      to_merge.size(),
      new_data_files.size());
    // First construct some manifest entries for the new data files. Regardless
    // of if we upload a brand new manifest or merge with an existing manifest,
    // the new data files will need new entries.
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
    chunked_vector<manifest_file> ret;
    if (to_merge.size() < default_min_to_merge_new_files) {
        // Upload and return. This bin is too small to merge.
        if (!new_data_files.empty()) {
            auto new_mfile_res = co_await merge_mfiles(
              {},
              std::move(new_data_entries),
              max_schema_id_in_added,
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
      std::move(to_merge),
      std::move(new_data_entries),
      max_schema_id_in_added,
      pspec,
      ctx);
    if (merged_mfile_res.has_error()) {
        co_return merged_mfile_res.error();
    }
    ret.emplace_back(std::move(merged_mfile_res.value()));
    co_return ret;
}

ss::future<checked<manifest_file, action::errc>>
merge_append_action::merge_mfiles(
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
        // Download the manifest file and collect the entries into the merged
        // container.
        auto mfile_res = co_await io_.download_manifest(mfile.manifest_path);
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

    // If the partition spec is not the default one, there is a risk that the
    // partition key type can't be resolved with the current schema (e.g. if the
    // spec uses a source column that was subsequently deleted). To prevent
    // that, we choose the schema with the highest id among all manifests and
    // new files with this spec. Presumably, any of these schemas can be used to
    // resolve the key type, but we choose the one with the highest id because
    // it should contain the "most promoted" types, and therefore partition keys
    // for all files can be promoted to the key type resolved with this schema.
    auto schema_id = pspec.spec_id == table_.default_spec_id
                       ? table_.current_schema_id
                       : max_schema_id;
    const auto* schema = table_.get_schema(schema_id);
    if (!schema) {
        vlog(log.error, "Table schema {} is missing from metadata", schema_id);
        co_return errc::unexpected_state;
    }

    auto pk_type = partition_key_type::create(pspec, *schema);
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
            co_return errc::unexpected_state;
        }
    }

    const auto merged_manifest_path = get_manifest_path(
      table_, ctx.commit_uuid, generate_manifest_num());
    const auto mfile_up_res = co_await upload_as_manifest(
      merged_manifest_path, *schema, pspec, std::move(merged_entries));
    if (mfile_up_res.has_error()) {
        co_return to_action_errc(mfile_up_res.error());
    }
    manifest_file merged_file{
      .manifest_path = merged_manifest_path,
      .manifest_length = mfile_up_res.value(),
      .partition_spec_id = pspec.spec_id,
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

ss::future<checked<chunked_vector<manifest_file>, action::errc>>
merge_append_action::pack_mlist_and_new_data(
  const table_snapshot_ctx& ctx,
  manifest_list old_mlist,
  chunked_vector<file_to_append> new_data_files) {
    struct per_spec_data {
        chunked_vector<manifest_file> existing_manifests;
        chunked_vector<file_to_append> new_data_files;
    };

    chunked_hash_map<partition_spec::id_t, per_spec_data> spec2data;
    for (auto& m : old_mlist.files) {
        auto spec_id = m.partition_spec_id;
        spec2data[spec_id].existing_manifests.push_back(std::move(m));
    }
    for (auto& f : new_data_files) {
        auto spec_id = f.partition_spec_id;
        spec2data[spec_id].new_data_files.push_back(std::move(f));
    }

    chunked_vector<manifest_file> new_mfiles;
    for (auto& [spec_id, data] : spec2data) {
        const auto* pspec = table_.get_partition_spec(spec_id);
        if (!pspec) {
            vlog(log.error, "partition spec {} not found in metadata", spec_id);
            co_return errc::unexpected_state;
        }

        auto num_old_manifests = data.existing_manifests.size();
        auto binned_mfiles = manifest_packer::pack(
          default_target_size_bytes, std::move(data.existing_manifests));
        vlog(
          log.info,
          "Packed {} manifests into {} bins for partition spec id {}",
          num_old_manifests,
          binned_mfiles.size(),
          spec_id);
        if (binned_mfiles.empty()) {
            // If we had no manifests at all, at least create an empty bin to
            // add new manifests to below.
            binned_mfiles.emplace_back(chunked_vector<manifest_file>{});
        }
        // Always add files to the first bin, which by convention will be the
        // latest data. We may not merge existing manifests if the bin is
        // small, but we'll at least add metadata for the new data files.
        auto merged_bins_res = co_await maybe_merge_mfiles_and_new_data(
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

        // Merge the rest of the bins.
        for (size_t i = 1; i < binned_mfiles.size(); i++) {
            auto& bin = binned_mfiles[i];
            if (bin.size() == 1) {
                // The bin has only a single manifest so there's nothing to do,
                // just add it as is.
                new_mfiles.emplace_back(std::move(bin[0]));
                continue;
            }
            auto merged_bin_res = co_await merge_mfiles(
              std::move(bin), {}, std::nullopt, *pspec, ctx);
            if (merged_bin_res.has_error()) {
                co_return merged_bin_res.error();
            }
            new_mfiles.emplace_back(std::move(merged_bin_res.value()));
        }
    }
    co_return new_mfiles;
}

} // namespace iceberg
