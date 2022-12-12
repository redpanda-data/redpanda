/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "model/metadata.h"
#include "model/record.h"
#include "s3/s3_client.h"
#include "storage/ntp_config.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cloud_storage {

/// Log download result
///
/// The struct contains information about the
/// download completion status and the offset of the
/// last record batch being downloaded.
struct log_recovery_result {
    bool completed{false};
    model::offset min_kafka_offset;
    model::offset max_kafka_offset;
    cloud_storage::partition_manifest manifest;
};

/// Data recovery provider is used to download topic segments from S3 (or
/// compatible storage) during topic re-creation process
class partition_recovery_manager {
public:
    partition_recovery_manager(
      s3::bucket_name bucket, ss::sharded<remote>& remote);

    partition_recovery_manager(const partition_recovery_manager&) = delete;
    partition_recovery_manager(partition_recovery_manager&&) = delete;
    partition_recovery_manager& operator=(const partition_recovery_manager&)
      = delete;
    partition_recovery_manager& operator=(partition_recovery_manager&&)
      = delete;

    ~partition_recovery_manager();

    ss::future<> stop();

    void cancel();

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    /// \return download result struct that contains 'completed=true'
    ///         if actual download happened. The 'last_offset' field will
    ///         be set to max offset of the downloaded log.
    ss::future<log_recovery_result> download_log(
      const storage::ntp_config& ntp_cfg,
      model::initial_revision_id remote_revsion,
      int32_t remote_partition_count);

private:
    s3::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    ss::gate _gate;
    retry_chain_node _root;
    ss::abort_source _as;
};

/// Topic downloader is used to download topic segments from S3 (or compatible
/// storage) during topic re-creation
class partition_downloader {
    static constexpr size_t max_concurrency = 1;

public:
    partition_downloader(
      const storage::ntp_config& ntpc,
      remote* remote,
      model::initial_revision_id remote_revision_id,
      int32_t remote_partition_count,
      s3::bucket_name bucket,
      ss::gate& gate_root,
      retry_chain_node& parent,
      storage::opt_abort_source_t as);

    partition_downloader(const partition_downloader&) = delete;
    partition_downloader(partition_downloader&&) = delete;
    partition_downloader& operator=(const partition_downloader&) = delete;
    partition_downloader& operator=(partition_downloader&&) = delete;
    ~partition_downloader() = default;

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    /// \return download result struct that contains 'log_completed=true'
    ///         if actual download happened. The 'last_offset' field will
    ///         be set to max offset of the downloaded log.
    ss::future<log_recovery_result> download_log();

private:
    /// Download full log based on manifest data
    ss::future<log_recovery_result>
    download_log(const remote_manifest_path& key);

    ss::future<> download_log(
      const partition_manifest& manifest, const std::filesystem::path& prefix);

    ss::future<partition_manifest>
    download_manifest(const remote_manifest_path& path);

    struct recovery_material {
        partition_manifest partition_manifest;
    };

    /// Locate all data needed to recover single partition
    ss::future<recovery_material>
    find_recovery_material(const remote_manifest_path& key);

    struct offset_range {
        model::offset min_offset;
        model::offset max_offset;

        friend auto operator<=>(const offset_range&, const offset_range&)
          = default;
    };

    /// Represents file path of the downloaded file with
    /// the expected final destination. The caller should move
    /// the file and sync the directory.
    struct download_part {
        std::filesystem::path part_prefix;
        std::filesystem::path dest_prefix;
        size_t num_files;
        offset_range range;
    };

    /// Sort offsets and find out the useful offset range
    /// without the gaps. Update download_part using this information.
    void update_downloaded_offsets(
      std::vector<partition_downloader::offset_range> dloffsets,
      partition_downloader::download_part& dlpart);

    /// Download segment file to the target location
    ///
    /// The downloaded file will have a custom suffix
    /// which has to be changed. The downloaded file path
    /// is returned by the futue.
    ss::future<std::optional<cloud_storage::stream_stats>>
    download_segment_file(const segment_meta& segm, const download_part& part);

    /// Helper for download_segment_file
    ss::future<uint64_t> download_segment_file_stream(
      uint64_t,
      ss::input_stream<char>,
      const download_part&,
      remote_segment_path,
      std::filesystem::path,
      offset_translator,
      stream_stats&);

    using offset_map_t = absl::btree_map<model::offset, segment_meta>;

    ss::future<download_part> download_log_with_capped_size(
      offset_map_t offset_map,
      const partition_manifest& manifest,
      const std::filesystem::path& prefix,
      size_t max_size);

    ss::future<download_part> download_log_with_capped_time(
      offset_map_t offset_map,
      const partition_manifest& manifest,
      const std::filesystem::path& prefix,
      model::timestamp_clock::duration retention_time);

    /// Rename files in the list
    ss::future<> move_parts(download_part dls);

    ss::future<std::optional<model::record_batch_header>>
    read_first_record_header(const std::filesystem::path& path);

    const storage::ntp_config& _ntpc;
    s3::bucket_name _bucket;
    remote* _remote;
    model::initial_revision_id _remote_revision_id;
    int32_t _remote_partition_count;
    ss::gate& _gate;
    retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
    storage::opt_abort_source_t _as;
};

} // namespace cloud_storage
