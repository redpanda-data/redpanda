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
#include "cloud_storage/remote_label.h"
#include "cloud_storage/remote_path_provider.h"
#include "model/metadata.h"
#include "model/record.h"
#include "storage/ntp_config.h"
#include "storage/offset_translator_state.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <vector>

namespace cluster {
class topic_recovery_status_frontend;
}

namespace cloud_storage {

struct topic_recovery_service;

/// Log download result
///
/// The struct contains information about the
/// download completion status and the offset of the
/// last record batch being downloaded.
struct log_recovery_result {
    bool logs_recovered{false};
    bool clean_download{false};
    model::offset min_offset;
    model::offset max_offset;
    cloud_storage::partition_manifest manifest;
    ss::lw_shared_ptr<storage::offset_translator_state> ot_state;
};

/// Data recovery provider is used to download topic segments from S3 (or
/// compatible storage) during topic re-creation process
class partition_recovery_manager {
public:
    partition_recovery_manager(
      cloud_storage_clients::bucket_name bucket, ss::sharded<remote>& remote);

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
      int32_t remote_partition_count,
      cloud_storage::remote_path_provider& path_provider);

    void set_topic_recovery_components(
      ss::sharded<cluster::topic_recovery_status_frontend>&
        topic_recovery_status_frontend,
      ss::sharded<cloud_storage::topic_recovery_service>&
        topic_recovery_service);

private:
    ss::future<bool> is_topic_recovery_active() const;

    cloud_storage_clients::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    // Late initialized objects
    std::optional<std::reference_wrapper<
      ss::sharded<cluster::topic_recovery_status_frontend>>>
      _topic_recovery_status_frontend;
    std::optional<std::reference_wrapper<
      ss::sharded<cloud_storage::topic_recovery_service>>>
      _topic_recovery_service;
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
      const cloud_storage::remote_path_provider& path_provider,
      remote* remote,
      model::initial_revision_id remote_revision_id,
      int32_t remote_partition_count,
      cloud_storage_clients::bucket_name bucket,
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
    ss::future<log_recovery_result> maybe_download_log();

private:
    /// Download log segments from S3 to the local storage
    /// if required.
    ss::future<log_recovery_result> download_log();

    ss::future<> download_log(
      const partition_manifest& manifest, const std::filesystem::path& prefix);

    struct recovery_material {
        partition_manifest partition_manifest;
    };

    /// Locate all data needed to recover single partition
    ss::future<recovery_material> find_recovery_material();

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
        ss::lw_shared_ptr<storage::offset_translator_state> ot_state;
        // This flag is set to false when we failed to download something
        // during recovery. Only partitions which are restored cleanly are
        // allowed to use tiered-storage to avoid data corruption.
        bool clean_download{false};
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
    const cloud_storage::remote_path_provider& _remote_path_provider;
    cloud_storage_clients::bucket_name _bucket;
    remote* _remote;
    model::initial_revision_id _remote_revision_id;
    int32_t _remote_partition_count;
    ss::gate& _gate;
    retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
    storage::opt_abort_source_t _as;
};

} // namespace cloud_storage
