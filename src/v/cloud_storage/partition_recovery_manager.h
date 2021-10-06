#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "model/record.h"
#include "s3/client.h"
#include "storage/ntp_config.h"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

#include <compare>
#include <iterator>
#include <vector>

namespace cloud_storage {

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
    /// \return true if log was actually downloaded, false otherwise
    ss::future<bool> download_log(const storage::ntp_config& ntp_cfg);

private:
    s3::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    ss::gate _gate;
    retry_chain_node _root;
};

/// Topic downloader is used to download topic segments from S3 (or compatible
/// storage) during topic re-creation
class partition_downloader {
    static constexpr size_t max_concurrency = 1;

public:
    partition_downloader(
      const storage::ntp_config& ntpc,
      remote* remote,
      s3::bucket_name bucket,
      ss::gate& gate_root,
      retry_chain_node& parent);

    partition_downloader(const partition_downloader&) = delete;
    partition_downloader(partition_downloader&&) = delete;
    partition_downloader& operator=(const partition_downloader&) = delete;
    partition_downloader& operator=(partition_downloader&&) = delete;
    ~partition_downloader() = default;

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    /// \return true if log was actually downloaded, false otherwise
    ss::future<bool> download_log();

private:
    /// Download full log based on manifest data
    ss::future<> download_log(const remote_manifest_path& key);

    ss::future<>
    download_log(const manifest& manifest, const std::filesystem::path& prefix);

    ss::future<manifest> download_manifest(const remote_manifest_path& path);

    struct recovery_material {
        std::vector<remote_manifest_path> paths;
        topic_manifest topic_manifest;
    };

    /// Locate all data needed to recover single partition
    ss::future<recovery_material>
    find_recovery_material(const remote_manifest_path& key);

    /// Find all candidate partition manifests
    ss::future<std::vector<remote_manifest_path>>
    find_matching_partition_manifests(topic_manifest& manifest);

    /// Represents file path of the downloaded file with
    /// the expected final destination. The caller should move
    /// the file and sync the directory.
    struct download_part {
        std::filesystem::path part_prefix;
        std::filesystem::path dest_prefix;
        size_t num_files;
    };

    /// Download file to the target location
    ///
    /// The downloaded file will have a custom suffix
    /// which has to be changed. The downloaded file path
    /// is returned by the futue.
    ss::future<> download_file(
      const remote_segment_path& target,
      const manifest& manifest,
      const download_part& part);

    struct segment {
        remote_segment_path path;
        manifest::segment_meta meta;
    };

    using offset_map_t = absl::btree_map<model::offset, segment>;

    ss::future<offset_map_t> build_offset_map(const recovery_material& mat);

    ss::future<download_part> download_log_with_capped_size(
      const offset_map_t& offset_map,
      const manifest& manifest,
      const std::filesystem::path& prefix,
      size_t max_size);

    ss::future<download_part> download_log_with_capped_time(
      const offset_map_t& offset_map,
      const manifest& manifest,
      const std::filesystem::path& prefix,
      model::timestamp_clock::duration retention_time);

    /// Rename files in the list
    ss::future<> move_parts(download_part dls);

    ss::future<std::optional<model::record_batch_header>>
    read_first_record_header(const std::filesystem::path& path);

    const storage::ntp_config& _ntpc;
    s3::bucket_name _bucket;
    remote* _remote;
    ss::gate& _gate;
    retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
};

} // namespace cloud_storage
