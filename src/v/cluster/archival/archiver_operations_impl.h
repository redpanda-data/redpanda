/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "cluster/archival/archiver_operations_api.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

namespace archival {

namespace detail {
// Note: here we have several interfaces for easy mocking. They are used by the
// archiver_operations_api implementation instead of the real redpanda services.

struct cluster_partition_api {
    cluster_partition_api() = default;
    cluster_partition_api(const cluster_partition_api&) = delete;
    cluster_partition_api(cluster_partition_api&&) = delete;
    cluster_partition_api& operator=(const cluster_partition_api&) = delete;
    cluster_partition_api& operator=(cluster_partition_api&&) = delete;

    virtual ~cluster_partition_api() = default;
    virtual const cloud_storage::partition_manifest& manifest() const = 0;
    virtual model::offset get_next_uploaded_offset() const = 0;
    virtual model::offset get_applied_offset() const = 0;
    virtual model::producer_id get_highest_producer_id() const = 0;

    /// Same as log->offset_delta
    /// \throws std::runtime_error if offset is out of range
    virtual model::offset_delta offset_delta(model::offset o) const = 0;

    virtual std::optional<model::term_id>
      get_offset_term(model::offset) const = 0;

    virtual model::initial_revision_id get_initial_revision() const = 0;

    // aborted transactions
    virtual ss::future<fragmented_vector<model::tx_range>>
    aborted_transactions(model::offset base, model::offset last) const = 0;

    // Replicate archival metadata add_segment command
    //
    // The method accepts 'clean_offset' which is the insync offset of the
    // last uploaded manifest. This is a projected clean offset. It is possible
    // for the archiver to upload the manifest  in parallel with segments.
    // This means that when the `add_segments` command is called the uploaded
    // version of the manifest becomes out of sync. The manifest upload is going
    // one step behind the replication. Instead of uploading manifest after
    // replication we're doing this before the replication to pipeline
    // operations.
    //
    // The data upload workflow keeps track of this by storing two offsets.
    // - clean offset is the offset of the last uploaded manifest
    // - dirty offset is the current offset of the STM manifest
    //
    // If clean != dirty then the manifest in the cloud doesn't match the STM.
    // The 'clean' offset is passed into this method and triggers 'mark_clean'
    // command to be added to the replicated batch. When the commands are
    // applied to the STM the 'dirty' offset is propagated forward. This offset
    // is returned from this method and passed back to the workflow so the
    // workflow could track this offset and inform the scheduler about the out
    // of sync manifest.
    virtual ss::future<result<model::offset>> add_segments(
      std::vector<cloud_storage::segment_meta>,
      std::optional<model::offset> clean_offset,
      std::optional<model::offset> read_write_fence,
      model::producer_id highest_pid,
      ss::lowres_clock::time_point deadline,
      ss::abort_source& external_as,
      bool is_validated) noexcept
      = 0;

    virtual cloud_storage::remote_segment_path
    get_remote_segment_path(const cloud_storage::segment_meta&)
      = 0;

    virtual cloud_storage::remote_manifest_path
    get_remote_manifest_path(const cloud_storage::base_manifest&)
      = 0;
};

struct cluster_partition_manager_api {
    cluster_partition_manager_api() = default;
    virtual ~cluster_partition_manager_api() = default;
    cluster_partition_manager_api(const cluster_partition_manager_api&)
      = delete;
    cluster_partition_manager_api(cluster_partition_manager_api&&) = delete;
    cluster_partition_manager_api&
    operator=(const cluster_partition_manager_api&)
      = delete;
    cluster_partition_manager_api& operator=(cluster_partition_manager_api&&)
      = delete;

    virtual ss::shared_ptr<cluster_partition_api>
    get_partition(const model::ntp&) = 0;
};

using upload_result = cloud_storage::upload_result;

/// Wrapper for the cloud_storage::remote
struct cloud_storage_remote_api {
    cloud_storage_remote_api() = default;
    cloud_storage_remote_api(const cloud_storage_remote_api&) = delete;
    cloud_storage_remote_api(cloud_storage_remote_api&&) = delete;
    cloud_storage_remote_api& operator=(const cloud_storage_remote_api&)
      = delete;
    cloud_storage_remote_api& operator=(cloud_storage_remote_api&&) = delete;
    virtual ~cloud_storage_remote_api() = default;

    /// Upload object to the cloud storage
    ///
    /// This method is similar to 'cloud_storage::remote::upload_segment' but
    /// it's not performing any retries. The upload workflow should retry in
    /// case of error. Otherwise the retries are invisible to the scheduler and
    /// the workflow. The method can be used to upload different types of
    /// objects. Not only segments. The type of object is encoded in the 'type'
    /// parameter and the key is generic.
    virtual ss::future<upload_result> upload_stream(
      const cloud_storage_clients::bucket_name& bucket,
      cloud_storage_clients::object_key key,
      uint64_t content_length,
      ss::input_stream<char> stream,
      cloud_storage::upload_type type,
      retry_chain_node& parent)
      = 0;

    /// Upload manifest to the cloud storage location
    ///
    /// The location is defined by the manifest itself
    virtual ss::future<upload_result> upload_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage::base_manifest& manifest,
      cloud_storage::remote_manifest_path path,
      retry_chain_node& parent)
      = 0;
};

struct prepared_segment_upload {
    inclusive_offset_range offsets;
    size_t size_bytes{0};
    bool is_compacted{false};
    cloud_storage::segment_meta meta;
    ss::input_stream<char> payload;
};

/// Wrapper for the archival::segment_upload
struct segment_upload_builder_api {
    segment_upload_builder_api() = default;
    segment_upload_builder_api(const segment_upload_builder_api&) = default;
    segment_upload_builder_api(segment_upload_builder_api&&) = default;
    segment_upload_builder_api& operator=(const segment_upload_builder_api&)
      = default;
    segment_upload_builder_api& operator=(segment_upload_builder_api&&)
      = default;
    virtual ~segment_upload_builder_api() = default;

    virtual ss::future<result<std::unique_ptr<prepared_segment_upload>>>
    prepare_segment_upload(
      ss::shared_ptr<cluster_partition_api> part,
      size_limited_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline)
      = 0;
};

inline std::ostream& operator<<(std::ostream& o, const cluster_partition_api&) {
    return o << "cluster_partition_api";
}
inline std::ostream&
operator<<(std::ostream& o, const cloud_storage_remote_api&) {
    return o << "cloud_storage_remote_api";
}
inline std::ostream&
operator<<(std::ostream& o, const cluster_partition_manager_api&) {
    return o << "cluster_partition_manager_api";
}
inline std::ostream&
operator<<(std::ostream& o, const segment_upload_builder_api&) {
    return o << "segment_upload_builder_api";
}

/// Create archiver_operations_api instance with mock objects
ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  std::unique_ptr<cloud_storage_remote_api>,
  std::unique_ptr<cluster_partition_manager_api>,
  std::unique_ptr<segment_upload_builder_api>,
  cloud_storage_clients::bucket_name);

/// Create a wrapper for cluster::partition
///
/// This function is intended for testing only
std::unique_ptr<cluster_partition_api>
  make_cluster_partition_wrapper(ss::lw_shared_ptr<cluster::partition>);

/// Create a wrapper for upload_builder
///
/// This is needed for testing because the wrapper is relatively
/// complex and requires some additional testing (without mocking)
std::unique_ptr<segment_upload_builder_api>
make_segment_upload_builder_wrapper();

} // namespace detail

/// Create archiver_operations_api instance
ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster::partition_manager>& pm,
  cloud_storage_clients::bucket_name bucket,
  ss::scheduling_group sg);

} // namespace archival
