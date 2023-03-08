/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/logger.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/partition_probe.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/segment_state.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "storage/ntp_config.h"
#include "storage/translating_reader.h"
#include "storage/types.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include <chrono>
#include <functional>

namespace cloud_storage {

using namespace std::chrono_literals;

class partition_record_batch_reader_impl;
struct materialized_segment_state;

/// Remote partition manintains list of remote segments
/// and list of active readers. Only one reader can be
/// maintained per segment. The idea here is that the
/// subsequent `make_reader` calls should reuse cached
/// readers. We can expect that the historical reads
/// won't conflict frequently. The conflict will result
/// in rescan of the segment (since we don't have indexes
/// for remote segments).
class remote_partition
  : public ss::enable_shared_from_this<remote_partition>
  , public ss::weakly_referencable<remote_partition> {
    friend class partition_record_batch_reader_impl;

public:
    /// C-tor
    ///
    /// The manifest's lifetime should be bound to the lifetime of the owner
    /// of the remote_partition.
    remote_partition(
      const partition_manifest& m,
      remote& api,
      cache& c,
      cloud_storage_clients::bucket_name bucket);

    /// Start remote partition
    ss::future<> start();

    /// Stop remote partition
    ///
    /// This will stop all readers and background gc loop
    ss::future<> stop();

    /// Create a reader
    ///
    /// Note that config.start_offset and config.max_offset are kafka offsets.
    /// All offset translation is done internally. The returned record batch
    /// reader will produce batches with kafka offsets and the config will be
    /// updated using kafka offsets.
    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt);

    /// Look up offset from timestamp
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg);

    /// Return first uploaded kafka offset
    kafka::offset first_uploaded_offset();

    /// Return last uploaded kafka offset
    model::offset last_uploaded_offset();

    /// Get partition NTP
    const model::ntp& get_ntp() const;

    /// Returns true if at least one segment is uploaded to the bucket
    bool is_data_available() const;

    // returns term last kafka offset
    std::optional<kafka::offset> get_term_last_offset(model::term_id) const;

    // Get list of aborted transactions that overlap with the offset range
    ss::future<std::vector<model::tx_range>>
    aborted_transactions(offset_range offsets);

    /// Helper for erase()
    ss::future<bool> tolerant_delete_object(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage_clients::object_key& path,
      retry_chain_node& parent);

    struct finalize_result {
        // If this is set, use this manifest for deletion instead of the usual
        // local state (the remote content was newer than our local content)
        std::optional<partition_manifest> manifest;
        download_result get_status{download_result::failed};
    };

    /// Flush metadata to object storage, prior to a topic deletion with
    /// remote deletion disabled.
    ss::future<finalize_result>
    finalize(ss::abort_source&, raft::vnode, raft::group_configuration);

    /// Remove objects from S3
    ss::future<>
    erase(ss::abort_source&, raft::vnode, raft::group_configuration);

    /// Hook for materialized_segment to notify us when a segment is evicted
    void offload_segment(model::offset);

private:
    friend struct materialized_segment_state;

    using materialized_segment_ptr
      = std::unique_ptr<materialized_segment_state>;

    using segment_map_t
      = absl::btree_map<model::offset, materialized_segment_ptr>;
    using iterator = segment_map_t::iterator;

    /// This is exposed for the benefit of the materialized_segment_state
    materialized_segments& materialized();

    /// Materialize segment if needed and create a reader
    ///
    /// \param config is a reader config
    /// \param offset_key is an key of the segment state in the _segments
    /// \param st is a segment state referenced by offset_key
    std::unique_ptr<remote_segment_batch_reader> borrow_reader(
      storage::log_reader_config config,
      kafka::offset offset_key,
      materialized_segment_ptr& st);

    /// Return reader back to segment_state
    void return_reader(std::unique_ptr<remote_segment_batch_reader>);

    /// Iterators used by the partition_record_batch_reader_impl class
    iterator seek_by_timestamp(model::timestamp);

    /// The result of the borrow_next_reader method
    ///
    struct borrow_result_t {
        /// The reader (can be set to null)
        std::unique_ptr<remote_segment_batch_reader> reader;
        /// The offset of the next segment, default value means that there is no
        /// next segment yet
        model::offset next_segment_offset;
    };

    /// Borrow next reader in a sequence
    ///
    /// If the invocation is first the method will use config.start_offset to
    /// find the target. It can find already materialized segment and reuse the
    /// reader. Alternatively, it can materialize the segment and create a
    /// reader.
    borrow_result_t borrow_next_reader(
      storage::log_reader_config config, model::offset hint = {});

    /// Materialize new segment
    /// @return iterator that points to newly added segment (always valid
    /// iterator)
    iterator materialize_segment(const segment_meta&);

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    ss::abort_source _as;
    remote& _api;
    cache& _cache;
    const partition_manifest& _manifest;
    cloud_storage_clients::bucket_name _bucket;

    segment_map_t _segments;
    partition_probe _probe;
};

} // namespace cloud_storage
