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

#include "cloud_storage/fwd.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/segment_state.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "storage/translating_reader.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

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
      ss::shared_ptr<async_manifest_view> m,
      remote& api,
      cache& c,
      cloud_storage_clients::bucket_name bucket,
      partition_probe& probe);

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

    static size_t reader_mem_use_estimate() noexcept;

    /// Look up offset from timestamp
    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg);

    /// Whether a timequery on this timestamp can match this remote log
    /// log (i.e. whether it is <= the max offset).  Timestamps below the
    /// base_timestamp will also return true, as Kafka timequery semantics
    /// are that a timestamp below the start of a log will match the first
    /// record in the log.
    bool bounds_timestamp(model::timestamp) const;

    /// Return first uploaded kafka offset
    kafka::offset first_uploaded_offset();

    /// Return the offset one past the end of the last offset (i.e. the high
    /// watermark as reported by object storage).
    kafka::offset next_kafka_offset();

    /// Get partition NTP
    const model::ntp& get_ntp() const;

    /// Returns true if at least one segment is uploaded to the bucket
    bool is_data_available() const;

    uint64_t cloud_log_size() const;

    // Serialize the manifest to an ss::output_stream in JSON format.
    // Note that the caller must hold the archival_metadat_stm::_manifest_lock
    // and keep the stream alive until the future completes.
    ss::future<> serialize_json_manifest_to_output_stream(
      ss::output_stream<char>& output) const;

    // returns term last kafka offset
    ss::future<std::optional<kafka::offset>>
      get_term_last_offset(model::term_id) const;

    // Get list of aborted transactions that overlap with the offset range
    ss::future<std::vector<model::tx_range>>
    aborted_transactions(offset_range offsets);

    /// Do background flush metadata to object storage, prior to a topic
    /// deletion
    void finalize();

    enum class erase_result { erased, failed };

    static ss::future<erase_result> erase(
      cloud_storage::remote&,
      cloud_storage_clients::bucket_name,
      const remote_path_provider& path_provider,
      partition_manifest,
      remote_manifest_path,
      retry_chain_node&);

    /// Hook for materialized_segment to notify us when a segment is evicted
    void offload_segment(model::offset);

    // Place on the eviction queue.
    void
    evict_segment_reader(std::unique_ptr<remote_segment_batch_reader> reader);
    void evict_segment(ss::lw_shared_ptr<remote_segment> segment);

    // Compute cache usage statistics. This method uses information from the
    // most recent segment to determine target cache size needs. The results
    // depend on if the partition appears to be able to use chunk-based storage
    // vs segment-based storage in the cache.
    cache_usage_target get_cache_usage_target() const;

private:
    friend struct materialized_segment_state;

    using materialized_segment_ptr
      = std::unique_ptr<materialized_segment_state>;

    using segment_map_t
      = absl::btree_map<model::offset, materialized_segment_ptr>;
    using iterator = segment_map_t::iterator;

    /// This is exposed for the benefit of the materialized_segment_state
    materialized_resources& materialized();

    ss::future<> run_eviction_loop();

    /// Materialize segment if needed and create a reader
    ///
    /// \param config is a reader config
    /// \param offset_key is an key of the segment state in the _segments
    /// \param st is a segment state referenced by offset_key
    std::unique_ptr<remote_segment_batch_reader> borrow_segment_reader(
      storage::log_reader_config config,
      kafka::offset offset_key,
      materialized_segment_ptr& st);

    /// Return reader back to segment_state
    void return_segment_reader(std::unique_ptr<remote_segment_batch_reader>);

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
    borrow_result_t borrow_next_segment_reader(
      const partition_manifest& manifest,
      storage::log_reader_config config,
      segment_units segment_unit,
      segment_reader_units segment_reader_unit,
      model::offset hint = {});

    /// Materialize a new segment or grab one if it already exists
    /// @return iterator that points a materialized segment (always valid
    /// iterator)
    iterator get_or_materialize_segment(
      const remote_segment_path& path, const segment_meta&, segment_units);

    model::ntp _ntp;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    ss::abort_source _as;
    remote& _api;
    cache& _cache;
    ss::shared_ptr<async_manifest_view> _manifest_view;
    cloud_storage_clients::bucket_name _bucket;

    /// Special item in eviction_list that holds a promise and sets it
    /// when the eviction fiber calls stop() (see flush_evicted)
    struct eviction_barrier {
        ss::promise<> promise;

        ss::future<> stop() {
            promise.set_value();
            stopped = true;
            return ss::now();
        }

        bool stopped{false};

        bool is_stopped() const { return stopped; }
    };

    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      ss::lw_shared_ptr<remote_segment>,
      ss::lw_shared_ptr<eviction_barrier>>;
    using eviction_list_t = std::deque<evicted_resource_t>;

    /// Kick this condition variable when appending to eviction_list
    ss::condition_variable _has_evictions_cvar;

    /// List of segments and readers waiting to have their stop() method
    /// called before destruction
    eviction_list_t _eviction_pending;
    segment_map_t _segments;
    partition_probe& _probe;
    ts_read_path_probe& _ts_probe;
};

} // namespace cloud_storage
