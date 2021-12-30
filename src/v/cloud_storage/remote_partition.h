/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "s3/client.h"
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

namespace cloud_storage {

class partition_record_batch_reader_impl;

/// Remote partition manintains list of remote segments
/// and list of active readers. Only one reader can be
/// maintained per segment. The idea here is that the
/// subsequent `make_reader` calls should reuse cached
/// readers. We can expect that the historical reads
/// won't conflict frequently. The conflict will result
/// in rescan of the segment (since we don't have indexes
/// for remote segments).
class remote_partition
  : public ss::enable_lw_shared_from_this<remote_partition> {
    friend class partition_record_batch_reader_impl;

    static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
    static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;

public:
    /// C-tor
    ///
    /// The manifest's lifetime should be bound to the lifetime of the owner
    /// of the remote_partition.
    remote_partition(
      const manifest& m, remote& api, cache& c, s3::bucket_name bucket);

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

    /// Return first uploaded kafka offset
    model::offset first_uploaded_offset();

    /// Get partition NTP
    const model::ntp& get_ntp() const;

    /// Returns true if at least one segment is uploaded to the bucket
    bool is_data_available() const;

private:
    /// Create new remote_segment instances for all new
    /// items in the manifest.
    void update_segments_incrementally();

    ss::future<> run_eviction_loop();

    void gc_stale_materialized_segments(bool force_collection);

    friend struct offloaded_segment_state;

    struct materialized_segment_state;

    /// State that have to be materialized before use
    struct offloaded_segment_state {
        explicit offloaded_segment_state(manifest::key key);

        std::unique_ptr<materialized_segment_state>
        materialize(remote_partition& p, model::offset offset_key);

        ss::future<> stop();

        std::unique_ptr<offloaded_segment_state> offload(remote_partition*);

        manifest::key manifest_key;
    };

    /// State with materialized segment and cached reader
    ///
    /// The object represent the state in which there is(or was) at
    /// least one active reader that consumes data from the
    /// remote segment.
    struct materialized_segment_state {
        materialized_segment_state(
          manifest::key mk, model::offset offk, remote_partition& p);

        void return_reader(std::unique_ptr<remote_segment_batch_reader> reader);

        /// Borrow reader or make a new one.
        /// In either case return a reader.
        std::unique_ptr<remote_segment_batch_reader> borrow_reader(
          const storage::log_reader_config& cfg, retry_chain_logger& ctxlog);

        ss::future<> stop();

        std::unique_ptr<offloaded_segment_state>
        offload(remote_partition* partition);

        /// Key of the segment metatdata in the manifest
        manifest::key manifest_key;
        /// Key of the segment in _segments collection of the remote_partition
        model::offset offset_key;
        ss::lw_shared_ptr<remote_segment> segment;
        /// Batch readers that can be used to scan the segment
        std::list<std::unique_ptr<remote_segment_batch_reader>> readers;
        /// Reader access time
        ss::lowres_clock::time_point atime;
        /// List hook for the list of all materalized segments
        intrusive_list_hook _hook;
    };

    using offloaded_segment_ptr = std::unique_ptr<offloaded_segment_state>;
    using materialized_segment_ptr
      = std::unique_ptr<materialized_segment_state>;
    using segment_state
      = std::variant<offloaded_segment_ptr, materialized_segment_ptr>;

    /// Materialize segment if needed and create a reader
    ///
    /// \param config is a reader config
    /// \param offset_key is an key of the segment state in the _segments
    /// \param st is a segment state referenced by offset_key
    std::unique_ptr<remote_segment_batch_reader> borrow_reader(
      storage::log_reader_config config,
      model::offset offset_key,
      segment_state& st);

    /// Return reader back to segment_state
    void return_reader(
      std::unique_ptr<remote_segment_batch_reader>, segment_state& st);

    /// Put reader into the eviction list which will
    /// eventually lead to it being closed and deallocated
    void evict_reader(std::unique_ptr<remote_segment_batch_reader> reader) {
        _eviction_list.push_back(std::move(reader));
        _cvar.signal();
    }
    void evict_segment(ss::lw_shared_ptr<remote_segment> segment) {
        _eviction_list.push_back(std::move(segment));
        _cvar.signal();
    }

    using segment_map_t = std::map<model::offset, segment_state>;

    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      ss::lw_shared_ptr<remote_segment>>;

    using eviction_list_t = std::deque<evicted_resource_t>;

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    ss::abort_source _as;
    remote& _api;
    cache& _cache;
    const manifest& _manifest;
    std::optional<model::offset> _first_uploaded_offset;
    s3::bucket_name _bucket;
    segment_map_t _segments;
    eviction_list_t _eviction_list;
    intrusive_list<
      materialized_segment_state,
      &materialized_segment_state::_hook>
      _materialized;
    ss::condition_variable _cvar;
    /// Timer use to periodically evict stale readers
    ss::timer<ss::lowres_clock> _stm_timer;
    simple_time_jitter<ss::lowres_clock> _stm_jitter;
};

} // namespace cloud_storage
