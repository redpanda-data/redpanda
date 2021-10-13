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
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>

namespace cloud_storage {

class record_batch_reader_impl;

/// Remote partition manintains list of remote segments
/// and list of active readers. Only one reader can be
/// maintained per segment. The idea here is that the
/// subsequent `make_reader` calls should reuse cached
/// readers. We can expect that the historical reads
/// won't conflict frequently. The conflict will result
/// in rescan of the segment (since we don't have indexes
/// for remote segments).
class remote_partition : public ss::weakly_referencable<remote_partition> {
    friend class record_batch_reader_impl;

public:
    /// C-tor
    ///
    /// The manifest's lifetime should be bound to the lifetime of the owner
    /// of the remote_partition.
    remote_partition(
      const manifest& m, remote& api, cache& c, s3::bucket_name bucket);

    ~remote_partition() noexcept {
        vlog(
          cst_log.info,
          "remote_partition d-tor, {} segments, {} in eviction list",
          _segments.size(),
          _eviction_list.size());
    }

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt);

    model::offset first_uploaded_offset() const {
        // TODO: cache in the field
        model::offset starting_offset = model::offset::max();
        for (const auto& m : _manifest) {
            starting_offset = std::min(starting_offset, m.second.base_offset);
        }
        return starting_offset;
    }

    model::offset last_uploaded_offset() const {
        return _manifest.get_last_offset();
    }

    ss::future<> stop();

    /// Translate kafka offset to redpanda offset
    ///
    /// The method only handles base_offsets correctly. This means that
    /// the result will be correct only if the offset is the first on in the
    /// segment (for which we store delta offset).
    /// That's enough to be used for segment lookup but not enough to translate
    /// any offset
    std::optional<model::offset> from_kafka_offset(model::offset o) const;

private:
    void update_segmnets_incrementally();

    ss::future<> run_eviction_loop();

    // TODO: periodically clean up stale readers

    struct reader_state {
        explicit reader_state(const storage::log_reader_config& cfg)
          : config(cfg)
          , reader(nullptr)
          , atime(ss::lowres_clock::now()) {}

        /// Config which was used to create a reader
        storage::log_reader_config config;
        /// Batch reader that can be used to scan the segment
        std::unique_ptr<remote_segment_batch_reader> reader;
        /// Reader access time
        ss::lowres_clock::time_point atime;
    };

    struct remote_segment_state {
        /// Remote segment (immutable)
        ss::lw_shared_ptr<remote_segment> segment;
        /// Cached state of the current reader
        std::unique_ptr<reader_state> reader;
    };

    /// Put reader state into the eviction list which will
    /// eventually lead to it being closed and deallocated
    void evict_reader(std::unique_ptr<remote_segment_batch_reader> reader) {
        _eviction_list.push_back(std::move(reader));
        _cvar.signal();
    }

    using segment_map_t = std::map<model::offset, remote_segment_state>;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    remote& _api;
    cache& _cache;
    const manifest& _manifest;
    ss::lw_shared_ptr<offset_translator> _translator;
    s3::bucket_name _bucket;
    segment_map_t _segments;
    std::deque<std::unique_ptr<remote_segment_batch_reader>> _eviction_list;
    ss::condition_variable _cvar;
};

} // namespace cloud_storage
