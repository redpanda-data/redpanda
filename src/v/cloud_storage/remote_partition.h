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
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

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

    static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
    static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;
    static constexpr size_t stm_readahead = 1;

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

    ss::future<> start();

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

    ss::future<> stop();

    /// Translate kafka offset to redpanda offset
    ///
    /// The method only handles base_offsets correctly. This means that
    /// the result will be correct only if the offset is the first on in the
    /// segment (for which we store delta offset).
    /// That's enough to be used for segment lookup but not enough to translate
    /// any offset
    std::optional<model::offset> from_kafka_offset(model::offset o);

private:
    void update_segmnets_incrementally();

    ss::future<> run_eviction_loop();

    void gc_stale_materialized_segments();

    /// The object is used to store data for a reader
    ///
    /// The reader (remote_segment_batch_reader) is storing the
    /// reference to the config. Because of that the reader souldn't
    /// have longer lifetime than the config. This is acheived by
    /// storing both objects in the same struct.
    struct reader_state {
        explicit reader_state(const storage::log_reader_config& cfg)
          : config(cfg)
          , reader(nullptr) {}

        ss::future<> stop() {
            if (reader) {
                co_await reader->stop();
                reader.reset();
            }
        }

        /// Config which was used to create a reader
        storage::log_reader_config config;
        /// Batch reader that can be used to scan the segment
        std::unique_ptr<remote_segment_batch_reader> reader;
    };

    friend struct offloaded_segment_state;

    struct materialized_segment_state;

    /// State that have to be materialized before use
    struct offloaded_segment_state {
        explicit offloaded_segment_state(manifest::key key)
          : key(std::move(key)) {}

        std::unique_ptr<materialized_segment_state>
        materialize(remote_partition& p) {
            auto st = std::make_unique<materialized_segment_state>(key, p);
            return st;
        }

        ss::future<> stop() { return ss::now(); }

        std::unique_ptr<offloaded_segment_state> offload(remote_partition*) {
            auto st = std::make_unique<offloaded_segment_state>(key);
            return st;
        }

        manifest::key key;
    };

    /// State with materialized segment and cached reader
    ///
    /// The object represent the state in which there is(or was) at
    /// least one active reader that consumes data from the
    /// remote segment.
    struct materialized_segment_state {
        materialized_segment_state(manifest::key mk, remote_partition& p)
          : key(std::move(mk))
          , segment(std::make_unique<remote_segment>(
              p._api, p._cache, p._bucket, p._manifest, key, p._rtc))
          , atime(ss::lowres_clock::now()) {
            p._materialized.push_back(*this);
        }

        void return_reader(std::unique_ptr<reader_state> state) {
            atime = ss::lowres_clock::now();
            readers.push_back(std::move(state));
        }

        /// Borrow reader or make a new one.
        /// In either case return a reader.
        std::unique_ptr<reader_state>
        borrow_reader(const storage::log_reader_config& cfg) {
            atime = ss::lowres_clock::now();
            for (auto it = readers.begin(); it != readers.end(); it++) {
                if ((*it)->config.start_offset == cfg.start_offset) {
                    // here we're reusing the existing reader
                    auto tmp = std::move(*it);
                    tmp->config = cfg;
                    readers.erase(it);
                    return tmp;
                }
            }
            // this may only happen if we have some concurrency
            auto state = std::make_unique<reader_state>(cfg);
            state->reader = std::make_unique<remote_segment_batch_reader>(
              *segment, state->config, segment->get_term());
            return state;
        }

        ss::future<> stop() {
            for (auto& rs : readers) {
                if (rs->reader) {
                    co_await rs->reader->stop();
                }
            }
            co_await segment->stop();
        }

        std::unique_ptr<offloaded_segment_state>
        offload(remote_partition* partition) {
            _hook.unlink();
            for (auto&& rs : readers) {
                partition->evict_reader(std::move(rs->reader));
            }
            partition->evict_segment(std::move(segment));
            auto st = std::make_unique<offloaded_segment_state>(key);
            return st;
        }

        manifest::key key;
        std::unique_ptr<remote_segment> segment;
        /// Batch reader that can be used to scan the segment
        std::list<std::unique_ptr<reader_state>> readers;
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
    std::unique_ptr<reader_state>
    borrow_reader(storage::log_reader_config config, segment_state& st);

    /// Return reader back to segment_state
    void return_reader(std::unique_ptr<reader_state>, segment_state& st);

    /// Put reader into the eviction list which will
    /// eventually lead to it being closed and deallocated
    void evict_reader(std::unique_ptr<remote_segment_batch_reader> reader) {
        _eviction_list.push_back(std::move(reader));
        _cvar.signal();
    }
    void evict_segment(std::unique_ptr<remote_segment> segment) {
        _eviction_list.push_back(std::move(segment));
        _cvar.signal();
    }

    using segment_map_t = std::map<model::offset, segment_state>;

    /// Start hydrating segments
    void start_readahead(segment_map_t::iterator current);
    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      std::unique_ptr<remote_segment>>;

    using eviction_list_t = std::deque<evicted_resource_t>;

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    remote& _api;
    cache& _cache;
    const manifest& _manifest;
    ss::lw_shared_ptr<offset_translator> _translator;
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
}; // namespace cloud_storage

} // namespace cloud_storage
