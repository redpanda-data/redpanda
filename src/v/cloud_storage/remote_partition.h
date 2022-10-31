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
#include "model/fundamental.h"
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

#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include <chrono>
#include <functional>

namespace cloud_storage {

using namespace std::chrono_literals;

class partition_record_batch_reader_impl;
struct materialized_segment_state;

namespace details {

/// Iterator adapter for absl::btree_map that privides
/// iterator stability guarantee by caching the key and
/// doing a lookup on every increment.
/// This turns iterator increment into O(logN) operation
/// Deleting from underlying btree_map is not supported.
template<class TKey, class TVal>
class btree_map_stable_iterator
  : public boost::iterator_facade<
      btree_map_stable_iterator<TKey, TVal>,
      typename absl::btree_map<TKey, TVal>::value_type,
      boost::bidirectional_traversal_tag> {
    using map_t = absl::btree_map<TKey, TVal>;
    using self_t = btree_map_stable_iterator<TKey, TVal>;
    using value_t = typename map_t::value_type;

public:
    /// Creates an iterator that points to the end of the sequence
    explicit btree_map_stable_iterator(map_t& map)
      : _key(std::nullopt)
      , _map(std::ref(map)) {}

    /// Create an iterator that points to the arbitrary element
    explicit btree_map_stable_iterator(map_t& map, TKey o)
      : _key(std::nullopt)
      , _map(std::ref(map)) {
        // Invariant: the iterator is pointing to end (_map is null) or
        // _key is initialized using correct value.
        if (map.empty()) {
            set_end();
        }
        if (auto it = _map.get().find(o); it != _map.get().end()) {
            _key = it->first;
        } else {
            // Same behaviour as map::find, if key can't be found setup
            // iterator to point to end of key sequence.
            set_end();
        }
    }

    /// Returns true if the element referenced by this iterator
    /// was removed from the collection.
    bool is_invalidated() const {
        if (!_key) {
            return true;
        }
        return _map.get().count(*_key) == 0;
    }

private:
    friend class boost::iterator_core_access;

    // Increment iterator if possible.
    // The _key will be set to next element key or nullopt.
    void increment() {
        vassert(
          _key.has_value(), "btree_map_stable_iterator can't be incremented");
        auto it = _map.get().find(*_key);
        // _key should be present since deletions are not supported
        if (it == _map.get().end()) {
            // The element referenced by this iterator was
            // deleted.
            set_end();
            return;
        }
        ++it;
        if (it == _map.get().end()) {
            set_end();
        } else {
            _key = it->first;
        }
    }

    // Decrement iterator if possible.
    // The _key will be set to prev element key.
    void decrement() {
        if (_key) {
            auto it = _map.get().find(*_key);
            if (it == _map.get().end()) {
                set_end();
                return;
            }
            --it;
            _key = it->first;
        } else {
            if (_map.get().empty()) {
                return;
            }
            auto it = _map.get().end();
            --it;
            _key = it->first;
        }
    }

    bool equal(const self_t& other) const { return _key == other._key; }

    value_t& dereference() const {
        vassert(
          _key.has_value(),
          "btree_map_stable_iterator doesn't point to an element and can't be "
          "dereferenced");
        auto it = _map.get().find(*_key);
        return *it;
    }

    void set_end() { _key = std::nullopt; }

    /// Key of the current element, nullopt is iter == end
    std::optional<TKey> _key;
    /// Reference to the container
    std::reference_wrapper<map_t> _map;
};

} // namespace details

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

    static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
    static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;

public:
    /// C-tor
    ///
    /// The manifest's lifetime should be bound to the lifetime of the owner
    /// of the remote_partition.
    remote_partition(
      const partition_manifest& m,
      remote& api,
      cache& c,
      s3::bucket_name bucket);

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
      const s3::bucket_name& bucket,
      const s3::object_key& path,
      retry_chain_node& parent);

    /// Remove objects from S3
    ss::future<> erase();

private:
    /// Create new remote_segment instances for all new
    /// items in the manifest.
    void maybe_sync_with_manifest();

    ss::future<> run_eviction_loop();

    void gc_stale_materialized_segments(bool force_collection);

    friend struct offloaded_segment_state;
    friend struct materialized_segment_state;

    using materialized_segment_ptr
      = std::unique_ptr<materialized_segment_state>;
    using segment_state
      = std::variant<offloaded_segment_state, materialized_segment_ptr>;

    static_assert(
      sizeof(segment_state) == sizeof(std::variant<size_t>),
      "segment_state has unexpected size");

    using iterator
      = details::btree_map_stable_iterator<kafka::offset, segment_state>;

    /// Materialize segment if needed and create a reader
    ///
    /// \param config is a reader config
    /// \param offset_key is an key of the segment state in the _segments
    /// \param st is a segment state referenced by offset_key
    std::unique_ptr<remote_segment_batch_reader> borrow_reader(
      storage::log_reader_config config,
      kafka::offset offset_key,
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

    /// Iterators used by the partition_record_batch_reader_impl class
    iterator begin();
    iterator end();
    iterator upper_bound(kafka::offset);
    iterator seek_by_timestamp(model::timestamp);

    using segment_map_t = absl::btree_map<kafka::offset, segment_state>;
    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      ss::lw_shared_ptr<remote_segment>>;

    using eviction_list_t = std::deque<evicted_resource_t>;

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
    remote& _api;
    cache& _cache;
    const partition_manifest& _manifest;
    model::offset _insync_offset;
    s3::bucket_name _bucket;

    // Deleting from _segments is not supported.
    // absl::btree_map doesn't provide a pointer stabilty. We are
    // using remote_partition::btree_map_stable_iterator to work around this.
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
    partition_probe _probe;
};

} // namespace cloud_storage
