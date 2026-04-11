/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <memory>

namespace cloud_topics {

/// Cached L1 object reader with its live I/O stream.
struct cached_l1_reader {
    l1::object_id oid;
    kafka::offset last_object_offset;
    kafka::offset next_offset;
    std::unique_ptr<l1::object_reader> reader;
};

/// Cache of live L1 object readers. Each cached reader holds an open I/O
/// stream positioned at the next offset to read, preserving readahead
/// buffers across fetches. On take, the caller gets the reader directly
/// without reopening a stream.
///
/// Entries are evicted after a 60s TTL or when the cache is full (LRU).
/// With at most max_cached_readers entries, linear scans are cheap.
class l1_reader_cache {
public:
    static constexpr size_t default_max_cached_readers = 128;

    explicit l1_reader_cache(size_t max_readers = default_max_cached_readers)
      : _max_cached_readers(max_readers) {}

    /// Try to take a cached reader for the given partition. Returns the
    /// reader if one exists whose next_offset matches start_offset.
    /// Otherwise returns nullopt.
    std::optional<cached_l1_reader> take_reader(
      const model::topic_id_partition& tidp, kafka::offset start_offset);

    /// Return a reader to the cache for future reuse. If the reader is
    /// exhausted (next_offset > last_object_offset) or the cache is
    /// shutting down, the reader is closed instead. If the cache is
    /// full, the least-recently-used entry is evicted.
    ss::future<> return_reader(
      const model::topic_id_partition& tidp, cached_l1_reader entry);

    /// Close all cached readers and cancel the TTL timer.
    ss::future<> stop();

    /// How long a reader survives without being taken.
    static constexpr std::chrono::seconds ttl{60};

    /// How often the TTL sweep runs.
    static constexpr std::chrono::seconds eviction_interval{10};

private:
    struct cache_entry {
        cached_l1_reader reader;
        model::topic_id_partition tidp;
        ss::lowres_clock::time_point atime;
        safe_intrusive_list_hook _hook;
    };

    ss::future<> evict_stale();
    void arm_timer();

    /// Close an object_reader, swallowing exceptions. Takes ownership so
    /// the reader stays alive for the duration of the close.
    static ss::future<> close_reader_safe(std::unique_ptr<l1::object_reader>);

    size_t _max_cached_readers;
    counted_intrusive_list<cache_entry, &cache_entry::_hook> _entries;
    ss::timer<ss::lowres_clock> _ttl_timer{[this] {
        ssx::spawn_with_gate(_gate, [this] { return evict_stale(); });
    }};
    ss::gate _gate;
};

} // namespace cloud_topics
