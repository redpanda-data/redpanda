
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "random/simple_time_jitter.h"
#include "seastarx.h"

#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include <absl/container/node_hash_map.h>

#include <cstdint>

namespace cloud_storage {

class remote_segment;

using segment_chunk_id_t = uint64_t;

enum class chunk_state {
    not_available,
    download_in_progress,
    hydrated,
};

struct segment_chunk {
    using expiry_handler = std::function<void(ss::promise<>&)>;

    chunk_state current_state;

    // Handle to chunk data file. Shared among all readers. Put behind an
    // optional to preserve safety when evicting a chunk. When a chunk is no
    // longer used by any reader and has a low enough score to evict, it's
    // handle is first removed from the struct before closing, so that any new
    // readers asking to hydrate the chunk at the same time do not get a closed
    // file handle due to race.
    std::optional<ss::lw_shared_ptr<ss::file>> handle;

    // Keeps track of the number of readers which will require the chunk in
    // future, after they have read their current chunk. For example if a
    // reader's fetch config spans chunks [4,10], then while it is reading chunk
    // 4, it should increment this count for all the readers [5,10]. This helps
    // the eviction process decide which chunks are not required soon.
    uint64_t required_by_readers_in_future{0};

    // How soon does a reader require this chunk after it is done reading the
    // current chunk. For example given a reader with fetch config [4,10] it
    // requires chunk 5 first and chunk 10 last after it is done reading
    // chunk 4. So it sets incrementing required_after_n_chunks values into each
    // chunk [5,10]. Helps to break ties if two chunks have the same number of
    // required_by_readers_in_future, the chunk with this field lower is more
    // urgently required.
    uint64_t required_after_n_chunks{0};

    // List of readers waiting to hydrate this chunk. The first reader starts
    // hydration and the others wait for the hydration to finish.
    ss::expiring_fifo<ss::promise<>, expiry_handler> waiters;
};

struct segment_chunks {
    using chunk_map_t = absl::node_hash_map<segment_chunk_id_t, segment_chunk>;

    // Hydrates the given chunk id. The remote segment object is used for
    // hydration. The waiters are managed per chunk in `segment_chunk::waiters`.
    // The first reader to request hydration queues the download. The next
    // readers are added to wait list.
    ss::future<> hydrate_chunk_id(segment_chunk_id_t chunk_id);

    // Attempts to download chunk into cache and load the file handle into
    // segment_chunk. Should be retried if there is a failure due to cache
    // eviction between download and opening the file handle.
    ss::future<bool> do_hydrate_and_materialize(
      segment_chunk_id_t chunk_id, segment_chunk& chunk);

    // Periodically closes chunk file handles for the space to be reclaimable by
    // cache eviction. The chunks are evicted when they are no longer opened for
    // reading by any readers. We also take into account any readers that may
    // need a chunk soon when evicting, by sorting on
    // `segment_chunk::required_by_readers_in_future` and
    // `segment_chunk::required_after_n_chunks` values.
    ss::future<> release_unused_chunks();

    chunk_map_t chunks;
    remote_segment& segment;
    simple_time_jitter<ss::lowres_clock> _cache_backoff_jitter;
    ss::abort_source _as;
};

} // namespace cloud_storage
