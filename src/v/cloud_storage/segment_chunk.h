
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
    std::optional<ss::lw_shared_ptr<ss::file>> handle;
    uint64_t required_by_readers_in_future{0};
    uint64_t required_after_n_chunks{0};
    ss::expiring_fifo<ss::promise<>, expiry_handler> waiters;
};

struct segment_chunks {
    using chunk_map_t = absl::node_hash_map<segment_chunk_id_t, segment_chunk>;

    ss::future<> hydrate_chunk_id(segment_chunk_id_t chunk_id);
    ss::future<> release_unused_chunks();

    chunk_map_t chunks;
    remote_segment& segment;
    uint64_t max_chunks_to_keep_hydrated;
};

struct chunk_data_source_impl final : public ss::data_source_impl {};

} // namespace cloud_storage