
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

#include "base/seastarx.h"

#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/file.hh>

namespace storage {
struct log_reader_config;
}

namespace cloud_storage {

using chunk_start_offset_t = uint64_t;

enum class chunk_state {
    not_available,
    download_in_progress,
    hydrated,
};

std::ostream& operator<<(std::ostream& os, chunk_state);

struct segment_chunk {
    chunk_state current_state;

    // Handle to chunk data file. Shared among all readers. Put behind an
    // optional to preserve safety when evicting a chunk. When a chunk is no
    // longer used by any reader and has a low enough score to evict, its
    // handle is first removed from the struct before closing, so that any new
    // readers asking to hydrate the chunk at the same time do not get a closed
    // file handle due to race.
    using handle_t = ss::lw_shared_ptr<ss::file>;
    std::optional<handle_t> handle;

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
    using expiry_handler = std::function<void(ss::promise<handle_t>&)>;
    ss::expiring_fifo<ss::promise<handle_t>, expiry_handler> waiters;

    std::strong_ordering operator<=>(const segment_chunk&) const;
};

} // namespace cloud_storage
