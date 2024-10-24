/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_chunk.h"

namespace cloud_storage {

std::ostream& operator<<(std::ostream& os, chunk_state c) {
    switch (c) {
    case chunk_state::not_available:
        return os << "not available";
    case chunk_state::download_in_progress:
        return os << "download in progress";
    case chunk_state::hydrated:
        return os << "hydrated";
    }
}

std::strong_ordering
segment_chunk::operator<=>(const segment_chunk& chunk) const {
    const auto cmp = required_by_readers_in_future
                     <=> chunk.required_by_readers_in_future;
    if (cmp != std::strong_ordering::equal) {
        return cmp;
    }

    // A low required_after_n_chunks means the chunk is required sooner than the
    // other chunk. However the default value of required_after_n_chunks is 0
    // which can erroneously win in sorting by appearing lowest, so if one of
    // the two compared numbers is 0 and the other is not, the non zero number
    // must win.

    const auto required_this = required_after_n_chunks > 0
                                 ? required_after_n_chunks
                                 : std::numeric_limits<uint64_t>::max();
    const auto required_that = chunk.required_after_n_chunks > 0
                                 ? chunk.required_after_n_chunks
                                 : std::numeric_limits<uint64_t>::max();
    return required_that <=> required_this;
}

} // namespace cloud_storage
