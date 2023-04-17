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

#include "cloud_storage/remote_segment.h"

namespace cloud_storage {

ss::future<> segment_chunks::hydrate_chunk_id(segment_chunk_id_t chunk_id) {
    auto& chunk = chunks[chunk_id];
    auto curr_state = chunk.current_state;
    if (curr_state == chunk_state::hydrated) {
        co_return;
    }

    if (curr_state == chunk_state::download_in_progress) {
        ss::promise<> p;
        auto f = p.get_future();
        chunk.waiters.push_back(
          std::move(p), ss::lowres_clock::time_point::max());
        co_return co_await f.discard_result();
    }

    auto file_handle = co_await segment.hydrate_chunk_id(chunk_id);
    chunk.handle = std::move(file_handle);
    chunk.current_state = chunk_state::hydrated;

    while (!chunk.waiters.empty()) {
        auto& p = chunk.waiters.front();
        p.set_value();
        chunk.waiters.pop_front();
    }
}

ss::future<> segment_chunks::release_unused_chunks() {
    std::vector<chunk_map_t::iterator> to_release;
    uint64_t hydrated_chunks = 0;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        const auto& metadata = it->second;
        if (metadata.current_state == chunk_state::hydrated) {
            hydrated_chunks += 1;
        }

        if (
          metadata.current_state == chunk_state::hydrated
          && metadata.handle.has_value()) {
            if (metadata.handle->owned()) {
                to_release.push_back(it);
            }
        }
    }

    std::sort(
      to_release.begin(),
      to_release.end(),
      [](const auto& it_a, const auto& it_b) {
          if (
            it_a->second.required_by_readers_in_future
            != it_b->second.required_by_readers_in_future) {
              return it_a->second.required_by_readers_in_future
                     < it_b->second.required_by_readers_in_future;
          }

          return it_a->second.required_after_n_chunks
                 > it_a->second.required_after_n_chunks;
      });

    for (auto& it : to_release) {
        if (hydrated_chunks <= max_chunks_to_keep_hydrated) {
            break;
        }

        auto handle = std::move(it->second.handle.value());
        co_await handle->close();
        hydrated_chunks -= 1;
    }
}

} // namespace cloud_storage
