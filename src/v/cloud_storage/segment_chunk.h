
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

#include "model/fundamental.h"
#include "random/simple_time_jitter.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include <absl/container/node_hash_map.h>

namespace storage {
struct log_reader_config;
}

namespace cloud_storage {

class remote_segment;

using file_offset_t = uint64_t;

enum class chunk_state {
    not_available,
    download_in_progress,
    hydrated,
};

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

class segment_chunks {
    using chunk_map_t = absl::btree_map<file_offset_t, segment_chunk>;

public:
    explicit segment_chunks(
      remote_segment& segment, uint64_t max_hydrated_chunks);

    segment_chunks(const segment_chunks&) = delete;
    segment_chunks& operator=(const segment_chunks&) = delete;

    ss::future<> start();
    ss::future<> stop();

    bool downloads_in_progress() const;

    // Hydrates the given chunk id. The remote segment object is used for
    // hydration. The waiters are managed per chunk in `segment_chunk::waiters`.
    // The first reader to request hydration queues the download. The next
    // readers are added to wait list.
    ss::future<segment_chunk::handle_t>
    hydrate_chunk(file_offset_t chunk_start);

    // For all chunks between first and last, increment the
    // required_by_readers_in_future value by one, and increment the
    // required_after_n_chunks values with progressively larger values to denote
    // how far in future the chunk will be required.
    void register_readers(file_offset_t first, file_offset_t last);

    // Mark the first chunk id as acquired by decrementing its
    // required_by_readers_in_future count, and decrement the
    // required_after_n_chunks counts for everything from [first, last] by one.
    // A chunk with required_by_readers_in_future count 0 which does not share
    // its handle with any data source is eligible for trimming.
    void
    mark_acquired_and_update_stats(file_offset_t first, file_offset_t last);

    // Returns reference to metadata for chunk for given chunk id
    segment_chunk& get(file_offset_t);

    file_offset_t get_next_chunk_start(file_offset_t f) const;

    using iterator_t = chunk_map_t::iterator;
    iterator_t begin();
    iterator_t end();

private:
    // Attempts to download chunk into cache and load the file handle into
    // segment_chunk. Should be retried if there is a failure due to cache
    // eviction between download and opening the file handle.
    ss::future<bool>
    do_hydrate_and_materialize(file_offset_t chunk_start, segment_chunk& chunk);

    // Periodically closes chunk file handles for the space to be reclaimable by
    // cache eviction. The chunks are evicted when they are no longer opened for
    // reading by any readers. We also take into account any readers that may
    // need a chunk soon when evicting, by sorting on
    // `segment_chunk::required_by_readers_in_future` and
    // `segment_chunk::required_after_n_chunks` values.
    ss::future<> trim_chunk_files();

    chunk_map_t _chunks;
    remote_segment& _segment;

    simple_time_jitter<ss::lowres_clock> _cache_backoff_jitter;

    simple_time_jitter<ss::lowres_clock> _eviction_jitter;
    ss::timer<ss::lowres_clock> _eviction_timer;

    ss::abort_source _as;
    ss::gate _gate;

    bool _started{false};

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    uint64_t _max_hydrated_chunks;
};

class chunk_data_source_impl final : public ss::data_source_impl {
public:
    chunk_data_source_impl(
      segment_chunks& chunks,
      remote_segment& segment,
      kafka::offset start,
      kafka::offset end,
      file_offset_t begin_stream_at,
      ss::file_input_stream_options stream_options);

    chunk_data_source_impl(const chunk_data_source_impl&) = delete;
    chunk_data_source_impl& operator=(const chunk_data_source_impl&) = delete;

    ~chunk_data_source_impl() override;

    ss::future<ss::temporary_buffer<char>> get() override;

    ss::future<> close() override;

private:
    // Acquires the file handle for the given chunk, then opens a file stream
    // into it. The stream for the first chunk starts at the reader config start
    // offset, and the stream for the last chunk ends at the reader config last
    // offset.
    ss::future<> load_stream_for_chunk(file_offset_t chunk_start);

    segment_chunks& _chunks;
    remote_segment& _segment;

    file_offset_t _first_chunk_start;
    file_offset_t _last_chunk_start;
    file_offset_t _begin_stream_at;

    file_offset_t _current_chunk_start;
    std::optional<ss::input_stream<char>> _current_stream{};

    ss::lw_shared_ptr<ss::file> _current_data_file;
    ss::file_input_stream_options _stream_options;

    ss::gate _gate;

    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

} // namespace cloud_storage
