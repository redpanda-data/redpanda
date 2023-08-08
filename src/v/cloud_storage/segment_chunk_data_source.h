
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

#include "cloud_storage/segment_chunk_api.h"
#include "model/fundamental.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

namespace cloud_storage {

class remote_segment_batch_reader;

class chunk_data_source_impl final : public ss::data_source_impl {
public:
    chunk_data_source_impl(
      segment_chunks& chunks,
      remote_segment& segment,
      kafka::offset start,
      kafka::offset end,
      int64_t begin_stream_at,
      ss::file_input_stream_options stream_options,
      std::optional<uint16_t> prefetch_override = std::nullopt,
      std::optional<std::reference_wrapper<remote_segment_batch_reader>> reader
      = std::nullopt);

    chunk_data_source_impl(const chunk_data_source_impl&) = delete;
    chunk_data_source_impl& operator=(const chunk_data_source_impl&) = delete;
    chunk_data_source_impl(chunk_data_source_impl&&) = delete;
    chunk_data_source_impl& operator=(chunk_data_source_impl&&) = delete;

    ~chunk_data_source_impl() override;

    ss::future<ss::temporary_buffer<char>> get() override;

    ss::future<> close() override;

    bool is_transient() const;

private:
    // Acquires the file handle for the given chunk, then opens a file stream
    // into it. The stream for the first chunk starts at the reader config start
    // offset, and the stream for the last chunk ends at the reader config last
    // offset.
    ss::future<> load_stream_for_chunk(chunk_start_offset_t chunk_start);

    ss::future<> maybe_close_stream();
    ss::future<> load_chunk_handle(
      chunk_start_offset_t chunk_start,
      eager_stream_ref eager_stream = std::nullopt);

    ss::future<> skip_stream_to(uint64_t begin);

    segment_chunks& _chunks;
    remote_segment& _segment;

    chunk_start_offset_t _first_chunk_start;
    chunk_start_offset_t _last_chunk_start;
    uint64_t _begin_stream_at;

    chunk_start_offset_t _current_chunk_start;
    std::optional<ss::input_stream<char>> _current_stream{};

    ss::lw_shared_ptr<ss::file> _current_data_file;
    ss::file_input_stream_options _stream_options;

    ss::gate _gate;

    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    std::optional<uint16_t> _prefetch_override;

    enum class stream_type {
        disk,
        download,
    };

    stream_type _current_stream_t;
    chunk_start_offset_t _last_download_end;
    bool _is_transient{false};
    std::optional<std::reference_wrapper<remote_segment_batch_reader>>
      _attached_reader;
};

} // namespace cloud_storage
