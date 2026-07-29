/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace cloud_topics::l1 {

/// Controls where the first chunk starts. When yes, chunk boundaries are
/// chunk_size aligned, so overlapping reads request identical
/// (position, length) chunks that a downstream cache can dedup. When no, the
/// first chunk starts exactly at start_pos -- for a caller with no cache to
/// dedup against, which would otherwise fetch and discard the bytes between the
/// chunk boundary and start_pos.
using use_chunk_aligned_reads
  = ss::bool_class<struct use_chunk_aligned_reads_tag>;

/// A data source that fetches the byte range [start_pos, start_pos+total_len)
/// one fixed-size chunk at a time, lazily, and presents the result as a single
/// contiguous stream.
///
/// Only the chunks actually consumed are fetched: a reader that stops early
/// simply stops pulling, so the un-read tail is never fetched. Chunks are fixed
/// byte ranges and need not be aligned to any structure in the data -- the
/// source concatenates one chunk's tail with the next chunk's head, so a record
/// spanning a boundary is read seamlessly. EOF is signalled only at the true
/// end (start_pos+total_len), so a consumer never mistakes a chunk boundary for
/// the end of the data.
///
/// `Fetch` is any callable
///   (size_t file_position, size_t length, ss::abort_source*)
///     -> ss::future<std::expected<ss::input_stream<char>, E>>
/// for any formattable error `E`. A chunk-fetch failure (an unexpected result)
/// surfaces as an exception from get(); a record-batch reader propagates it
/// (its read API has no error variant), matching how such readers already
/// report stream errors.
template<typename Fetch>
class chunk_data_source final : public ss::data_source_impl {
public:
    /// See use_chunk_aligned_reads. When yes the first fetch is aligned
    /// at or before start_pos and the leading `start_pos % chunk_size` bytes
    /// are skipped; when no the first chunk starts exactly at start_pos with no
    /// skipped head.
    chunk_data_source(
      Fetch fetch,
      size_t start_pos,
      size_t total_len,
      size_t chunk_size,
      ss::abort_source* as,
      use_chunk_aligned_reads aligned)
      : _fetch(std::move(fetch))
      , _end(start_pos + total_len)
      , _chunk_size(chunk_size)
      , _next_chunk_start(
          aligned ? start_pos - (start_pos % chunk_size) : start_pos)
      , _skip_head(aligned ? start_pos % chunk_size : 0)
      , _as(as) {
        vassert(
          chunk_size > 0, "chunk_data_source requires a non-zero chunk size");
    }

    ss::future<ss::temporary_buffer<char>> get() override {
        while (true) {
            // Surface an abort as an exception on every read -- including while
            // draining a buffered chunk or between chunks -- so a cancelled
            // read fails loudly rather than terminating as a (truncated) clean
            // end-of-stream.
            if (_as != nullptr) {
                _as->check();
            }
            if (!_cur.has_value()) {
                if (_next_chunk_start >= _end) {
                    // Past the requested range: true EOF.
                    co_return ss::temporary_buffer<char>{};
                }
                auto chunk_end = std::min(
                  _next_chunk_start + _chunk_size, _end);
                auto len = chunk_end - _next_chunk_start;
                auto stream = co_await _fetch(_next_chunk_start, len, _as);
                if (!stream.has_value()) {
                    throw std::runtime_error(
                      fmt::format(
                        "failed to fetch chunk at byte {} (len {}): {}",
                        _next_chunk_start,
                        len,
                        stream.error()));
                }
                _cur = std::move(*stream);
                if (_skip_head > 0) {
                    co_await _cur->skip(_skip_head);
                    _skip_head = 0;
                }
                _next_chunk_start += _chunk_size;
            }
            auto buf = co_await _cur->read();
            if (buf.empty()) {
                // Current chunk exhausted; advance to the next.
                co_await _cur->close();
                _cur.reset();
                continue;
            }
            co_return buf;
        }
    }

    ss::future<> close() override {
        if (_cur.has_value()) {
            co_await _cur->close();
            _cur.reset();
        }
    }

private:
    Fetch _fetch;
    size_t _end;
    size_t _chunk_size;
    // Start of the next chunk to fetch. When chunk-aligned this is the chunk
    // boundary at or before start_pos; otherwise it is start_pos itself.
    size_t _next_chunk_start;
    // Bytes to discard from the first chunk so the stream starts at start_pos
    // (non-zero only when chunk-aligned and start_pos sits mid-chunk). Zeroed
    // after the first chunk.
    size_t _skip_head;
    std::optional<ss::input_stream<char>> _cur;
    ss::abort_source* _as;
};

/// Deduce `Fetch` and wrap a chunk_data_source in an ss::data_source.
template<typename Fetch>
ss::data_source make_chunk_data_source(
  Fetch fetch,
  size_t start_pos,
  size_t total_len,
  size_t chunk_size,
  ss::abort_source* as,
  use_chunk_aligned_reads aligned) {
    return ss::data_source{std::make_unique<chunk_data_source<Fetch>>(
      std::move(fetch), start_pos, total_len, chunk_size, as, aligned)};
}

} // namespace cloud_topics::l1
