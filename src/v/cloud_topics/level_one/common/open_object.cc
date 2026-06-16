/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/open_object.h"

#include "cloud_topics/level_one/common/chunk_data_source.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"

namespace cloud_topics::l1 {

namespace {

// Chunking for a read, given whether the cache is bypassed. Applied by wrapping
// the per-range read_object fetch in a chunk_data_source; chunk_size == 0 means
// "don't chunk" -- pass the whole requested range straight through.
struct read_chunking {
    size_t chunk_size;
    use_chunk_aligned_reads aligned;
};

read_chunking chunking_for(bool skip_cache) {
    const auto& cfg = config::shard_local_cfg();
    if (skip_cache) {
        // Cache-bypassing bulk/one-shot read (leveling, compaction): bound peak
        // memory with the streaming chunk size. Nothing is cached, so there is
        // no (pos,size) key to align to -- chunks start at the read's position.
        return {
          cfg.cloud_topics_l1_streaming_read_chunk_size(),
          use_chunk_aligned_reads::no};
    }
    // Cached native read: read_object stores the whole requested extent in the
    // cache and serves it from a file, so the cache file bounds memory and no
    // stream-level chunking is needed.
    return {0, use_chunk_aligned_reads::yes};
}

// Wrap a per-range fetch so a request for [pos, pos+len) is served as a
// chunk_data_source that lazily pulls fixed-size chunks (only the chunks a read
// touches are fetched). chunk_size == 0 passes the whole range straight through
// (the cached-native case).
fetch_range_fn chunked_fetch(fetch_range_fn inner, read_chunking c) {
    if (c.chunk_size == 0) {
        return inner;
    }
    return [inner = std::move(inner),
            c](size_t pos, size_t len, ss::abort_source* as)
             -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
        co_return ss::input_stream<char>{
          make_chunk_data_source(inner, pos, len, c.chunk_size, as, c.aligned)};
    };
}

} // namespace

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>> open_object(
  io& io_impl,
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id g,
  bool skip_cache) {
    // Native L1 object: read and parse the footer, then hand the handle a fetch
    // that reads data ranges back through io::read_object, with the read's
    // chunking layered on by chunked_fetch.
    auto footer_buf = co_await io_impl.fetch_native_footer(
      extent, as, g, skip_cache);
    if (!footer_buf.has_value()) {
        co_return std::unexpected(footer_buf.error());
    }
    auto footer_result = co_await footer::read(std::move(footer_buf).value());
    if (!std::holds_alternative<footer>(footer_result)) {
        vlog(cd_log.warn, "Failed to parse L1 footer for object {}", extent.id);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    fetch_range_fn inner = [io = &io_impl, oid = extent.id, g, skip_cache](
                             size_t pos, size_t len, ss::abort_source* fetch_as)
      -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
        return io->read_object(
          object_extent{.id = oid, .position = pos, .size = len},
          fetch_as,
          g,
          skip_cache);
    };
    co_return std::make_unique<l1_native_object_handle>(
      std::get<footer>(std::move(footer_result)),
      chunked_fetch(std::move(inner), chunking_for(skip_cache)));
}

} // namespace cloud_topics::l1
