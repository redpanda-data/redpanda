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

#include <seastar/util/bool_class.hh>

namespace cloud_topics::l1 {

namespace {

/// Whether reads bypass the cloud cache, as io::read_object's skip_cache does.
using bypass_cache = ss::bool_class<struct bypass_cache_tag>;

// Chunking for a read, given whether the cache is bypassed. Applied by wrapping
// the per-range read_object fetch in a chunk_data_source; chunk_size == 0 means
// "don't chunk" -- pass the whole requested range straight through.
struct read_chunking {
    size_t chunk_size;
    use_chunk_aligned_reads aligned;
};

read_chunking chunking_for(bypass_cache bypass) {
    const auto& cfg = config::shard_local_cfg();
    if (bypass) {
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

// Build a function (pos, len, abort_source) -> input_stream for use by the
// object_handle to fetch data. The resulting stream may be chunked based on
// bypass, see chunking_for.
template<typename OffsetRangeToExtent>
fetch_range_fn chunked_fetch(
  io& io_impl,
  cloud_io::group_id g,
  bypass_cache bypass,
  OffsetRangeToExtent extent_for) {
    auto chunking_policy = chunking_for(bypass);
    fetch_range_fn inner =
      [io = &io_impl, extent_for = std::move(extent_for), g, bypass](
        size_t pos, size_t len, ss::abort_source* as)
      -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
        return io->read_object(
          extent_for(pos, len), as, g, static_cast<bool>(bypass));
    };
    if (chunking_policy.chunk_size == 0) {
        return inner;
    }
    return [inner = std::move(inner), chunking_policy](
             this auto, size_t pos, size_t len, ss::abort_source* as)
             -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
        co_return ss::input_stream<char>{make_chunk_data_source(
          inner,
          pos,
          len,
          chunking_policy.chunk_size,
          as,
          chunking_policy.aligned)};
    };
}

// Open a native L1 object: read and parse the footer the extent locates, then
// return a handle whose byte transport reads data ranges back through
// io::read_object -- note the extents it builds describe those ranges, unlike
// the one passed in here.
ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
open_native_object(
  io& io_impl,
  object_extent footer_extent,
  ss::abort_source* as,
  cloud_io::group_id g,
  bool skip_cache) {
    auto footer_buf = co_await io_impl.fetch_native_footer(
      footer_extent, as, g, skip_cache);
    if (!footer_buf.has_value()) {
        co_return std::unexpected(footer_buf.error());
    }
    auto footer_result = co_await footer::read(std::move(footer_buf).value());
    if (!std::holds_alternative<footer>(footer_result)) {
        vlog(
          cd_log.warn,
          "Failed to parse L1 footer for object {}",
          footer_extent.id);
        co_return std::unexpected(io::errc::cloud_op_error);
    }
    // A data range of a native object is identified by the object id; the
    // footer extent above located the footer, not the data.
    co_return std::make_unique<l1_native_object_handle>(
      std::get<footer>(std::move(footer_result)),
      chunked_fetch(
        io_impl,
        g,
        bypass_cache(skip_cache),
        [oid = footer_extent.id](size_t pos, size_t len) {
            return object_extent{.id = oid, .position = pos, .size = len};
        }));
}

} // namespace

ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>> open_object(
  io& io_impl,
  object_extent extent,
  ss::abort_source* as,
  cloud_io::group_id g,
  bool skip_cache) {
    // Every extent is a native L1 object at this point; a later commit adds
    // open_imported_object for extents describing an imported tiered-storage
    // segment and dispatches on extent.imported here.
    return open_native_object(io_impl, std::move(extent), as, g, skip_cache);
}

} // namespace cloud_topics::l1
