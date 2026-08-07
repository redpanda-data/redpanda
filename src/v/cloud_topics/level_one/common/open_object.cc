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

#include "cloud_storage/offset_index.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_topics/level_one/common/chunk_data_source.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/ts_object.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"

#include <seastar/util/bool_class.hh>

namespace cloud_topics::l1 {

namespace {

/// Whether the object being read is an imported tiered-storage segment or a
/// native L1 object.
using is_imported = ss::bool_class<struct is_imported_tag>;

/// Whether reads bypass the cloud cache, as io::read_object's skip_cache does.
using bypass_cache = ss::bool_class<struct bypass_cache_tag>;

// Chunking for a read, given the object format and whether the cache is
// bypassed. Applied by wrapping the per-range read_object fetch in a
// chunk_data_source; chunk_size == 0 means "don't chunk" -- pass the whole
// requested range straight through.
struct read_chunking {
    size_t chunk_size;
    use_chunk_aligned_reads aligned;
};

read_chunking chunking_for(is_imported imported, bypass_cache bypass) {
    const auto& cfg = config::shard_local_cfg();
    if (bypass) {
        // Cache-bypassing bulk/one-shot read (leveling, compaction): bound peak
        // memory with the streaming chunk size. Nothing is cached, so there is
        // no (pos,size) key to align to -- chunks start at the read's position.
        return {
          cfg.cloud_topics_l1_streaming_read_chunk_size(),
          use_chunk_aligned_reads::no};
    }
    if (imported) {
        // Cached imported read: chunk at the cloud-storage cache-chunk
        // granularity, chunk-aligned so overlapping reads request identical
        // (pos,size) chunks that the cache dedups. Disabling chunk reads falls
        // back to a single whole-suffix cache entry.
        if (cfg.cloud_storage_disable_chunk_reads()) {
            return {0, use_chunk_aligned_reads::yes};
        }
        return {
          cfg.cloud_storage_cache_chunk_size(), use_chunk_aligned_reads::yes};
    }
    // Cached native read: read_object stores the whole requested extent in the
    // cache and serves it from a file, so the cache file bounds memory and no
    // stream-level chunking is needed.
    return {0, use_chunk_aligned_reads::yes};
}

// Build a function (pos, len, abort_source) -> input_stream for use by the
// object_handle to fetch data. The resulting stream may be chunked based on
// (imported, bypass), see chunking_for.
template<typename OffsetRangeToExtent>
fetch_range_fn chunked_fetch(
  io& io_impl,
  cloud_io::group_id g,
  is_imported imported,
  bypass_cache bypass,
  OffsetRangeToExtent extent_for) {
    auto chunking_policy = chunking_for(imported, bypass);
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

// Open an imported tiered-storage segment: build its index from the .index
// sidecar (io::fetch_ts_index), its aborted-transaction set from the .tx
// sidecar (io::fetch_ts_tx, gated by the import-time tx_state), and return a
// ts_object_handle whose byte transport reads chunks back through
// io::read_object (routed to the segment's ts_path).
ss::future<std::expected<std::unique_ptr<object_handle>, io::errc>>
open_imported_object(
  io& io_impl,
  object_extent segment_extent,
  ss::abort_source* as,
  cloud_io::group_id g,
  bool skip_cache) {
    const auto& imported = *segment_extent.imported;

    // Seek through the segment's real offset index: a present .index is
    // deserialized; a missing one (notfound) leaves the index empty, so seeks
    // fall back to a full-segment scan from position 0 with the segment base
    // delta -- matching native tiered storage. Any other error propagates
    // rather than being masked as an empty-index full scan.
    cloud_storage::offset_index oi(
      model::offset{0},
      kafka::offset{0},
      0,
      cloud_storage::remote_segment_sampling_step_bytes,
      model::timestamp::missing());
    auto index_bytes = co_await io_impl.fetch_ts_index(segment_extent, as);
    if (index_bytes.has_value()) {
        oi.from_iobuf(std::move(*index_bytes));
    } else if (index_bytes.error() != io::errc::cloud_missing_object) {
        vlog(
          cd_log.warn,
          "Failed to fetch index for imported segment {}: {}",
          imported.ts_path,
          index_bytes.error());
        co_return std::unexpected(index_bytes.error());
    }
    auto idx = std::make_unique<ts_segment_index>(
      std::move(oi), imported.delta_base, segment_extent.size);

    // Aborted-transaction ranges for this segment, so the reader can strip
    // aborted data and make the imported region committed-only (like native CT
    // L1). Ranges are in raw log-offset space, as the reader needs. Whether to
    // consult the .tx sidecar is decided by the import-time tx_state, which
    // mirrors what native tiered storage knows without a probe: only v1/v2
    // non-compacted segments require one.
    aborted_transactions aborted;
    if (imported.tx_state != tx_manifest_state::absent) {
        // present or unknown: consult the .tx sidecar.
        auto tx = co_await io_impl.fetch_ts_tx(segment_extent, as);
        if (tx.has_value()) {
            for (auto& r : *tx) {
                aborted.insert(r);
            }
        } else if (tx.error() == io::errc::cloud_missing_object) {
            // A missing .tx means no aborted transactions. Tolerate it and read
            // the segment as committed-only, matching native tiered storage,
            // which treats a notfound .tx as empty. When tx_state == present
            // the source metadata recorded a .tx, so its absence is unexpected
            // (a lost or partial upload) and we warn -- but as a compatibility
            // layer we degrade gracefully rather than turning behavior tiered
            // storage silently tolerated into a hard read failure.
            if (imported.tx_state == tx_manifest_state::present) {
                vlog(
                  cd_log.warn,
                  "Imported segment {} was expected to have a .tx manifest "
                  "(its "
                  "metadata recorded one) but it is missing; reading the "
                  "segment "
                  "as committed-only",
                  imported.ts_path);
            }
        } else {
            // A transient/other error (not a definitive notfound): propagate so
            // the read is retried, rather than silently dropping
            // aborted-transaction filtering.
            vlog(
              cd_log.warn,
              "Failed to fetch tx ranges for imported segment {}: {}",
              imported.ts_path,
              tx.error());
            co_return std::unexpected(tx.error());
        }
    }
    // tx_state == absent: known to have no aborted transactions (compacted, or
    // v3 with an empty .tx manifest), so skip the sidecar entirely -- `aborted`
    // stays empty.

    // A data range of an imported segment is identified by the segment
    // descriptor, which routes read_object to its ts_path (with cache/skip
    // handling, same as any other L1 read).
    co_return std::make_unique<ts_object_handle>(
      std::move(idx),
      imported.segment_term,
      std::move(aborted),
      chunked_fetch(
        io_impl,
        g,
        is_imported::yes,
        bypass_cache(skip_cache),
        [imported](size_t pos, size_t len) {
            return object_extent{
              .position = pos, .size = len, .imported = imported};
        }));
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
        is_imported::no,
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
    if (extent.imported.has_value()) {
        return open_imported_object(
          io_impl, std::move(extent), as, g, skip_cache);
    }
    return open_native_object(io_impl, std::move(extent), as, g, skip_cache);
}

} // namespace cloud_topics::l1
