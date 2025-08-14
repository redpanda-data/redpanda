// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/batch_compression.h"

#include "bytes/iobuf.h"
#include "compression/compression.h"

#include <seastar/core/coroutine.hh>

namespace model {

ss::future<model::record_batch> decompress_batch(model::record_batch&& b) {
    return ss::futurize_invoke(decompress_batch_sync, std::move(b));
}

ss::future<model::record_batch> decompress_batch(const model::record_batch& b) {
    return ss::futurize_invoke(maybe_decompress_batch_sync, b);
}

model::record_batch decompress_batch_sync(model::record_batch&& b) {
    if (!b.compressed()) {
        return std::move(b);
    }

    return maybe_decompress_batch_sync(b);
}

model::record_batch maybe_decompress_batch_sync(const model::record_batch& b) {
    if (unlikely(!b.compressed())) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to decompressed a non-compressed batch:{}",
          b.header()));
    }
    iobuf body_buf = ::compression::compressor::uncompress(
      b.data(), b.header().attrs.compression());
    // must remove compression first!
    auto h = b.header();
    h.attrs.remove_compression();
    h.reset_size_checksum_metadata(body_buf);
    auto batch = model::record_batch(
      h, std::move(body_buf), model::record_batch::tag_ctor_ng{});
    return batch;
}

ss::future<model::record_batch>
compress_batch(model::compression c, model::record_batch b) {
    if (c == model::compression::none) {
        vassert(
          b.header().attrs.compression() == model::compression::none,
          "Asked to compress a batch with `none` compression, but header "
          "metadata is incorrect: {}",
          b.header());
        co_return b;
    }
    auto h = b.header();
    auto payload = co_await ::compression::stream_compressor::compress(
      std::move(b).release_data(), c);
    // compression bit must be set first!
    h.attrs |= c;
    h.reset_size_checksum_metadata(payload);
    auto batch = model::record_batch(
      h, std::move(payload), model::record_batch::tag_ctor_ng{});
    co_return batch;
}

} // namespace model
