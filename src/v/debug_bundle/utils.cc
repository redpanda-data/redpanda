/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "debug_bundle/utils.h"

#include "base/seastarx.h"
#include "base/units.h"
#include "crypto/crypto.h"
#include "crypto/types.h"
#include "ssx/future-util.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

namespace debug_bundle {
ss::future<bytes> calculate_sha256_sum(std::string_view path) {
    const size_t buffer_size = 64_KiB;
    const unsigned int read_ahead = 1;
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    auto handle = co_await ss::open_file_dma(path, ss::open_flags::ro);
    auto h = ss::defer(
      [handle]() mutable noexcept { ssx::background = handle.close(); });

    auto stream = ss::make_file_input_stream(
      handle,
      ss::file_input_stream_options{
        .buffer_size = buffer_size, .read_ahead = read_ahead});

    while (true) {
        auto buf = co_await stream.read();
        if (buf.empty()) {
            break;
        }
        ctx.update({buf.get(), buf.size()});
    }

    co_await stream.close();

    co_return std::move(ctx).final();
}
} // namespace debug_bundle
