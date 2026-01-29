/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/core/compression.h"

#include "compression/async_stream_zstd.h"
#include "compression/compression.h"
#include "lsm/core/exceptions.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <utility>

namespace lsm {

namespace {

compression::type convert_type(compression_type type) {
    switch (type) {
    case compression_type::zstd:
        return compression::type::zstd;
    case compression_type::none:
        throw invalid_argument_exception(
          "attempted to compress with type none");
    }
}

} // namespace

ss::future<iobuf> compress(iobuf buf, compression_type type) {
    return compression::stream_compressor::compress(
      std::move(buf), convert_type(type));
}

ss::future<ioarray> uncompress(ioarray array, compression_type type) {
    switch (type) {
    case compression_type::none:
        throw invalid_argument_exception(
          "attempted to compress with type none");
    case compression_type::zstd: {
        try {
            co_return co_await compression::async_stream_zstd_instance()
              .uncompress(std::move(array));
        } catch (const std::exception& ex) {
            throw invalid_argument_exception(
              "failed to uncompress: {}", ex.what());
        }
    }
    }
}

compression_type compression_type_from_raw(uint8_t v) {
    auto ct = static_cast<compression_type>(v);
    switch (ct) {
    case compression_type::none:
    case compression_type::zstd:
        return ct;
    }
    throw corruption_exception("unknown compression type: {}", v);
}

} // namespace lsm
