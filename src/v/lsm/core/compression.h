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

#pragma once

#include "bytes/ioarray.h"
#include "bytes/iobuf.h"

#include <seastar/core/future.hh>

#include <cstdint>

namespace lsm {

// The type of compression
//
// NOTE: these values are serialized so take care to change them
enum class compression_type : uint8_t {
    none = 0,
    zstd = 1,
};

// Convert a raw byte to a compression type (or throw)
compression_type compression_type_from_raw(uint8_t);

// Compress the iobuf and return the compressed iobuf.
//
// REQUIRES: compression_type is not `none`.
ss::future<iobuf> compress(iobuf, compression_type);

// Uncompress the iobuf and return the uncompressed iobuf.
//
// REQUIRES: compression_type is not `none`.
ss::future<ioarray> uncompress(ioarray, compression_type);

} // namespace lsm
