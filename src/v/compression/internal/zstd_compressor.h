/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "compression/stream_zstd.h"
namespace compression::internal {

struct zstd_compressor {
    static iobuf compress(const iobuf& b) {
        stream_zstd fn;
        return fn.compress(b);
    }
    static iobuf uncompress(const iobuf& b) {
        stream_zstd fn;
        return fn.uncompress(b);
    }
};

} // namespace compression::internal
