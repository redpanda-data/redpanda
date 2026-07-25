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

namespace compression::internal {
struct snappy_java_compressor {
    struct snappy_magic {
        static const constexpr std::array<uint8_t, 8> java_magic = {
          0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0};
        // Previously, these version fields were erroneously written with
        // little-endian encoding. They are now corrected to be written and
        // decoded using big-endian, but we must retain backwards compatibility
        // with the existing, improperly encoded batches.
        static const constexpr int32_t default_version = 1;
        static const constexpr int32_t min_compatible_version = 1;
        static const constexpr size_t header_len = java_magic.size()
                                                   + sizeof(default_version)
                                                   + sizeof(
                                                     min_compatible_version);
    };

    static iobuf compress(const iobuf&);
    static iobuf uncompress(const iobuf&);

    /// Preallocate this shard's compression workspace (~278KiB). Called
    /// during broker startup, before the large-allocation warning threshold
    /// is armed. Otherwise the workspace is created lazily on first use.
    static void init_workspace();
};

} // namespace compression::internal
