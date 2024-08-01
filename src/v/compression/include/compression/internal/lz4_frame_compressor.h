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
#include "thirdparty/lz4/lz4frame.h"

namespace compression::internal {

struct lz4_frame_compressor {
    static iobuf compress(const iobuf&);
    static iobuf
    compress_with_block_size(const iobuf&, std::optional<LZ4F_blockSizeID_t>);
    static iobuf uncompress(const iobuf&);
};

} // namespace compression::internal
