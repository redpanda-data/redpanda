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
#include "utils/static_deleter_fn.h"

#include <memory>
#include <zstd.h>

namespace compression {
class stream_zstd {
public:
    using zstd_compress_ctx = std::unique_ptr<
      ZSTD_CCtx,
      // wrap ZSTD C API
      static_sized_deleter_fn<ZSTD_CCtx, &ZSTD_freeCCtx>>;

    iobuf compress(const iobuf& b) { return do_compress(b); }
    iobuf uncompress(const iobuf& b) { return do_uncompress(b); }
    iobuf compress(iobuf&& b) { return do_compress(b); }
    iobuf uncompress(iobuf&& b) { return do_uncompress(b); }

    static void init_workspace(size_t);

private:
    iobuf do_compress(const iobuf&);
    iobuf do_uncompress(const iobuf&);

    void reset_compressor();
    zstd_compress_ctx& compressor();
    ZSTD_DCtx* decompressor();

    zstd_compress_ctx _compress{nullptr};
};

} // namespace compression
