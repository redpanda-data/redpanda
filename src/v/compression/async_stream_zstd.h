/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "utils/object_pool.h"

#include <seastar/core/aligned_buffer.hh>

#include <memory>
#include <zstd.h>

namespace compression {
/*
 * A streaming zstd compression class
 *
 * Both compress and uncompress process the iobuf
 * parameter in fixed sized windows and allow for a
 * scheduling point after each window is processed
 */
class async_stream_zstd {
public:
    async_stream_zstd() = delete;
    async_stream_zstd(size_t, int);

    ss::future<iobuf> compress(iobuf);
    ss::future<iobuf> uncompress(iobuf);

    size_t decompression_size() const;

private:
    struct static_cctx {
        static_cctx(int);
        static_cctx(static_cctx&&) noexcept;
        static_cctx& operator=(static_cctx&& x) noexcept;

        ZSTD_CCtx* _compress_ctx;
        std::unique_ptr<char[], ss::free_deleter> _c_workspace;
        ss::temporary_buffer<char> _c_buffer;
    };

    struct static_dctx {
        static_dctx(size_t);
        static_dctx(static_dctx&&) noexcept;
        static_dctx& operator=(static_dctx&& x) noexcept;

        ZSTD_DCtx* _decompress_ctx;
        std::unique_ptr<char[], ss::free_deleter> _d_workspace;
        ss::temporary_buffer<char> _d_buffer;
    };

    size_t _decompression_size;

    object_pool<static_cctx> _compression_ctx_pool;
    object_pool<static_dctx> _decompression_ctx_pool;
};

void initialize_async_stream_zstd(size_t);
async_stream_zstd& async_stream_zstd_instance();

} // namespace compression
