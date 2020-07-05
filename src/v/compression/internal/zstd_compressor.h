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
