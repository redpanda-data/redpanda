#pragma once
#include "bytes/iobuf.h"
namespace compression::internal {

struct lz4_frame_compressor {
    static iobuf compress(const iobuf&);
    static iobuf uncompress(const iobuf&);
};

} // namespace compression::internal
