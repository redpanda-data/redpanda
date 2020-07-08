#pragma once
#include "bytes/iobuf.h"
namespace compression::internal {

struct snappy_compressor {
    static iobuf compress(const iobuf&);
    static iobuf uncompress(const iobuf&);
};

} // namespace compression::internal
