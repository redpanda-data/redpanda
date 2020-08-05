#pragma once

#include "bytes/iobuf.h"

namespace compression {

struct snappy_standard_compressor {
    static iobuf compress(const iobuf&);
    static iobuf uncompress(const iobuf&);
};

} // namespace compression
