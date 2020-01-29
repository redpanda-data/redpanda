#pragma once
#include "bytes/iobuf.h"
namespace compression {

enum class type { none, gzip, snappy, lz4, zstd };
// a very simple compressor. Exposes virtually no knobs and uses
// the defaults for all compressors. In the future, we can make these
// a virtual interface so we can instantiate them
struct compressor {
    static iobuf compress(const iobuf&, type);
    static iobuf uncompress(const iobuf&, type);
};

} // namespace compression
