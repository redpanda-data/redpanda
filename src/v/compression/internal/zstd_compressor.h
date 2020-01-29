#pragma once
#include "bytes/iobuf.h"
namespace compression::internal {

struct zstd {
    static iobuf compress(const iobuf&) {
        throw std::runtime_error("not implemented");
    }
    static iobuf uncompress(const iobuf&) {
        throw std::runtime_error("not implemented");
    }
};

} // namespace compression::internal
