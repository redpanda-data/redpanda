#include "compression/compression.h"

#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_compressor.h"
#include "compression/internal/snappy_compressor.h"
#include "compression/internal/zstd_compressor.h"

namespace compression {
iobuf compressor::compress(const iobuf& io, type t) {
    switch (t) {
    case type::none:
        throw std::runtime_error("compressor: nothing to compress for 'none'");
    case type::gzip:
        return internal::gzip::compress(io);
    case type::snappy:
        return internal::snappy::compress(io);
    case type::lz4:
        return internal::lz4::compress(io);
    case type::zstd:
        return internal::zstd::compress(io);
    }
    __builtin_unreachable();
}
iobuf compressor::uncompress(const iobuf& io, type t) {
    switch (t) {
    case type::none:
        throw std::runtime_error(
          "compressor: nothing to uncompress for 'none'");
    case type::gzip:
        return internal::gzip::uncompress(io);
    case type::snappy:
        return internal::snappy::uncompress(io);
    case type::lz4:
        return internal::lz4::uncompress(io);
    case type::zstd:
        return internal::zstd::uncompress(io);
    }
    __builtin_unreachable();
}

} // namespace compression
