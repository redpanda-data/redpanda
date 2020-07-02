#include "compression/internal/snappy_compressor.h"

#include "bytes/bytes.h"

#include <fmt/core.h>

#include <snappy.h>

namespace compression::internal {

inline iobuf do_compress(const char* src, size_t src_size) {
    size_t max = snappy::MaxCompressedLength(src_size);
    iobuf ret;
    auto ph = ret.reserve(max);
    char* output = ph.mutable_index();
    snappy::RawCompress(src, src_size, output, &max);
    ret.trim_back(ret.size_bytes() - max);
    return ret;
}

iobuf snappy_compressor::compress(const iobuf& b) {
    if (b.empty()) {
        return iobuf{};
    }
    if (std::distance(b.begin(), b.end()) == 1) {
        return do_compress(b.begin()->get(), b.size_bytes());
    }
    // TODO: use snappy::Source interface instead
    auto linearized = iobuf_to_bytes(b);
    return do_compress(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(linearized.data()),
      linearized.size());
}

inline iobuf do_uncompressed(const char* src, size_t src_size) {
    size_t output_size = 0;
    if (!::snappy::GetUncompressedLength(src, src_size, &output_size)) {
        throw std::runtime_error(fmt::format(
          "Could not find uncompressed sizez from input buffer of size: {}",
          src_size));
    }
    iobuf ret;
    auto ph = ret.reserve(output_size);
    char* output = ph.mutable_index();
    ::snappy::RawUncompress(src, src_size, output);
    return ret;
}

iobuf snappy_compressor::uncompress(const iobuf& b) {
    if (b.empty()) {
        return iobuf{};
    }
    if (std::distance(b.begin(), b.end()) == 1) {
        return do_uncompressed(b.begin()->get(), b.size_bytes());
    }
    // linearize buffer
    // TODO: use snappy::Sink interface instead
    auto linearized = iobuf_to_bytes(b);
    return do_uncompressed(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(linearized.data()),
      linearized.size());
}
} // namespace compression::internal
