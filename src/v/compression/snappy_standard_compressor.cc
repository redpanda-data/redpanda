// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/snappy_standard_compressor.h"

#include "bytes/bytes.h"
#include "likely.h"
#include "units.h"

#include <fmt/format.h>

#include <snappy-sinksource.h>
#include <snappy.h>

namespace compression {

class iobuf_source final : public snappy::Source {
    static constexpr auto max_region_size = 128_KiB;

public:
    explicit iobuf_source(const iobuf& buf)
      : _buf(buf)
      , _in(_buf.cbegin(), _buf.cend())
      , _available(_buf.size_bytes()) {}

    size_t Available() const override { return _available; }

    const char* Peek(size_t* len) override {
        const char* region = nullptr;
        size_t region_size = 0;

        // peek at the next contiguous region
        auto in = _in;
        (void)in.consume(
          max_region_size,
          [&region, &region_size](const char* ptr, size_t size) {
              region = ptr;
              region_size = size;
              return ss::stop_iteration::yes;
          });

        vassert(
          ((region && region_size && _available)
           || (!region && !region_size && !_available)),
          "Inconsistent snappy source: region_null {} region_size {} "
          "_available {}",
          region == nullptr,
          region_size,
          _available);

        *len = region_size;
        return region;
    }

    void Skip(size_t n) override {
        vassert(
          _available >= n,
          "Cannot skip snappy source: available {} < n {}",
          _available,
          n);
        _in.skip(n);
        _available -= n;
    }

private:
    const iobuf& _buf;
    iobuf::iterator_consumer _in;
    size_t _available;
};

class iobuf_sink final : public snappy::Sink {
public:
    explicit iobuf_sink(iobuf& buf)
      : _buf(buf) {}

    void Append(const char* bytes, size_t n) override { _buf.append(bytes, n); }

private:
    iobuf& _buf;
};

iobuf snappy_standard_compressor::compress(const iobuf& b) {
    iobuf ret;
    iobuf_sink sink(ret);
    iobuf_source source(b);
    auto n = ::snappy::Compress(&source, &sink);
    vassert(
      ret.size_bytes() == n,
      "Snappy compression expected {} bytes written found {}",
      n,
      ret.size_bytes());
    return ret;
}

iobuf do_uncompressed(const char* src, size_t src_size) {
    size_t output_size = 0;
    if (unlikely(
          !::snappy::GetUncompressedLength(src, src_size, &output_size))) {
        throw std::runtime_error(fmt::format(
          "Could not find uncompressed size from input buffer of size: {}",
          src_size));
    }
    iobuf ret;
    if (output_size == 0) {
        // empty frame
        return ret;
    }
    auto ph = ret.reserve(output_size);
    char* output = ph.mutable_index();
    if (!::snappy::RawUncompress(src, src_size, output)) {
        throw std::runtime_error(fmt::format(
          "snappy: Could not decompress input size: {}, to output size:{}",
          src_size,
          output_size));
    }
    return ret;
}

iobuf snappy_standard_compressor::uncompress(const iobuf& b) {
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
} // namespace compression
