// Copyright 2020 Redpanda Data, Inc.
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
#include "vassert.h"

#include <fmt/format.h>

#include <snappy-sinksource.h>
#include <snappy.h>

namespace compression {

class snappy_iobuf_source final : public snappy::Source {
    static constexpr auto max_region_size = 128_KiB;

public:
    explicit snappy_iobuf_source(const iobuf& buf)
      : _buf(buf)
      , _in(_buf.cbegin(), _buf.cend())
      , _available(_buf.size_bytes()) {}

    size_t Available() const final { return _available; }

    const char* Peek(size_t* len) final {
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

    void Skip(size_t n) final {
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

class snappy_iobuf_sink final : public snappy::Sink {
public:
    explicit snappy_iobuf_sink(iobuf& buf)
      : _buf(buf) {}

    void Append(const char* bytes, size_t n) final { _buf.append(bytes, n); }

private:
    iobuf& _buf;
};

iobuf snappy_standard_compressor::compress(const iobuf& b) {
    iobuf ret;
    snappy_iobuf_sink sink(ret);
    snappy_iobuf_source source(b);
    auto n = ::snappy::Compress(&source, &sink);
    vassert(
      ret.size_bytes() == n,
      "Snappy compression expected {} bytes written found {}",
      n,
      ret.size_bytes());
    return ret;
}

iobuf snappy_standard_compressor::uncompress(const iobuf& b) {
    iobuf ret;
    auto output_size = get_uncompressed_length(b);
    if (output_size > 0) {
        uncompress_append(b, ret, output_size);
    }
    return ret;
}

size_t snappy_standard_compressor::get_uncompressed_length(const iobuf& b) {
    snappy_iobuf_source src(b);
    uint32_t output_size = 0;
    if (unlikely(!::snappy::GetUncompressedLength(&src, &output_size))) {
        throw std::runtime_error(fmt::format(
          "Could not find uncompressed size from input buffer of size: {}",
          b.size_bytes()));
    }
    return output_size;
}

void snappy_standard_compressor::uncompress_append(
  const iobuf& input, iobuf& output, size_t output_size) {
    /*
     * allocate buffers to decompress into
     */
    size_t remaining = output_size;
    std::vector<ss::temporary_buffer<char>> bufs;
    while (remaining) {
        auto size = details::io_allocation_size::next_allocation_size(
          remaining);
        size = std::min(remaining, size);
        bufs.emplace_back(size);
        remaining -= size;
    }

    /*
     * setup vectors for iovec decompression interface
     */
    std::vector<iovec> iovecs;
    for (auto& buf : bufs) {
        iovec vec{
          .iov_base = buf.get_write(),
          .iov_len = buf.size(),
        };
        iovecs.push_back(vec);
    }

    snappy_iobuf_source src(input);
    if (!::snappy::RawUncompressToIOVec(&src, iovecs.data(), iovecs.size())) {
        throw std::runtime_error(fmt::format(
          "snappy: Could not decompress input size: {}, to output size:{}",
          input.size_bytes(),
          output_size));
    }

    for (auto& buf : bufs) {
        output.append(std::move(buf));
    }
}

} // namespace compression
