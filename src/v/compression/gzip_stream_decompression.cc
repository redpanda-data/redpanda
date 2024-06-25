/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "compression/gzip_stream_decompression.h"

namespace compression {

gzip_stream_decompressor::gzip_stream_decompressor(
  ss::input_stream<char> stream, size_t chunk_size)
  : _stream{std::move(stream)}
  , _chunk_size{chunk_size} {}

void gzip_stream_decompressor::reset() {
    vassert(!_decompression_started, "decompressor initialized twice");
    constexpr auto default_windowbits = 15;
    constexpr auto decode_with_header_detection = 32;
    if (auto init_res = inflateInit2(
          &_zs, default_windowbits + decode_with_header_detection);
        init_res != Z_OK) {
        throw stream_decompression_error{fmt::format(
          "Failed to initialize decompression context: {}", zError(init_res))};
    }
    _decompression_started = true;
}

ss::future<> gzip_stream_decompressor::stop() { co_await _gate.close(); }

gzip_stream_decompressor::~gzip_stream_decompressor() {
    vassert(
      _gate.is_closed(), "gzip_stream_decompressor destroyed without stopping");
    if (_decompression_started) {
        inflateEnd(&_zs);
    }
}

ss::future<> gzip_stream_decompressor::read_from_stream() {
    auto h = _gate.hold();
    vassert(
      _zs.avail_in == 0,
      "attempt to read from stream when there are {} unprocessed input bytes)",
      _zs.avail_in);
    _buffer = co_await _stream.read_up_to(_chunk_size);
    if (_buffer.empty()) {
        _eof = true;
        co_return;
    }

    _zs.next_in = const_cast<unsigned char*>(
      reinterpret_cast<const unsigned char*>(_buffer.get()));
    _zs.avail_in = _buffer.size();
}

ss::future<std::optional<iobuf>> gzip_stream_decompressor::next() {
    auto h = _gate.hold();
    vassert(
      _decompression_started,
      "Attempt to decompress without initializing stream");

    if (_zs.avail_in == 0) {
        co_await read_from_stream();
    }

    if (eof()) {
        co_return std::nullopt;
    }

    iobuf result;
    // TODO this construct could potentially _not_ be a while loop. In one
    // iteration we end up with at most _chunk_size bytes of output. If we reach
    // that limit, we bail out of this while loop and we return the iobuf. If we
    // end up short of _chunk_size, then _zs.avail_in should have reached 0 ie
    // we fully consumed the input. In either case there would probably be only
    // one iteration of this loop per call to next().
    while (_zs.avail_in > 0 && result.size_bytes() <= _chunk_size) {
        // At most one _chunk_size worth of data will be returned in this call.
        ss::temporary_buffer<char> decompressed{_chunk_size};

        _zs.next_out = reinterpret_cast<unsigned char*>(
          decompressed.get_write());
        _zs.avail_out = decompressed.size();

        const auto available = _zs.avail_in;
        const auto out = _zs.avail_out;

        if (auto code = inflate(&_zs, Z_NO_FLUSH);
            code != Z_OK && code != Z_STREAM_END) {
            throw stream_decompression_error(
              fmt::format("Failed to decompress chunk: {}", zError(code)));
        }

        // It is possible for the avail_in field not to change, if the
        // decompressed data does not fit into the output buffer fully. Several
        // loop iterations may be required to consume avail_in. However if both
        // avail_in and avail_out do not change, then we are stuck in a loop, as
        // no progress was made by the inflate call.
        if (_zs.avail_in == available && _zs.avail_out == out) {
            throw stream_decompression_error{fmt::format(
              "decompression stuck in loop: bytes left in chunk: {}, bytes "
              "available in output buffer: {}",
              _zs.avail_in,
              _zs.avail_out)};
        }

        auto bytes_read = decompressed.size() - _zs.avail_out;
        decompressed.trim(bytes_read);
        if (!decompressed.empty()) {
            result.append(std::move(decompressed));
        }
    }
    co_return result;
}

} // namespace compression
