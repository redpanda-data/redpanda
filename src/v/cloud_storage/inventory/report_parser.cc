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

#include "cloud_storage/inventory/report_parser.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"

#include <seastar/util/short_streams.hh>

#include <zlib.h>

namespace {
constexpr auto newline{'\n'};

bool has_newline(const iobuf& b) {
    iobuf::byte_iterator begin{b.cbegin(), b.cend()};
    iobuf::byte_iterator end{b.cend(), b.cend()};
    while (begin != end) {
        if (*begin == newline) {
            return true;
        }
        ++begin;
    }
    return false;
}

} // namespace

namespace cloud_storage::inventory {

report_parser::report_parser(
  ss::input_stream<char> stream,
  is_gzip_compressed is_compressed,
  size_t chunk_size)
  : _stream{std::move(stream)}
  , _is_compressed{is_compressed}
  , _chunk_size{chunk_size} {
    if (_is_compressed) {
        _stream_inflate.emplace(std::move(_stream), _chunk_size);
    }
}

ss::future<report_parser::rows_t> report_parser::next() {
    auto h = _gate.hold();
    while (!has_newline(_buffer)) {
        auto fill_res = co_await fill_buffer();
        if (!fill_res.has_value()) {
            _eof = true;
            break;
        }
        _buffer.append(std::move(fill_res.value()));
    }

    if (_buffer.empty()) {
        co_return rows_t{};
    }

    if (eof() && !has_newline(_buffer)) {
        iobuf_parser p{std::move(_buffer)};
        co_return rows_t{p.read_string(p.bytes_left())};
    }

    vassert(
      has_newline(_buffer), "Buffer {} does not contain a newline", _buffer);
    co_return split_to_rows();
}

ss::future<> report_parser::stop() {
    co_await _gate.close();
    if (_decompression_started) {
        co_await _stream_inflate->stop();
    }
}

report_parser::rows_t report_parser::split_to_rows() {
    using ibi = iobuf::byte_iterator;
    rows_t rows;
    fragmented_vector<char> row;
    for (auto begin = ibi{_buffer.cbegin(), _buffer.cend()},
              end = ibi{_buffer.cend(), _buffer.cend()};
         begin != end;
         ++begin) {
        if (*begin == newline) {
            rows.emplace_back(row.begin(), row.end());
            row.clear();
        } else {
            row.push_back(*begin);
        }
    }
    _buffer.trim_front(_buffer.size_bytes() - row.size());
    return rows;
}

ss::future<std::optional<iobuf>> report_parser::fill_buffer() {
    if (_is_compressed) {
        vassert(
          _stream_inflate.has_value(),
          "decompression context not initialized, cannot decompress report");
        if (unlikely(!_decompression_started)) {
            _decompression_started = true;
            _stream_inflate->reset();
        }
        co_return co_await _stream_inflate->next();
    }

    auto buf = co_await _stream.read_up_to(_chunk_size);
    if (buf.empty()) {
        co_return std::nullopt;
    }
    iobuf b;
    b.append(std::move(buf));
    co_return b;
}

} // namespace cloud_storage::inventory
