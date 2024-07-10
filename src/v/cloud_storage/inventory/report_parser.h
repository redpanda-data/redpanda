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

#pragma once

#include "base/seastarx.h"
#include "bytes/details/io_allocation_size.h"
#include "compression/gzip_stream_decompression.h"
#include "container/fragmented_vector.h"

#include <seastar/core/iostream.hh>

#include <zlib.h>

namespace cloud_storage::inventory {

using is_gzip_compressed = ss::bool_class<struct gzip_compressed_tag>;

template<typename C>
concept RowsConsumer = requires(C c, fragmented_vector<ss::sstring> rows) {
    { c(rows) } -> std::same_as<ss::future<>>;
};

class report_decompression_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

/// Parses newline delimited inventory report, supplied as an input stream.
/// The report may optionally be GZip compressed. The rows returned by the
/// parser have newlines stripped off, but no other modifications are
/// performed. In particular, whitespaces and carriage returns are still
/// present in the returned row. For CSV files, parsing of the row is not
/// performed and must be done by the caller.
class report_parser {
public:
    using row_t = iobuf;
    using rows_t = fragmented_vector<ss::sstring>;

    report_parser(const report_parser&) = delete;
    report_parser& operator=(const report_parser&) = delete;

    report_parser(report_parser&&) = delete;
    report_parser& operator=(report_parser&&) = delete;

    // The chunk size supplied here controls the following behavior:
    // 1. The size of data read from the input stream per read_up_to
    // invocation
    // 2. The max buffer size allocated during chunk decompression if the
    // report is compressed
    explicit report_parser(
      ss::input_stream<char> stream,
      is_gzip_compressed is_compressed = is_gzip_compressed::no,
      size_t chunk_size = details::io_allocation_size::max_chunk_size);

    // Returns the next line from the input stream, or nullopt if the stream
    // is fully consumed.
    ss::future<rows_t> next();
    bool eof() const { return _eof; }

    // High level API for consuming a report. The consumer is supplied with
    // batches of rows in the stream until EOF. The consumer must return a
    // future
    template<RowsConsumer Consumer>
    ss::future<> consume(Consumer c) {
        auto h = _gate.hold();
        for (auto rows = co_await next(); !rows.empty();
             rows = co_await next()) {
            co_await c(std::move(rows));
        }
    }

    ss::future<> stop();

private:
    ss::future<std::optional<iobuf>> fill_buffer();

    // Splits the buffer into rows on newline characters, upto the last newline.
    // The rows are removed from _buffer and returned. The remaining part of the
    // buffer after the last newline is preserved.
    rows_t split_to_rows();

    ss::input_stream<char> _stream;
    const is_gzip_compressed _is_compressed;

    row_t _buffer;
    bool _eof{false};

    bool _decompression_started{false};
    size_t _chunk_size;
    std::optional<compression::gzip_stream_decompressor> _stream_inflate;
    ss::gate _gate;
};
} // namespace cloud_storage::inventory
