// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "jumbo_log/segment/internal.h"
#include "jumbo_log/segment/segment.h"
#include "jumbo_log/segment/sparse_index.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <stdint.h>
#include <utility>

namespace jumbo_log::segment_writer {
inline constexpr auto CHUNK_SIZE_TARGET = 4 * 1024 * 1024;

struct options {
    jumbo_log::segment::chunk_size_t chunk_size_target = CHUNK_SIZE_TARGET;
};

class sparse_index_accumulator {
    friend class jumbo_log::segment::sparse_index;
    friend class sparse_index_writer;

public:
    void add_entry(
      model::ntp ntp,
      model::offset offset,
      size_t offset_bytes,
      size_t size_bytes);

    void add_upper_limit(model::ntp ntp, model::offset offset);

    segment::sparse_index to_index() && {
        return segment::sparse_index(std::move(_data));
    }

private:
    jumbo_log::segment::internal::sparse_index_data _data;
};

class offset_tracking_output_stream {
public:
    explicit offset_tracking_output_stream(ss::output_stream<char>* out)
      : _out(out) {}

public:
    ss::future<> write(const char* data, size_t size) {
        _offset += size;
        return _out->write(data, size);
    }

    ss::future<> write(iobuf&& buf);

    size_t offset() const { return _offset; }

private:
    ss::output_stream<char>* _out;
    size_t _offset = 0;
};

class sparse_index_writer {
public:
    ss::future<int64_t> operator()(
      offset_tracking_output_stream* out, const segment::sparse_index* index);
};

class iobuf_chunk_writer {
public:
    void write(model::ntp ntp, model::record_batch&& batch);
    iobuf advance();
    size_t size_bytes() const;

    std::pair<model::ntp, model::offset> first() const { return _first; }
    std::pair<model::ntp, model::offset> last() const { return _last; }

private:
    void maybe_flush_batch_buffer();

private:
    iobuf _buf;

    // We collect batches separately from the chunk buffer to be able to write
    // as we need their size to know when to stop reading or how much to skip.
    iobuf _batch_buf;

    std::pair<model::ntp, model::offset> _first;
    std::pair<model::ntp, model::offset> _last;
};

/// This writer expects data to be already ordered by ntp and offset.
/// It will throw an exception if the data is not ordered.
class writer {
public:
    explicit writer(ss::output_stream<char>* out, options opts = {})
      : _out(out)
      , _opts(opts) {}

public:
    ss::future<> write(model::ntp ntp, model::record_batch&& batch);

    /// Close the writer. This will write the footer but leave the
    /// underlying output stream open.
    ss::future<> close();

private:
    ss::future<> write_header();
    ss::future<> maybe_flush_chunk_writer();
    ss::future<> write_footer();
    ss::future<> write_footer_epilogue();

private:
    bool _did_write_header = false;
    bool _closed = false;

    offset_tracking_output_stream _out;
    options _opts;

    int64_t _sparse_index_size = 0;

    iobuf_chunk_writer _chunk_writer;
    sparse_index_accumulator _index_builder;
};

} // namespace jumbo_log::segment_writer
