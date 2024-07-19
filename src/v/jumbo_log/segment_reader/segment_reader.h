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
#include "bytes/iobuf_parser.h"
#include "jumbo_log/segment/sparse_index.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"
#include "model/record.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>

#include <stdint.h>
#include <utility>

namespace jumbo_log::segment_reader {

class chunk_reader_from_iobuf {
public:
    explicit chunk_reader_from_iobuf(
      iobuf buf,
      model::ntp ntp,
      model::bounded_offset_interval offset_interval);

    ss::future<ss::circular_buffer<model::record_batch>> read_some();

    bool end_of_stream() const { return _parser.bytes_left() == 0; }

private:
    iobuf_parser _parser;
    model::ntp _ntp;
    model::bounded_offset_interval _offset_interval;

    // State.
    bool _reached_end = false;
    bool _ntp_seek_done = false;
    bool _first_ntp_check_done = false;
    bool _offset_seek_done = false;

    size_t _batches_bytes_left = 0;
};

class sparse_index_reader_from_iobuf {
private:
    explicit sparse_index_reader_from_iobuf();

public:
    static ss::future<std::pair<jumbo_log::segment::sparse_index, int64_t>>
    read(iobuf buf);
};

} // namespace jumbo_log::segment_reader
