// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/segment_reader.h"

#include "base/vlog.h"
#include "io/pager.h"
#include "io/paging_data_source.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/skipping_data_source.h"

namespace storage::experimental::mvlog {
segment_reader::segment_reader(readable_segment* segment)
  : segment_(segment) {
    ++segment_->num_readers_;
}
segment_reader::~segment_reader() { --segment_->num_readers_; }

skipping_data_source::read_list_t
segment_reader::make_read_intervals(size_t start_pos, size_t length) const {
    // TODO(awong): a future commit will build appropriate intervals for this
    // reader's truncation id.
    skipping_data_source::read_list_t read_intervals;
    read_intervals.emplace_back(start_pos, length);
    return read_intervals;
}

ss::input_stream<char> segment_reader::make_stream(size_t start_pos) const {
    return make_stream(start_pos, segment_->pager_->size() - start_pos);
}

ss::input_stream<char>
segment_reader::make_stream(size_t start_pos, size_t length) const {
    auto read_intervals = make_read_intervals(start_pos, length);
    for (const auto& interval : read_intervals) {
        vlog(
          log.trace,
          "Reading interval [{}, {})",
          interval.offset,
          interval.offset + interval.length);
    }
    return ss::input_stream<char>(
      ss::data_source(std::make_unique<skipping_data_source>(
        segment_->pager_, std::move(read_intervals))));
}

} // namespace storage::experimental::mvlog
