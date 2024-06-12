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
#include "storage/mvlog/file.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/skipping_data_source.h"

namespace storage::experimental::mvlog {

segment_reader::segment_reader(
  interval_set<size_t> gaps, readable_segment* segment)
  : gaps_(std::move(gaps))
  , file_size_(segment->file_->size())
  , segment_(segment) {
    ++segment_->num_readers_;
}
segment_reader::~segment_reader() { --segment_->num_readers_; }

namespace {
auto to_start(interval_set<size_t>::const_iterator it) {
    return interval_set<size_t>::to_start(it);
}

auto to_end(interval_set<size_t>::const_iterator it) {
    return interval_set<size_t>::to_end(it);
}
} // namespace

skipping_data_source::read_list_t
segment_reader::make_read_intervals(size_t start_pos, size_t length) const {
    auto gap_it = gaps_.begin();
    const size_t max_pos = start_pos + length - 1;
    auto cur_iter_pos = start_pos;
    skipping_data_source::read_list_t read_intervals;

    // Iterate through the gaps associated with the version_id, collecting the
    // intervals to read, and skipping any that are entirely below the start
    // position.
    while (gap_it != gaps_.end() && cur_iter_pos <= max_pos) {
        const auto next_gap_max = to_end(gap_it) - 1;
        if (cur_iter_pos > next_gap_max) {
            // We are ahead of the next gap. Drop it from the list to consider.
            ++gap_it;
            continue;
        }
        if (cur_iter_pos >= to_start(gap_it)) {
            // We are in the middle of a gap. Skip to just past the end.
            // NOTE: the gap end is exclusive.
            cur_iter_pos = to_end(gap_it);
            ++gap_it;
            continue;
        }
        // The next gap is ahead of us. Read up to the start of it and skip
        // over the gap.
        const auto read_max_pos = std::min(max_pos, to_start(gap_it) - 1);
        const auto read_length = read_max_pos - cur_iter_pos + 1;
        read_intervals.emplace_back(cur_iter_pos, read_length);
        vlog(
          log.trace,
          "Adding read interval [{}, {}) within [{}, {})",
          cur_iter_pos,
          read_max_pos + 1,
          start_pos,
          max_pos + 1);
        cur_iter_pos = to_end(gap_it);
        ++gap_it;
    }
    // No more gaps, read the rest of the range.
    if (cur_iter_pos <= max_pos) {
        const auto remaining_length = max_pos - cur_iter_pos + 1;
        read_intervals.emplace_back(cur_iter_pos, remaining_length);
        vlog(
          log.trace,
          "Adding read interval [{}, {}) within [{}, {})",
          cur_iter_pos,
          cur_iter_pos + remaining_length,
          start_pos,
          max_pos + 1);
    }
    return read_intervals;
}

ss::input_stream<char> segment_reader::make_stream(size_t start_pos) const {
    return make_stream(start_pos, file_size_ - start_pos);
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
        segment_->file_, std::move(read_intervals))));
}

} // namespace storage::experimental::mvlog
