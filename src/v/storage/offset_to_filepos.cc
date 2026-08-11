/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/offset_to_filepos.h"

#include "base/vlog.h"
#include "storage/logger.h"
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_utils.h"
#include "utils/null_output_stream.h"

#include <seastar/core/iostream.hh>
#include <seastar/coroutine/exception.hh>

namespace storage {

namespace internal {

offset_to_filepos_consumer::offset_to_filepos_consumer(
  model::offset log_start_offset,
  model::offset target,
  size_t initial,
  model::timestamp initial_timestamp)
  : _target_last_offset(target)
  , _prev_batch_last_offset(model::prev_offset(log_start_offset))
  , _prev_batch_max_timestamp(initial_timestamp)
  , _prev_end_pos(initial) {}

ss::future<ss::stop_iteration>
offset_to_filepos_consumer::operator()(::model::record_batch batch) {
    if (batch.base_offset() >= _target_last_offset) {
        _filepos = {
          _prev_batch_last_offset, _prev_end_pos, _prev_batch_max_timestamp};
        co_return ss::stop_iteration::yes;
    }
    if (
      _target_last_offset > batch.base_offset()
      && _target_last_offset <= batch.last_offset()) {
        throw std::runtime_error(
          fmt::format(
            "Offset to file position consumer isn't able to translate "
            "offsets other than batch base offset or offset being in the "
            "gap. Requested offset: {}, current batch offsets: [{},{}]",
            _target_last_offset,
            batch.base_offset(),
            batch.last_offset()));
    }

    _prev_batch_last_offset = batch.last_offset();
    _prev_batch_max_timestamp = std::max(
      batch.header().first_timestamp, batch.header().max_timestamp);
    _prev_end_pos += batch.size_bytes();
    co_return ss::stop_iteration::no;
}

offset_to_filepos_consumer::type offset_to_filepos_consumer::end_of_stream() {
    return _filepos;
}

bool is_offset_in_batch(
  const model::record_batch_header& header, model::offset o) {
    // Note: header.contains also matches if offset is on the boundary. This
    // function checks if the offset lies strictly inside the batch.
    return header.base_offset < o && header.last_offset() > o;
}

} // namespace internal

ss::future<result<offset_to_file_pos_result>> convert_begin_offset_to_file_pos(
  model::offset begin_inclusive,
  ss::lw_shared_ptr<segment> segment,
  model::timestamp base_timestamp,
  should_fail_on_missing_offset fail_on_missing_offset) {
    auto ix_begin = segment->index().find_nearest(begin_inclusive);
    size_t scan_from = ix_begin ? ix_begin->filepos : 0;
    model::offset sto = ix_begin ? ix_begin->offset
                                 : segment->offsets().get_base_offset();

    model::timestamp ts = base_timestamp;
    bool offset_found = false;
    auto handle = co_await segment->reader().data_stream(scan_from);

    bool offset_inside_batch = false;
    auto res = co_await storage::internal::with_segment_reader_handle(
      std::move(handle),
      [&begin_inclusive, &sto, &offset_found, &ts, &offset_inside_batch](
        segment_reader_handle& reader_handle) {
          auto ostr = utils::make_null_output_stream();
          return transform_stream(
            reader_handle.take_stream(),
            std::move(ostr),
            [begin_inclusive, &sto, &ts, &offset_found, &offset_inside_batch](
              model::record_batch_header& hdr) {
                if (hdr.last_offset() < begin_inclusive) {
                    // The current record batch is accepted and will contribute
                    // to skipped length. This means that if we will read
                    // segment file starting from the 'scan_from' + 'res' we
                    // will be looking at the next record batch. We might not
                    // see the offset that we're looking for in this segment.
                    // This is why we need to update 'sto' per batch.
                    sto = hdr.last_offset() + model::offset(1);
                    return batch_consumer::consume_result::accept_batch;
                }

                if (internal::is_offset_in_batch(hdr, begin_inclusive)) {
                    offset_inside_batch = true;
                }

                offset_found = true;
                // Only a data batch may describe where the range begins: a
                // configuration or archival metadata batch carries walltime,
                // which on a topic whose data sits far from walltime would
                // describe a time the range does not cover - and one that can
                // land above the range's own maximum. Leaving the caller's
                // seed, the segment's own data-only base, under-reports
                // instead, which is the safe direction.
                if (hdr.type == model::record_batch_type::raft_data) {
                    ts = hdr.first_timestamp;
                }
                return batch_consumer::consume_result::stop_parser;
            });
      });

    if (res.has_error()) {
        vlog(stlog.error, "Can't read segment file, error: {}", res.error());
        co_await ss::coroutine::return_exception(
          std::system_error(res.error()));
    }

    if (!offset_found && fail_on_missing_offset) {
        vlog(
          stlog.warn,
          "Segment {} does not contain searched for offset: {}",
          segment,
          sto);
        co_return std::make_error_code(std::errc::invalid_seek);
    }

    size_t bytes_to_skip = scan_from + res.value();
    vlog(
      stlog.debug,
      "Scanned {} bytes starting from {}, total {}. Adjusted starting offset: "
      "{}",
      res.value(),
      scan_from,
      bytes_to_skip,
      sto);
    // Adjust content length and offsets at the begining of the file
    co_return offset_to_file_pos_result{
      sto, bytes_to_skip, ts, offset_inside_batch};
}

ss::future<result<offset_to_file_pos_result>> convert_end_offset_to_file_pos(
  model::offset end_inclusive,
  ss::lw_shared_ptr<segment> segment,
  model::timestamp max_timestamp,
  should_fail_on_missing_offset fail_on_missing_offset) {
    // Handle truncated segment upload (if the upload was triggered by time
    // limit). Note that the upload is not necessarily started at the beginning
    // of the segment.
    // Lookup the index, if the index is available and some value is found
    // use it as a starting point otherwise, start from the beginning.
    auto ix_end = segment->index().find_nearest(end_inclusive);
    size_t fsize = segment->reader().file_size();

    // NOTE: Index lookup might return an offset which isn't committed yet.
    // Subsequent call to segment_reader::data_stream will fail in this
    // case. In order to avoid this we need to make another index lookup
    // to find a lower offset which is committed.
    while (ix_end && ix_end->filepos >= fsize) {
        vlog(stlog.debug, "The position is not flushed {}", *ix_end);
        auto lookup_offset = ix_end->offset - model::offset(1);
        ix_end = segment->index().find_nearest(lookup_offset);
        if (ix_end) {
            vlog(stlog.debug, "Re-adjusted position {}", *ix_end);
        }
    }

    size_t scan_from = ix_end ? ix_end->filepos : 0;
    model::offset fo = ix_end ? ix_end->offset
                              : segment->offsets().get_base_offset();
    vlog(
      stlog.debug,
      "Segment index lookup returned: {}, scanning from pos {} - offset {}",
      ix_end,
      scan_from,
      fo);

    bool offset_found = false;
    model::timestamp ts = max_timestamp;
    // Running maximum over the data batches inside the range.
    model::timestamp max_data_ts = model::timestamp::missing();

    auto reader_handle = co_await segment->reader().data_stream(scan_from);

    bool offset_inside_batch = false;
    auto res = co_await storage::internal::with_segment_reader_handle(
      std::move(reader_handle),
      [&end_inclusive, &fo, &offset_found, &max_data_ts, &offset_inside_batch](
        segment_reader_handle& handle) {
          auto ostr = utils::make_null_output_stream();
          return transform_stream(
            handle.take_stream(),
            std::move(ostr),
            [off_end = end_inclusive,
             &fo,
             &max_data_ts,
             &offset_found,
             &offset_inside_batch](model::record_batch_header& hdr) {
                if (hdr.last_offset() <= off_end) {
                    // If last offset of the record batch is within the range
                    // we need to add it to the output stream (to calculate the
                    // total size).
                    fo = hdr.last_offset();

                    if (hdr.type == model::record_batch_type::raft_data) {
                        max_data_ts = std::max(
                          max_data_ts, model::batch_max_timestamp(hdr));
                    }

                    if (hdr.last_offset() == off_end) {
                        offset_found = true;
                    }

                    return batch_consumer::consume_result::accept_batch;
                }

                if (internal::is_offset_in_batch(hdr, off_end)) {
                    offset_inside_batch = true;
                }

                offset_found = true;

                return batch_consumer::consume_result::stop_parser;
            });
      });

    // The scan starts at an index entry, so data batches before it are never
    // visited and the largest timestamp in the range may be one of them. Bound
    // the result by what the index knows about everything up to that entry:
    // with a running-max time column that is the prefix maximum, and without
    // one the segment's own data-only maximum. Note the entry sits near the
    // range's end, so neither is a tight bound - both can reach past the range.
    // That is the safe direction for a query, though it does make a segment
    // look newer than its data to time-based retention; under-reporting would
    // make a timequery pass over the segment holding the first matching record.
    const auto prefix_max = ix_end
                                && segment->index().has_running_max_timestamps()
                              ? ix_end->timestamp
                              : segment->index().max_timestamp();
    ts = std::max(max_data_ts, prefix_max);

    if (res.has_error()) {
        vlog(stlog.error, "Can't read segment file, error: {}", res.error());
        co_await ss::coroutine::return_exception(
          std::system_error(res.error()));
    }

    if (!offset_found && fail_on_missing_offset) {
        vlog(
          stlog.warn,
          "Segment does not contain searched for offset: {}, segment offsets: "
          "{}",
          end_inclusive,
          segment->offsets());
        co_return std::make_error_code(std::errc::invalid_seek);
    }

    size_t stop_at = scan_from + res.value();
    vlog(
      stlog.debug,
      "Scanned {} bytes starting from {}, total {}. Adjusted final offset: {}",
      res.value(),
      scan_from,
      stop_at,
      fo);

    co_return offset_to_file_pos_result{fo, stop_at, ts, offset_inside_batch};
}

} // namespace storage
