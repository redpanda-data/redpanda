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

#include "storage/parser.h"
#include "storage/segment.h"

#include <seastar/core/iostream.hh>

namespace {
// Data sink for noop output_stream instance
// needed to implement scanning
struct null_data_sink final : ss::data_sink_impl {
    ss::future<> put(ss::net::packet data) final { return put(data.release()); }
    ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
        return ss::do_with(
          std::move(all), [this](std::vector<ss::temporary_buffer<char>>& all) {
              return ss::do_for_each(
                all, [this](ss::temporary_buffer<char>& buf) {
                    return put(std::move(buf));
                });
          });
    }
    ss::future<> put(ss::temporary_buffer<char>) final { return ss::now(); }
    ss::future<> flush() final { return ss::now(); }
    ss::future<> close() final { return ss::now(); }
};

ss::output_stream<char> make_null_output_stream() {
    auto ds = ss::data_sink(std::make_unique<null_data_sink>());
    ss::output_stream<char> ostr(std::move(ds), 4_KiB);
    return ostr;
}

} // namespace

namespace storage {

namespace internal {

offset_to_filepos_consumer::offset_to_filepos_consumer(
  model::offset log_start_offset, model::offset target, size_t initial)
  : _target_last_offset(target)
  , _prev_batch_last_offset(model::prev_offset(log_start_offset))
  , _accumulator(initial)
  , _prev(initial) {}

ss::future<ss::stop_iteration>
offset_to_filepos_consumer::operator()(::model::record_batch batch) {
    _prev = _accumulator;
    _accumulator += batch.size_bytes();

    if (_target_last_offset <= batch.base_offset()) {
        _filepos = {_prev_batch_last_offset, _prev};
        co_return ss::stop_iteration::yes;
    }
    if (
      _target_last_offset > batch.base_offset()
      && _target_last_offset <= batch.last_offset()) {
        throw std::runtime_error(fmt::format(
          "Offset to file position consumer isn't able to translate "
          "offsets other than batch base offset or offset being in the "
          "gap. Requested offset: {}, current batch offsets: [{},{}]",
          _target_last_offset,
          batch.base_offset(),
          batch.last_offset()));
    }

    _prev_batch_last_offset = batch.last_offset();
    co_return ss::stop_iteration::no;
}

offset_to_filepos_consumer::type offset_to_filepos_consumer::end_of_stream() {
    return _filepos;
}

} // namespace internal

std::ostream&
operator<<(std::ostream& os, convert_to_file_pos_outcome conv_res) {
    switch (conv_res) {
    case success:
        os << "success";
        break;
    case offset_not_in_segment:
        os << "offset_not_in_segment";
        break;
    }

    return os;
}

ss::future<checked<offset_to_file_pos_result, convert_to_file_pos_outcome>>
convert_begin_offset_to_file_pos(
  model::offset begin_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp base_timestamp,
  ss::io_priority_class io_priority) {
    auto ix_begin = segment->index().find_nearest(begin_inclusive);
    size_t scan_from = ix_begin ? ix_begin->filepos : 0;
    model::offset sto = ix_begin ? ix_begin->offset
                                 : segment->offsets().base_offset;
    auto reader_handle = co_await segment->reader().data_stream(
      scan_from, io_priority);
    auto ostr = make_null_output_stream();

    size_t bytes_to_skip = 0;

    model::timestamp ts = base_timestamp;
    bool offset_found = false;
    auto res = co_await storage::transform_stream(
      reader_handle.take_stream(),
      std::move(ostr),
      [begin_inclusive, &sto, &ts, &offset_found](
        model::record_batch_header& hdr) {
          if (hdr.last_offset() < begin_inclusive) {
              // The current record batch is accepted and will contribute to
              // skipped length. This means that if we will read segment
              // file starting from the 'scan_from' + 'res' we will be
              // looking at the next record batch. We might not see the
              // offset that we're looking for in this segment. This is why
              // we need to update 'sto' per batch.
              sto = hdr.last_offset() + model::offset(1);
              return storage::batch_consumer::consume_result::accept_batch;
          }
          offset_found = true;
          ts = hdr.first_timestamp;
          return storage::batch_consumer::consume_result::stop_parser;
      });
    co_await reader_handle.close();
    if (res.has_error()) {
        vlog(stlog.error, "Can't read segment file, error: {}", res.error());
        throw std::system_error(res.error());
    }

    if (!offset_found) {
        vlog(
          stlog.warn,
          "Segment does not contain searched for offset: {}, segment offsets: "
          "{}",
          sto,
          segment->offsets());
        co_return storage::convert_to_file_pos_outcome::offset_not_in_segment;
    }

    bytes_to_skip = scan_from + res.value();
    vlog(
      stlog.debug,
      "Scanned {} bytes starting from {}, total {}. Adjusted starting offset: "
      "{}",
      res.value(),
      scan_from,
      bytes_to_skip,
      sto);
    // Adjust content length and offsets at the begining of the file
    co_return offset_to_file_pos_result{sto, bytes_to_skip, ts};
}

ss::future<checked<offset_to_file_pos_result, convert_to_file_pos_outcome>>
convert_end_offset_to_file_pos(
  model::offset end_inclusive,
  ss::lw_shared_ptr<storage::segment> segment,
  model::timestamp max_timestamp,
  ss::io_priority_class io_priority) {
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
        vlog(stlog.debug, "Re-adjusted position {}", *ix_end);
    }

    size_t scan_from = ix_end ? ix_end->filepos : 0;
    model::offset fo = ix_end ? ix_end->offset : segment->offsets().base_offset;
    vlog(
      stlog.debug,
      "Segment index lookup returned: {}, scanning from pos {} - offset {}",
      ix_end,
      scan_from,
      fo);

    auto reader_handle = co_await segment->reader().data_stream(
      scan_from, io_priority);
    auto ostr = make_null_output_stream();
    model::timestamp ts = max_timestamp;
    size_t stop_at = 0;

    bool offset_found = false;
    auto res = co_await storage::transform_stream(
      reader_handle.take_stream(),
      std::move(ostr),
      [off_end = end_inclusive, &fo, &ts, &offset_found, &max_timestamp](
        model::record_batch_header& hdr) {
          if (hdr.last_offset() <= off_end) {
              // If last offset of the record batch is within the range
              // we need to add it to the output stream (to calculate the
              // total size).
              fo = hdr.last_offset();

              if (hdr.last_offset() == off_end) {
                  ts = hdr.max_timestamp;
              }

              return storage::batch_consumer::consume_result::accept_batch;
          }
          offset_found = true;

          if (ts == max_timestamp) {
              ts = hdr.max_timestamp;
          }
          return storage::batch_consumer::consume_result::stop_parser;
      });
    co_await reader_handle.close();

    if (res.has_error()) {
        vlog(stlog.error, "Can't read segment file, error: {}", res.error());
        throw std::system_error(res.error());
    }

    if (!offset_found) {
        vlog(
          stlog.warn,
          "Segment does not contain searched for offset: {}, segment offsets: "
          "{}",
          end_inclusive,
          segment->offsets());
        co_return storage::convert_to_file_pos_outcome::offset_not_in_segment;
    }

    stop_at = scan_from + res.value();
    vlog(
      stlog.debug,
      "Scanned {} bytes starting from {}, total {}. Adjusted final offset: {}",
      res.value(),
      scan_from,
      stop_at,
      fo);

    co_return offset_to_file_pos_result{fo, stop_at, ts};
}

} // namespace storage
