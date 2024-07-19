// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "jumbo_log/segment_reader/segment_reader.h"

#include "base/likely.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "jumbo_log/segment/internal.h"
#include "jumbo_log/segment/segment.h"
#include "jumbo_log/segment/sparse_index.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "serde/rw/rw.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <stdexcept>
#include <stdint.h>
#include <utility>

namespace {

void skip_all_batches_for_current_ntp_in_chunk(iobuf_parser& parser) {
    auto data_size
      = serde::read_nested<jumbo_log::segment::chunk_data_batches_size_t>(
        parser, 0);
    parser.skip(data_size);
}

inline constexpr auto READ_SOME_LIMIT = 4 * 1024 * 1024;

} // namespace

namespace jumbo_log::segment_reader {

chunk_reader_from_iobuf::chunk_reader_from_iobuf(
  iobuf buf, model::ntp ntp, model::bounded_offset_interval offset_interval)
  : _parser(std::move(buf))
  , _ntp(std::move(ntp))
  , _offset_interval(offset_interval) {}

ss::future<ss::circular_buffer<model::record_batch>>
chunk_reader_from_iobuf::read_some() {
    size_t ret_accumulated_bytes = 0;
    ss::circular_buffer<model::record_batch> ret{};

    while (_parser.bytes_left() > 0) {
        if (unlikely(_reached_end)) {
            co_return ret;
        } else if (!_ntp_seek_done) {
            auto ntp_under_cursor = serde::read_nested<model::ntp>(_parser, 0);
            if (ntp_under_cursor == _ntp) {
                _ntp_seek_done = true;
                _batches_bytes_left
                  = serde::read_nested<segment::chunk_data_batches_size_t>(
                    _parser, 0);
            } else if (_ntp < ntp_under_cursor) {
                if (!_first_ntp_check_done) {
                    throw std::runtime_error(
                      "jls reader invariant violation: "
                      "requested NTP is less-than the first "
                      "NTP in the chunk");
                }
                _ntp_seek_done = true;
                _reached_end = true;
            } else if (ntp_under_cursor < _ntp) {
                skip_all_batches_for_current_ntp_in_chunk(_parser);
            }
        } else if (!_offset_seek_done) {
            if (_batches_bytes_left == 0) {
                _reached_end = true;
                co_return ret;
            }

            // TODO(nv): We need only the header.
            auto before_bytes_left = _parser.bytes_left();
            auto record_batch = serde::read_nested<model::record_batch>(
              _parser, 0);
            auto after_bytes_left = _parser.bytes_left();

            // serde api is not very convenient for this.
            _batches_bytes_left -= before_bytes_left - after_bytes_left;

            // We are looking for the first batch that overlaps with the
            // requested offset interval.
            auto overlap = _offset_interval.overlaps(
              model::bounded_offset_interval::unchecked(
                record_batch.base_offset(), record_batch.last_offset()));

            if (overlap) {
                _offset_seek_done = true;

                ret.push_back(std::move(record_batch));
                ret_accumulated_bytes += record_batch.size_bytes();
            } else if (_offset_interval.min() > record_batch.last_offset()) {
                // Early exit if we know there won't be any overlap.
                _reached_end = true;
                co_return ret;
            } else {
                // Skip the batch.
            }
        } else {
            while (_batches_bytes_left > 0
                   && ret_accumulated_bytes < READ_SOME_LIMIT) {
                auto before_bytes_left = _parser.bytes_left();
                auto record_batch = serde::read_nested<model::record_batch>(
                  _parser, 0);
                auto after_bytes_left = _parser.bytes_left();

                _batches_bytes_left -= before_bytes_left - after_bytes_left;

                auto include = _offset_interval.overlaps(
                  model::bounded_offset_interval::unchecked(
                    record_batch.base_offset(), record_batch.last_offset()));

                // Keep we find the first batch that doesn't overlap anymore.
                if (include) {
                    ret_accumulated_bytes += record_batch.size_bytes();
                    ret.push_back(std::move(record_batch));
                } else {
                    _reached_end = true;
                    co_return ret;
                }
            }

            if (_batches_bytes_left == 0) {
                _reached_end = true;
            }
        }
    }

    co_return ret;
}

ss::future<std::pair<jumbo_log::segment::sparse_index, int64_t>>
sparse_index_reader_from_iobuf::read(iobuf buf) {
    auto parser = iobuf_parser(std::move(buf));
    auto bytes_left = parser.bytes_left();

    auto dat = jumbo_log::segment::internal::sparse_index_data{};

    auto num_entries = serde::read_nested<int64_t>(parser, 0);
    dat.entries.reserve(num_entries);

    for (int64_t i = 0; i < num_entries; ++i) {
        auto ntp = serde::read_nested<model::ntp>(parser, 0);
        auto offset = serde::read_nested<model::offset>(parser, 0);
        auto offset_bytes = serde::read_nested<size_t>(parser, 0);
        auto size_bytes = serde::read_nested<size_t>(parser, 0);

        dat.entries.push_back(
            segment::internal::chunk_index_entry{
                .ntp = ntp,
                .offset = offset,
                .loc = segment::chunk_loc{
                        .offset_bytes = offset_bytes,
                        .size_bytes = size_bytes,
                },
        });

        co_await seastar::maybe_yield();
    }

    auto upper_limit_ntp = serde::read_nested<model::ntp>(parser, 0);
    auto upper_limit_offset = serde::read_nested<model::offset>(parser, 0);

    dat.upper_limit = std::make_pair(upper_limit_ntp, upper_limit_offset);

    // Not sure if we want this yet. Currently the index size is not serialized
    // so we can't give a buffer of the exact size to this method.
    // vassert(
    //   parser.bytes_left() == 0,
    //   "jls reader invariant violation: "
    //   "unexpected bytes left in the sparse index");

    auto consumed_bytes = bytes_left - parser.bytes_left();

    co_return std::make_pair(
      jumbo_log::segment::sparse_index(std::move(dat)), consumed_bytes);
};

}; // namespace jumbo_log::segment_reader
