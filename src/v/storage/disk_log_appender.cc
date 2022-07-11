// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/disk_log_appender.h"

#include "likely.h"
#include "model/record_utils.h"
#include "storage/disk_log_impl.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "vlog.h"

#include <type_traits>

namespace storage {

disk_log_appender::disk_log_appender(
  disk_log_impl& log,
  log_append_config config,
  log_clock::time_point append_time,
  model::offset next_offset) noexcept
  : _log(log)
  , _config(config)
  , _append_time(append_time)
  , _idx(next_offset)
  , _base_offset(next_offset) {}

ss::future<> disk_log_appender::initialize() {
    if (_log._segs.empty()) {
        return ss::make_ready_future<>();
    }
    release_lock();
    auto ptr = _log._segs.back();
    // appending is a non-destructive op. so acquire read lock
    return ptr->read_lock().then([this, ptr](ss::rwlock::holder h) {
        _seg = ptr;
        _seg_lock = std::move(h);
        _bytes_left_in_segment = _log.bytes_left_before_roll();
    });
}

bool disk_log_appender::needs_to_roll_log(model::term_id batch_term) const {
    /**
     * _log._segs.empty() is a tricky condition. It is here to suppor concurrent
     * truncation (from 0) of an active log segment while we hold the lock of a
     * valid segment.
     *
     * Checking for term is because we support multiple term appends which
     * always roll
     *
     * _bytes_left_in_segment is for initial condition
     *
     */
    return _bytes_left_in_segment == 0 || _log.term() != batch_term
           || _log._segs.empty() /*see above before removing this condition*/;
}

void disk_log_appender::release_lock() {
    _seg = nullptr;
    _seg_lock = std::nullopt;
    _bytes_left_in_segment = 0;
}

ss::future<ss::stop_iteration>
disk_log_appender::operator()(model::record_batch& batch) {
    batch.header().base_offset = _idx;
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    if (_last_term != batch.term()) {
        release_lock();
    }
    _last_term = batch.term();
    auto f = ss::make_ready_future<>();
    if (unlikely(needs_to_roll_log(batch.term()))) {
        f = ss::do_until(
          [this, term = batch.term()] {
              // we might actually have space in the current log, but the terms
              // do not match for the current append, so we must roll
              return !needs_to_roll_log(term)
                     // we might have gotten the lock, but in a concurrency
                     // situation - say a segment eviction we need to double
                     // check that _after_ we got the lock, the segment wasn't
                     // somehow closed before the append
                     && _bytes_left_in_segment > 0;
          },
          [this] {
              release_lock();
              return _log.maybe_roll(_last_term, _idx, _config.io_priority)
                .then([this] { return initialize(); });
          });
    }
    return f
      .then([this, &batch]() mutable { return append_batch_to_segment(batch); })
      .handle_exception([this](std::exception_ptr e) {
          release_lock();
          vlog(stlog.info, "Could not append batch: {} - {}", e, *this);
          _log.get_probe().batch_write_error(e);
          return ss::make_exception_future<ss::stop_iteration>(e);
      });
}

ss::future<ss::stop_iteration>
disk_log_appender::append_batch_to_segment(const model::record_batch& batch) {
    // ghost batch handling, it doesn't happen often so we can use unlikely
    if (unlikely(
          batch.header().type == model::record_batch_type::ghost_batch)) {
        _idx = batch.last_offset() + model::offset(1); // next base offset
        _last_offset = batch.last_offset();
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    return _seg->append(batch).then([this](append_result r) {
        _idx = r.last_offset + model::offset(1); // next base offset
        _byte_size += r.byte_size;
        // do not track base_offset, only the last one
        _last_offset = r.last_offset;
        auto& p = _log.get_probe();
        p.add_bytes_written(r.byte_size);
        p.batch_written();

        // Register increase in dirty bytes since last STM snapshot
        _log.wrote_stm_bytes(r.byte_size);

        // substract the bytes from the append
        // take the min because _bytes_left_in_segment is optimistic
        _bytes_left_in_segment -= std::min(_bytes_left_in_segment, r.byte_size);
        return ss::stop_iteration::no;
    });
}

ss::future<append_result> disk_log_appender::end_of_stream() {
    auto retval = append_result{
      .append_time = _append_time,
      .base_offset = _base_offset,
      .last_offset = _last_offset,
      .byte_size = _byte_size,
      .last_term = _last_term};
    if (_config.should_fsync == storage::log_append_config::fsync::no) {
        return ss::make_ready_future<append_result>(retval);
    }
    return _log.flush().then([this, retval] {
        release_lock();
        return retval;
    });
}

std::ostream& operator<<(std::ostream& o, const disk_log_appender& a) {
    return o << "{offset_idx:" << a._idx
             << ", active_segment:" << (a._seg_lock ? "yes" : "no")
             << ", _bytes_left_in_segment:" << a._bytes_left_in_segment
             << ", _base_offset:" << a._base_offset
             << ", _last_offset:" << a._last_offset
             << ", _last_term:" << a._last_term
             << ", _byte_size:" << a._byte_size << "}";
}
} // namespace storage
