/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/async_data_uploader.h"

#include "base/vlog.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/types.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "storage/disk_log_impl.h"
#include "storage/offset_to_filepos.h"
#include "storage/record_batch_utils.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>

#include <exception>
#include <stdexcept>
#include <variant>

namespace archival {

class reader_ds : public ss::data_source_impl {
    /// Consumer that reads data batches to the read buffer
    /// of the data source.
    class consumer {
    public:
        explicit consumer(reader_ds* ds, inclusive_offset_range range)
          : _parent(ds)
          , _range(range) {}

        /// This consumer accepts all batches if there is enough space in the
        /// read buffer.
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            vlog(
              _parent->_ctxlog.debug,
              "consume started, buffered {} bytes",
              _parent->_buffer.size_bytes());
            if (!_range.contains(batch.header())) {
                vlog(
                  _parent->_ctxlog.debug,
                  "skip batch {}-{}",
                  batch.base_offset(),
                  batch.last_offset());

                co_return ss::stop_iteration::no;
            }
            vlog(
              _parent->_ctxlog.debug,
              "consuming batch {}-{}",
              batch.base_offset(),
              batch.last_offset());
            auto hdr_iobuf = storage::batch_header_to_disk_iobuf(
              batch.header());
            auto rec_iobuf = std::move(batch).release_data();
            _parent->_buffer.append(std::move(hdr_iobuf));
            _parent->_buffer.append(std::move(rec_iobuf));
            _parent->_num_batches++;
            bool max_bytes_reached = _parent->_buffer.size_bytes()
                                     > _parent->_max_bytes;
            vlog(
              _parent->_ctxlog.debug,
              "max_bytes_reached: {}",
              max_bytes_reached);
            co_return max_bytes_reached ? ss::stop_iteration::yes
                                        : ss::stop_iteration::no;
        }

        bool end_of_stream() const { return false; }

    private:
        reader_ds* _parent;
        inclusive_offset_range _range;
    };

    friend class consumer;

public:
    explicit reader_ds(
      model::ntp ntp,
      model::record_batch_reader r,
      size_t max_bytes,
      inclusive_offset_range range,
      model::timeout_clock::time_point deadline)
      : _reader(std::move(r))
      , _max_bytes(max_bytes)
      , _deadline(deadline)
      , _range(range)
      , _rtc(_as)
      , _ctxlog(archival_log, _rtc, ntp.path()) {}

    ss::future<ss::temporary_buffer<char>> get() override {
        // Consume using the reader until the reader is done
        if (_buffer.empty() && !_reader.is_end_of_stream()) {
            vlog(
              _ctxlog.trace, "Buffer is empty, pulling data from the reader");
            consumer c(this, _range);
            auto done = co_await _reader.consume(c, _deadline);
            vlog(
              _ctxlog.trace,
              "Consume returned {}, buffer has {} bytes",
              done,
              _buffer.size_bytes());
        }
        if (_buffer.empty()) {
            vlog(
              _ctxlog.trace,
              "Reader end-of-stream: {}",
              _reader.is_end_of_stream());
            ss::temporary_buffer<char> empty;
            co_return empty;
        }
        auto head = _buffer.begin();
        auto head_buf = std::move(*head).release();
        _buffer.pop_front();
        vlog(
          _ctxlog.debug,
          "get produced {} bytes, {} bytes buffered",
          head_buf.size(),
          _buffer.size_bytes());
        co_return head_buf;
    }
    ss::future<ss::temporary_buffer<char>> skip(uint64_t) override {
        ss::temporary_buffer<char> empty;
        co_return empty;
    }
    ss::future<> close() override {
        co_await std::move(_reader).release()->finally();
    }

private:
    model::record_batch_reader _reader;
    iobuf _buffer;
    size_t _max_bytes;
    size_t _num_batches{};
    model::timeout_clock::time_point _deadline;
    inclusive_offset_range _range;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

ss::input_stream<char> make_reader_input_stream(
  model::ntp ntp,
  model::record_batch_reader r,
  size_t read_buffer_size,
  inclusive_offset_range range,
  model::timeout_clock::duration timeout) {
    auto deadline = model::timeout_clock::now() + timeout;
    auto ds = std::make_unique<reader_ds>(
      ntp, std::move(r), read_buffer_size, range, deadline);
    ss::input_stream<char> s(ss::data_source(std::move(ds)));
    return s;
}

segment_upload::segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  size_t read_buffer_size,
  ss::scheduling_group sg)
  : _ntp(part->get_ntp_config().ntp())
  , _part(part)
  , _rd_buffer_size(read_buffer_size)
  , _sg(sg)
  , _rtc(_as)
  , _ctxlog(archival_log, _rtc, _ntp.path()) {}

void segment_upload::throw_if_not_initialized(std::string_view caller) const {
    if (_stream.has_value() == false) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Segment upload is not initialized {} to {}",
          _ntp,
          caller));
    }
}

ss::future<result<std::unique_ptr<segment_upload>>>
segment_upload::make_segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  inclusive_offset_range range,
  size_t read_buffer_size,
  ss::scheduling_group sg,
  model::timeout_clock::time_point deadline) {
    std::unique_ptr<segment_upload> upl(
      new segment_upload(part, read_buffer_size, sg));

    auto res = co_await upl->initialize(range, deadline);
    if (res.has_failure()) {
        co_return res.as_failure();
    }
    co_return std::move(upl);
}

ss::future<result<std::unique_ptr<segment_upload>>>
segment_upload::make_segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  size_limited_offset_range range,
  size_t read_buffer_size,
  ss::scheduling_group sg,
  model::timeout_clock::time_point deadline) {
    std::unique_ptr<segment_upload> upl(
      new segment_upload(part, read_buffer_size, sg));

    auto res = co_await upl->initialize(range, deadline);
    if (res.has_failure()) {
        co_return res.as_failure();
    }
    co_return std::move(upl);
}

ss::future<result<void>> segment_upload::initialize(
  inclusive_offset_range range, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    auto params = co_await compute_upload_parameters(range);
    if (params.has_failure()) {
        co_return params.as_failure();
    }
    // Invariant: `compute_upload_params` guarantees that range.last matches
    // params.offsets.last on success.
    _params = params.value();
    vassert(
      _params->offsets == range,
      "Unexpected offset range {}, expected {}",
      _params->offsets,
      range);
    // Create a log reader config to scan the uploaded offset
    // range. We should skip the batch cache.
    storage::log_reader_config reader_cfg(
      range.base, range.last, priority_manager::local().archival_priority());
    reader_cfg.skip_batch_cache = true;
    reader_cfg.skip_readers_cache = true;
    vlog(_ctxlog.debug, "Creating log reader, config: {}", reader_cfg);
    auto reader = co_await _part->make_reader(reader_cfg);
    _stream = make_reader_input_stream(
      _ntp,
      std::move(reader),
      _rd_buffer_size,
      range,
      model::time_until(deadline));
    co_return outcome::success();
}

ss::future<result<void>> segment_upload::initialize(
  size_limited_offset_range range, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    auto params = co_await compute_upload_parameters(range);
    if (params.has_failure()) {
        co_return params.as_failure();
    }
    _params = params.value();
    // Create a log reader config to scan the uploaded offset
    // range. We should skip the batch cache.
    storage::log_reader_config reader_cfg(
      params.value().offsets.base,
      params.value().offsets.last,
      priority_manager::local().archival_priority());
    reader_cfg.skip_batch_cache = true;
    reader_cfg.skip_readers_cache = true;
    vlog(_ctxlog.debug, "Creating log reader, config: {}", reader_cfg);
    auto reader = co_await _part->make_reader(reader_cfg);
    _stream = make_reader_input_stream(
      _ntp,
      std::move(reader),
      _rd_buffer_size,
      params.value().offsets,
      model::time_until(deadline));
    co_return outcome::success();
}

ss::future<> segment_upload::close() {
    if (_stream.has_value()) {
        co_await _stream->close();
    }
    co_await _gate.close();

    // Return units
    _params.reset();
}

ss::future<result<upload_reconciliation_result>>
segment_upload::compute_upload_parameters(
  std::variant<inclusive_offset_range, size_limited_offset_range> input) {
    auto holder = _gate.hold();
    try {
        auto range_base = std::visit(
          [](auto range) { return range.base; }, input);
        std::optional<storage::log::offset_range_size_result_t> sz;
        if (std::holds_alternative<inclusive_offset_range>(input)) {
            auto range = std::get<inclusive_offset_range>(input);
            sz = co_await _part->log()->offset_range_size(
              range.base,
              range.last,
              priority_manager::local().archival_priority());
        } else {
            auto range = std::get<size_limited_offset_range>(input);
            sz = co_await _part->log()->offset_range_size(
              range.base,
              storage::log::offset_range_size_requirements_t{
                .target_size = range.max_size,
                .min_size = range.min_size,
              },
              priority_manager::local().archival_priority());
        }
        if (!sz.has_value()) {
            // This means that there is not enough data in the log
            // to satisfy the request.
            co_return make_error_code(error_outcome::not_enough_data);
        }
        upload_reconciliation_result result{
          .size_bytes = sz->on_disk_size,
          .is_compacted = _part->log()->is_compacted(
            range_base, sz->last_offset),
          .offsets = inclusive_offset_range(range_base, sz->last_offset),
        };
        co_return result;
    } catch (const std::invalid_argument&) {
        // This means that the requested range.base is out of range (most likely
        // due to truncation).
        vlog(_ctxlog.warn, "Index out of range: {}", std::current_exception());
        co_return make_error_code(error_outcome::out_of_range);

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(
              _ctxlog.debug,
              "Shutdown exception: {}",
              std::current_exception());
            co_return make_error_code(error_outcome::shutting_down);
        }
        vlog(
          _ctxlog.error, "Unexpected exception: {}", std::current_exception());
        co_return make_error_code(error_outcome::unexpected_failure);
    }
}

} // namespace archival
