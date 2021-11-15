/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "resource_mgmt/io_priority.h"
#include "storage/parser.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/log.hh>

#include <exception>

namespace cloud_storage {

static constexpr size_t max_consume_size = 128_KiB;

static ss::lowres_clock::duration cache_hydration_timeout = 60s;
static ss::lowres_clock::duration cache_hydration_backoff = 250ms;

download_exception::download_exception(
  download_result r, std::filesystem::path p)
  : result(r)
  , path(std::move(p)) {
    vassert(
      r != download_result::success,
      "Exception created with successful error code");
}

const char* download_exception::what() const noexcept {
    switch (result) {
    case download_result::failed:
        return "Failed";
    case download_result::notfound:
        return "NotFound";
    case download_result::timedout:
        return "TimedOut";
    case download_result::success:
        vassert(false, "Successful result can't be used as an error");
    }
    __builtin_unreachable();
}

inline void expiry_handler_impl(ss::promise<std::filesystem::path>& pr) {
    pr.set_exception(ss::timed_out_error());
}

remote_segment::remote_segment(
  remote& r,
  cache& c,
  s3::bucket_name bucket,
  const manifest& m,
  manifest::key path,
  retry_chain_node& parent)
  : _api(r)
  , _cache(c)
  , _bucket(std::move(bucket))
  , _manifest(m)
  , _path(std::move(path))
  , _rtc(&parent)
  , _ctxlog(cst_log, _rtc, get_ntp().path())
  , _wait_list(expiry_handler_impl) {
    // run hydration loop in the background
    (void)run_hydrate_bg();
}

const model::ntp& remote_segment::get_ntp() const {
    return _manifest.get_ntp();
}

const model::offset remote_segment::get_max_rp_offset() const {
    const auto meta = _manifest.get(_path);
    // The remote_segment is built based on manifest so we can
    // expect the _path to be present in the manifest.
    vassert(
      meta, "Can't find segment metadata in manifest, segment path: {}", _path);
    return meta->committed_offset;
}

const model::offset remote_segment::get_base_offset_delta() const {
    const auto meta = _manifest.get(_path);
    vassert(
      meta, "Can't find segment metadata in manifest, segment path: {}", _path);
    return std::clamp(
      meta->delta_offset, model::offset(0), model::offset::max());
}

const model::offset remote_segment::get_base_rp_offset() const {
    const auto meta = _manifest.get(_path);
    vassert(
      meta, "Can't find segment metadata in manifest, segment path: {}", _path);
    return meta->base_offset;
}

const model::offset remote_segment::get_base_kafka_offset() const {
    const auto meta = _manifest.get(_path);
    vassert(
      meta, "Can't find segment metadata in manifest, segment path: {}", _path);
    auto delta = std::clamp(
      meta->delta_offset, model::offset(0), model::offset::max());
    return meta->base_offset - delta;
}

const model::term_id remote_segment::get_term() const {
    std::filesystem::path p = std::visit(
      [](auto&& arg) { return std::filesystem::path(arg()); }, _path);
    auto [_, term, success] = parse_segment_name(p);
    vassert(success, "Can't parse segment name, name: {}", p);
    return term;
}

ss::future<> remote_segment::stop() {
    vlog(_ctxlog.debug, "remote segment stop");
    _bg_cvar.broken();
    return _gate.close();
}

ss::future<ss::input_stream<char>>
remote_segment::data_stream(size_t pos, ss::io_priority_class) {
    vlog(_ctxlog.debug, "remote segment file input stream at {}", pos);
    ss::gate::holder g(_gate);
    // Hydrate segment on disk
    auto full_path = co_await hydrate();
    // Create a file stream
    auto opt = co_await _cache.get(full_path, pos);
    if (opt) {
        co_return std::move(opt->body);
    }
    throw remote_segment_exception(
      fmt::format("Segment {} already evicted", full_path));
}

ss::future<> remote_segment::run_hydrate_bg() {
    ss::gate::holder guard(_gate);
    auto full_path = _manifest.get_remote_segment_path(_path);
    try {
        while (!_gate.is_closed()) {
            co_await _bg_cvar.wait(
              [this] { return !_wait_list.empty() || _gate.is_closed(); });
            vlog(
              _ctxlog.info,
              "Start hydrating segment {}, {} consumers are awaiting",
              full_path,
              _wait_list.size());
            auto status = co_await _cache.is_cached(full_path);
            std::exception_ptr err;
            switch (status) {
            case cache_element_status::in_progress:
                vassert(
                  false,
                  "Hydration of segment {} is already in progress, {} waiters",
                  full_path,
                  _wait_list.size());
            case cache_element_status::available:
                vlog(
                  _ctxlog.debug,
                  "Hydrated segment {} is already available, {} waiters will "
                  "be invoked",
                  full_path,
                  _wait_list.size());
                break;
            case cache_element_status::not_available: {
                vlog(_ctxlog.info, "Hydrating segment {}", full_path);
                auto callback =
                  [this, full_path](
                    uint64_t size_bytes,
                    ss::input_stream<char> s) -> ss::future<uint64_t> {
                    co_await _cache.put(full_path, s).finally([&s] {
                        return s.close();
                    });
                    co_return size_bytes;
                };
                retry_chain_node local_rtc(
                  cache_hydration_timeout, cache_hydration_backoff, &_rtc);
                auto res = co_await _api.download_segment(
                  _bucket, _path, _manifest, callback, local_rtc);
                if (res != download_result::success) {
                    vlog(
                      _ctxlog.debug,
                      "Failed to hydrating a segment {}, {} waiter will be "
                      "invoked",
                      full_path,
                      _wait_list.size());
                    err = std::make_exception_ptr(
                      download_exception(res, full_path));
                }
            } break;
            }
            while (!_wait_list.empty()) {
                auto& p = _wait_list.front();
                if (err) {
                    p.set_exception(err);
                } else {
                    p.set_value(full_path);
                }
                _wait_list.pop_front();
            }
        }
    } catch (const ss::broken_condition_variable&) {
        vlog(_ctxlog.info, "Hydraton loop is stopped");
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Error in hydraton loop: {}",
          std::current_exception());
    }
}

ss::future<std::filesystem::path> remote_segment::hydrate() {
    return ss::with_gate(_gate, [this] {
        auto full_path = _manifest.get_remote_segment_path(_path);
        vlog(_ctxlog.debug, "segment {} hydration requested", full_path);
        ss::promise<std::filesystem::path> p;
        auto fut = p.get_future();
        _wait_list.push_back(std::move(p), ss::lowres_clock::time_point::max());
        _bg_cvar.signal();
        return fut;
    });
}

/// Batch consumer that connects to remote_segment_batch_reader.
/// It also does offset translation based on incomplete data in
/// manifests.
/// The implementation assumes that the config has kafka offsets
/// and does conversion based on that.
/// The problem is that we don't have full information regarding
/// offset translation in manifests. Because of that we can only
/// translate base_offset of every segment precisely. All other
/// offsets have to rely on state that this batch consumer maintains
/// while scanning the segment. This is not a problem since we
/// always have to scan the segments from the begining in shadow
/// indexing (the indexing to be implemented in the future). So
/// we will always be reusing an existing segment reader (with
/// data necessary for offset translation already present) or we
/// will start from the begining of the segment.
///
/// This consumer expects config.start_offset/max_offset to be
/// kafka offsets. It also returns batches with kafka offsets.
/// The log output always contains redpanda offsets unless the
/// annotation is added.
///
/// Note that the state that this consumer has can only be used
/// to translate current record batch.
class remote_segment_batch_consumer : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    remote_segment_batch_consumer(
      log_reader_config& conf,
      remote_segment_batch_reader& parent,
      model::term_id term,
      model::offset initial_delta,
      const model::ntp& ntp,
      retry_chain_node& rtc)
      : _config(conf)
      , _parent(parent)
      , _term(term)
      , _delta(initial_delta)
      , _rtc(&rtc)
      , _ctxlog(cst_log, _rtc, ntp.path()) {}

    /// Translate redpanda offset to kafka offset
    ///
    /// \note this can only be applied to current record batch
    model::offset rp_to_kafka(model::offset k) const noexcept {
        vassert(
          k >= _delta,
          "Redpanda offset {} is smaller than the delta {}",
          k,
          _delta);
        return k - _delta;
    }

    /// Translate kafka offset to redpanda offset
    ///
    /// \note this can only be applied to current record batch
    model::offset kafka_to_rp(model::offset k) const noexcept {
        return k + _delta;
    }

    /// Point config.start_offset to the next record batch
    ///
    /// \param header is a record batch header with redpanda offset
    /// \note this can only be applied to current record batch
    void
    advance_config_offsets(const model::record_batch_header& header) noexcept {
        if (header.type == model::record_batch_type::raft_data) {
            auto next = rp_to_kafka(header.last_offset()) + model::offset(1);
            if (next > _config.start_offset) {
                _config.start_offset = next;
            }
        }

        auto next_rp = header.last_offset() + model::offset{1};
        if (next_rp > _config.next_offset_redpanda) {
            _config.next_offset_redpanda = next_rp;
        }
    }

    consume_result accept_batch_start(
      const model::record_batch_header& header) const override {
        vlog(
          _ctxlog.trace,
          "accept_batch_start {}, current delta: {}",
          header,
          _delta);

        if (rp_to_kafka(header.base_offset) > _config.max_offset) {
            vlog(
              _ctxlog.debug,
              "accept_batch_start stop parser because {} > {}(kafka offset)",
              header.base_offset(),
              _config.max_offset);
            return batch_consumer::consume_result::stop_parser;
        }

        // Ignore filter and always return only raft_data since there is only
        // one usecase for this reader and the offset translation logic can only
        // handle this scenario anyway.
        if (model::record_batch_type::raft_data != header.type) {
            vlog(
              _ctxlog.debug,
              "accept_batch_start skip because record batch type is {}",
              header.type);
            return batch_consumer::consume_result::skip_batch;
        }

        // The segment can be scanned from the begining so we should skip
        // irrelevant batches.
        if (unlikely(
              rp_to_kafka(header.last_offset()) < _config.start_offset)) {
            vlog(
              _ctxlog.debug,
              "accept_batch_start skip because "
              "last_kafka_offset {} (last_rp_offset: {}) < "
              "config.start_offset: {}",
              rp_to_kafka(header.last_offset()),
              header.last_offset(),
              _config.start_offset);
            return batch_consumer::consume_result::skip_batch;
        }

        if (
          (_config.strict_max_bytes || _config.bytes_consumed)
          && (_config.bytes_consumed + header.size_bytes) > _config.max_bytes) {
            vlog(_ctxlog.debug, "accept_batch_start stop because overbudget");
            _config.over_budget = true;
            return batch_consumer::consume_result::stop_parser;
        }

        if (_config.first_timestamp > header.first_timestamp) {
            // kakfa needs to guarantee that the returned record is >=
            // first_timestamp
            vlog(
              _ctxlog.debug,
              "accept_batch_start skip because header timestamp is {}",
              header.first_timestamp);
            return batch_consumer::consume_result::skip_batch;
        }
        // we want to consume the batch
        return batch_consumer::consume_result::accept_batch;
    }

    /// Consume batch start
    void consume_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        vlog(
          _ctxlog.trace,
          "consume_batch_start called for {}",
          header.base_offset);
        _header = header;
        _header.ctx.term = _term;
    }

    /// Skip batch (called if accept_batch_start returned 'skip')
    void skip_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        // NOTE: that advance_config_start_offset should be called before
        // changing the _delta. The _delta that is be used for current record
        // batch can only account record batches in all previous batches.
        vlog(
          _ctxlog.debug, "skip_batch_start called for {}", header.base_offset);
        advance_config_offsets(header);
        if (header.type != model::record_batch_type::raft_data) {
            _delta += header.last_offset_delta + model::offset{1};
        }
    }

    void consume_records(iobuf&& ib) override { _records = std::move(ib); }

    /// Produce batch if within memory limits
    stop_parser consume_batch_end() override {
        auto batch = model::record_batch{
          _header, std::move(_records), model::record_batch::tag_ctor_ng{}};

        _config.bytes_consumed += batch.size_bytes();
        advance_config_offsets(batch.header());

        // NOTE: we need to translate offset of the batch after we updated
        // start offset of the config since it assumes that the header has
        // redpanda offset.
        batch.header().base_offset = rp_to_kafka(batch.base_offset());

        size_t sz = _parent.produce(std::move(batch));

        if (_config.over_budget) {
            return stop_parser::yes;
        }

        if (sz > max_consume_size) {
            return stop_parser::yes;
        }

        return stop_parser::no;
    }

    void print(std::ostream& o) const override {
        o << "remote_segment_batch_consumer";
    }

private:
    log_reader_config& _config;
    remote_segment_batch_reader& _parent;
    model::record_batch_header _header;
    iobuf _records;
    model::term_id _term;
    model::offset _delta;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

remote_segment_batch_reader::remote_segment_batch_reader(
  ss::lw_shared_ptr<remote_segment> s, const log_reader_config& config) noexcept
  : _seg(std::move(s))
  , _config(config)
  , _rtc(_seg->get_retry_chain_node())
  , _ctxlog(cst_log, _rtc, _seg->get_ntp().path())
  , _initial_delta(_seg->get_base_offset_delta()) {}

ss::future<result<ss::circular_buffer<model::record_batch>>>
remote_segment_batch_reader::read_some(
  model::timeout_clock::time_point deadline) {
    if (_ringbuf.empty()) {
        if (!_parser) {
            _parser = co_await init_parser();
        }
        auto bytes_consumed = co_await _parser->consume();
        if (!bytes_consumed) {
            co_return bytes_consumed.error();
        }
    }
    _total_size = 0;
    co_return std::move(_ringbuf);
}

ss::future<std::unique_ptr<storage::continuous_batch_parser>>
remote_segment_batch_reader::init_parser() {
    vlog(_ctxlog.debug, "remote_segment_batch_reader::init_parser");
    auto stream = co_await _seg->data_stream(
      0, priority_manager::local().shadow_indexing_priority());
    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<remote_segment_batch_consumer>(
        _config,
        *this,
        _seg->get_term(),
        _initial_delta,
        _seg->get_ntp(),
        *_seg->get_retry_chain_node()),
      std::move(stream));
    co_return parser;
}

size_t remote_segment_batch_reader::produce(model::record_batch batch) {
    vlog(_ctxlog.debug, "remote_segment_batch_reader::produce");
    _total_size += batch.size_bytes();
    _ringbuf.push_back(std::move(batch));
    return _total_size;
}

ss::future<> remote_segment_batch_reader::stop() {
    vlog(_ctxlog.debug, "remote_segment_batch_reader::close");
    if (_parser) {
        vlog(
          _ctxlog.debug, "remote_segment_batch_reader::close - parser-close");
        return _parser->close();
    }
    return ss::now();
}

} // namespace cloud_storage
