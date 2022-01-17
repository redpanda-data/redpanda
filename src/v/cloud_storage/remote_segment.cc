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
#include "config/configuration.h"
#include "model/fundamental.h"
#include "resource_mgmt/io_priority.h"
#include "storage/parser.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

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

inline void expiry_handler_impl(ss::promise<ss::file>& pr) {
    pr.set_exception(ss::timed_out_error());
}

remote_segment::remote_segment(
  remote& r,
  cache& c,
  s3::bucket_name bucket,
  const manifest& m,
  const manifest::key& name,
  retry_chain_node& parent)
  : _api(r)
  , _cache(c)
  , _bucket(std::move(bucket))
  , _ntp(m.get_ntp())
  , _rtc(&parent)
  , _ctxlog(cst_log, _rtc, get_ntp().path())
  , _wait_list(expiry_handler_impl) {
    auto meta = m.get(name);
    vassert(meta, "Can't find segment metadata in manifest, name: {}", name);

    _path = m.generate_segment_path(name, *meta);

    auto parsed_name = parse_segment_name(name);
    vassert(parsed_name, "Can't parse segment name, name: {}", name);
    _term = parsed_name->term;

    _base_rp_offset = meta->base_offset;
    _max_rp_offset = meta->committed_offset;
    _base_offset_delta = std::clamp(
      meta->delta_offset, model::offset(0), model::offset::max());

    // run hydration loop in the background
    (void)run_hydrate_bg();
}

const model::ntp& remote_segment::get_ntp() const { return _ntp; }

const model::offset remote_segment::get_max_rp_offset() const {
    return _max_rp_offset;
}

const model::offset remote_segment::get_base_offset_delta() const {
    return _base_offset_delta;
}

const model::offset remote_segment::get_base_rp_offset() const {
    return _base_rp_offset;
}

const model::offset remote_segment::get_base_kafka_offset() const {
    return _base_rp_offset - _base_offset_delta;
}

const model::term_id remote_segment::get_term() const { return _term; }

ss::future<> remote_segment::stop() {
    vlog(_ctxlog.debug, "remote segment stop");
    _bg_cvar.broken();
    co_await _gate.close();
    if (_data_file) {
        co_await _data_file.close().handle_exception(
          [this](std::exception_ptr err) {
              vlog(
                _ctxlog.error, "Error '{}' while closing the '{}'", err, _path);
          });
    }
}

ss::future<ss::input_stream<char>>
remote_segment::data_stream(size_t pos, ss::io_priority_class io_priority) {
    vlog(_ctxlog.debug, "remote segment file input stream at {}", pos);
    ss::gate::holder g(_gate);
    co_await hydrate();
    ss::file_input_stream_options options{};
    options.buffer_size = config::shard_local_cfg().storage_read_buffer_size();
    options.read_ahead
      = config::shard_local_cfg().storage_read_readahead_count();
    options.io_priority_class = io_priority;
    auto data_stream = ss::make_file_input_stream(
      _data_file, pos, std::move(options));
    co_return data_stream;
}

ss::future<> remote_segment::run_hydrate_bg() {
    ss::gate::holder guard(_gate);
    try {
        while (!_gate.is_closed()) {
            co_await _bg_cvar.wait(
              [this] { return !_wait_list.empty() || _gate.is_closed(); });
            vlog(
              _ctxlog.debug,
              "Segment {} requested, {} consumers are awaiting, data file is "
              "{}",
              _path,
              _wait_list.size(),
              _data_file ? "available" : "not available");
            std::exception_ptr err;
            if (!_data_file) {
                // We don't have a _data_file set so we have to check cache
                // and retrieve the file out of it or hydrate.
                // If _data_file is initialized we can use it safely since the
                // cache can't delete it until we close it.
                auto status = co_await _cache.is_cached(_path);
                switch (status) {
                case cache_element_status::in_progress:
                    vassert(
                      false,
                      "Hydration of segment {} is already in progress, {} "
                      "waiters",
                      _path,
                      _wait_list.size());
                case cache_element_status::available:
                    vlog(
                      _ctxlog.debug,
                      "Hydrated segment {} is already available, {} waiters "
                      "will "
                      "be invoked",
                      _path,
                      _wait_list.size());
                    break;
                case cache_element_status::not_available: {
                    vlog(_ctxlog.info, "Hydrating segment {}", _path);
                    auto callback =
                      [this](
                        uint64_t size_bytes,
                        ss::input_stream<char> s) -> ss::future<uint64_t> {
                        co_await _cache.put(_path, s).finally(
                          [&s] { return s.close(); });
                        co_return size_bytes;
                    };
                    retry_chain_node local_rtc(
                      cache_hydration_timeout, cache_hydration_backoff, &_rtc);
                    auto res = co_await _api.download_segment(
                      _bucket, _path, callback, local_rtc);
                    if (res != download_result::success) {
                        vlog(
                          _ctxlog.debug,
                          "Failed to hydrating a segment {}, {} waiter will be "
                          "invoked",
                          _path,
                          _wait_list.size());
                        err = std::make_exception_ptr(
                          download_exception(res, _path));
                    }
                } break;
                }
                if (!err) {
                    auto maybe_file = co_await _cache.get(_path);
                    if (!maybe_file) {
                        // We could got here because the cache check returned
                        // 'cache_element_status::available' but right after
                        // that the file was evicted from cache. It's also
                        // possible (but very unlikely) that we got here after
                        // successful hydration which was immediately followed
                        // by eviction. In any case we should just re-hydrate
                        // the segment. The 'wait' on cond-variable won't block
                        // because the
                        // '_wait_list' is not empty.
                        vlog(
                          _ctxlog.info,
                          "Segment {} was deleted from cache and need to be "
                          "re-hydrated, {} waiter are pending",
                          _path,
                          _wait_list.size());
                        continue;
                    }
                    _data_file = maybe_file->body;
                }
            }
            // Invariant: here we should have a data file or error to be set.
            // If the hydration failed we will have 'err' set to some value. The
            // error needs to be propagated further. Otherwise we will always
            // have _data_file set because if we don't we will retry the
            // hydration earlier.
            vassert(
              _data_file || err,
              "Segment hydration succeded but file isn't available");
            while (!_wait_list.empty()) {
                auto& p = _wait_list.front();
                if (err) {
                    p.set_exception(err);
                } else {
                    p.set_value(_data_file);
                }
                _wait_list.pop_front();
            }
        }
    } catch (const ss::broken_condition_variable&) {
        vlog(_ctxlog.debug, "Hydraton loop is stopped");
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Error in hydraton loop: {}",
          std::current_exception());
    }
}

ss::future<> remote_segment::hydrate() {
    return ss::with_gate(_gate, [this] {
        vlog(_ctxlog.debug, "segment {} hydration requested", _path);
        ss::promise<ss::file> p;
        auto fut = p.get_future();
        _wait_list.push_back(std::move(p), ss::lowres_clock::time_point::max());
        _bg_cvar.signal();
        return fut.discard_result();
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
      storage::log_reader_config& conf,
      remote_segment_batch_reader& parent,
      model::term_id term,
      const model::ntp& ntp,
      retry_chain_node& rtc)
      : _config(conf)
      , _parent(parent)
      , _term(term)
      , _rtc(&rtc)
      , _ctxlog(cst_log, _rtc, ntp.path()) {}

    /// Translate redpanda offset to kafka offset
    ///
    /// \note this can only be applied to current record batch
    model::offset rp_to_kafka(model::offset k) const noexcept {
        vassert(
          k >= _parent._cur_delta,
          "Redpanda offset {} is smaller than the delta {}",
          k,
          _parent._cur_delta);
        return k - _parent._cur_delta;
    }

    /// Translate kafka offset to redpanda offset
    ///
    /// \note this can only be applied to current record batch
    model::offset kafka_to_rp(model::offset k) const noexcept {
        return k + _parent._cur_delta;
    }

    /// Point config.start_offset to the next record batch
    ///
    /// \param header is a record batch header with redpanda offset
    /// \note this can only be applied to current record batch
    void
    advance_config_offsets(const model::record_batch_header& header) noexcept {
        _parent._cur_rp_offset = header.last_offset() + model::offset{1};

        if (header.type == model::record_batch_type::raft_data) {
            auto next = rp_to_kafka(header.last_offset()) + model::offset(1);
            if (next > _config.start_offset) {
                _config.start_offset = next;
            }
        }
    }

    consume_result accept_batch_start(
      const model::record_batch_header& header) const override {
        vlog(
          _ctxlog.trace,
          "accept_batch_start {}, current delta: {}",
          header,
          _parent._cur_delta);

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
        // changing the _cur_delta. The _cur_delta that is be used for current
        // record batch can only account record batches in all previous batches.
        vlog(
          _ctxlog.debug, "skip_batch_start called for {}", header.base_offset);
        advance_config_offsets(header);
        if (
          header.type == model::record_batch_type::raft_configuration
          || header.type == model::record_batch_type::archival_metadata) {
            vassert(
              _parent._cur_ot_state,
              "ntp {}: offset translator state for "
              "remote_segment_batch_consumer not initialized",
              _parent._seg->get_ntp());

            _parent._cur_ot_state->get().add_gap(
              header.base_offset, header.last_offset());
            vlog(
              _ctxlog.debug,
              "added offset translation gap [{}-{}], current state: {}",
              header.base_offset,
              header.last_offset(),
              _parent._cur_ot_state);

            _parent._cur_delta += header.last_offset_delta + model::offset{1};
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
    storage::log_reader_config& _config;
    remote_segment_batch_reader& _parent;
    model::record_batch_header _header;
    iobuf _records;
    model::term_id _term;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

remote_segment_batch_reader::remote_segment_batch_reader(
  ss::lw_shared_ptr<remote_segment> s,
  const storage::log_reader_config& config) noexcept
  : _seg(std::move(s))
  , _config(config)
  , _rtc(_seg->get_retry_chain_node())
  , _ctxlog(cst_log, _rtc, _seg->get_ntp().path())
  , _cur_rp_offset(_seg->get_base_rp_offset())
  , _cur_delta(_seg->get_base_offset_delta()) {}

ss::future<result<ss::circular_buffer<model::record_batch>>>
remote_segment_batch_reader::read_some(
  model::timeout_clock::time_point deadline,
  storage::offset_translator_state& ot_state) {
    ss::gate::holder h(_gate);
    if (_ringbuf.empty()) {
        if (!_parser) {
            // remote_segment_batch_reader shouldn't be used concurrently
            _parser = co_await init_parser();
        }

        if (ot_state.add_absolute_delta(_cur_rp_offset, _cur_delta)) {
            vlog(
              _ctxlog.debug,
              "offset translation: add_absolute_delta at offset {}, "
              "delta {}, current state: {}",
              _cur_rp_offset,
              _cur_delta,
              ot_state);
        }

        _cur_ot_state = ot_state;
        auto deferred = ss::defer([this] { _cur_ot_state = std::nullopt; });
        auto new_bytes_consumed = co_await _parser->consume();
        if (!new_bytes_consumed) {
            co_return new_bytes_consumed.error();
        }
        if (
          _bytes_consumed != 0 && _bytes_consumed == new_bytes_consumed.value()
          && !_config.over_budget) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "segment_reader is stuck, segment ntp: {}, _cur_rp_offset: {}, "
              "_bytes_consumed: "
              "{}",
              _seg->get_ntp(),
              _cur_rp_offset,
              _bytes_consumed));
        }
        _bytes_consumed = new_bytes_consumed.value();
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
        _config, *this, _seg->get_term(), _seg->get_ntp(), _rtc),
      std::move(stream));
    co_return parser;
}

size_t remote_segment_batch_reader::produce(model::record_batch batch) {
    ss::gate::holder h(_gate);
    vlog(_ctxlog.debug, "remote_segment_batch_reader::produce");
    _total_size += batch.size_bytes();
    _ringbuf.push_back(std::move(batch));
    return _total_size;
}

ss::future<> remote_segment_batch_reader::stop() {
    vlog(_ctxlog.debug, "remote_segment_batch_reader::stop");
    co_await _gate.close();
    if (_parser) {
        vlog(_ctxlog.debug, "remote_segment_batch_reader::stop - parser-close");
        co_await _parser->close();
        _parser.reset();
    }
    _stopped = true;
}

remote_segment_batch_reader::~remote_segment_batch_reader() noexcept {
    vassert(_stopped, "Destroyed without stopping");
}

} // namespace cloud_storage
