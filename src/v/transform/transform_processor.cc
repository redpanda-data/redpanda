/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "transform/transform_processor.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/simple_time_jitter.h"
#include "transform/logger.h"
#include "wasm/api.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>

#include <optional>

namespace transform {

namespace {

/**
 * A specific exception to throw on processor::stop so that we're not accidently
 * swallowing unexpected exceptions.
 */
class processor_shutdown_exception : public std::exception {
    const char* what() const noexcept override {
        return "processor shutting down";
    }
};

} // namespace

processor::processor(
  model::transform_id id,
  model::ntp ntp,
  model::transform_metadata meta,
  ss::shared_ptr<wasm::engine> engine,
  error_callback cb,
  std::unique_ptr<source> source,
  std::vector<std::unique_ptr<sink>> sinks,
  probe* p)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _source(std::move(source))
  , _sinks(std::move(sinks))
  , _error_callback(std::move(cb))
  , _probe(p)
  , _consumer_transform_pipe(1)
  , _transform_producer_pipe(1)
  , _task(ss::now())
  , _logger(tlog, ss::format("{}/{}", _meta.name(), _ntp.tp.partition())) {
    vassert(
      _sinks.size() == 1,
      "expected only a single sink, got: {}",
      _sinks.size());
}

ss::future<> processor::start() {
    try {
        co_await _engine->start();
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error starting processor engine: {}", ex);
        _error_callback(_id, _ntp, _meta);
    }
    _task = when_all_shutdown(
      run_consumer_loop(), run_transform_loop(), run_producer_loop());
}

ss::future<> processor::stop() {
    auto ex = std::make_exception_ptr(processor_shutdown_exception());
    _as.request_abort_ex(ex);
    _consumer_transform_pipe.abort(ex);
    _transform_producer_pipe.abort(ex);
    co_await std::exchange(_task, ss::now());
    co_await _engine->stop();
}

ss::future<> processor::poll_sleep() {
    constexpr auto fallback_poll_interval = std::chrono::seconds(1);
    simple_time_jitter<ss::lowres_clock> jitter(fallback_poll_interval);
    try {
        co_await ss::sleep_abortable<ss::lowres_clock>(
          jitter.next_duration(), _as);
    } catch (const ss::sleep_aborted&) {
        // do nothing, the caller will handle exiting properly.
    }
}

ss::future<> processor::run_consumer_loop() {
    auto offset = co_await _source->load_latest_offset();
    vlog(_logger.trace, "starting at offset {}", offset);
    while (!_as.abort_requested()) {
        auto reader = co_await _source->read_batch(offset, &_as);
        auto batches = co_await model::consume_reader_to_memory(
          std::move(reader), model::no_timeout);
        if (batches.empty()) {
            vlog(
              _logger.trace,
              "received no results, sleeping before polling at offset {}",
              offset);
            co_await poll_sleep();
            continue;
        }
        offset = model::next_offset(batches.back().last_offset());
        vlog(_logger.trace, "consumed up to offset {}", offset);
        for (auto& batch : batches) {
            _probe->increment_read_bytes(batch.size_bytes());
            co_await _consumer_transform_pipe.push_eventually(std::move(batch));
        }
    }
}

ss::future<> processor::run_transform_loop() {
    while (!_as.abort_requested()) {
        auto batch = co_await _consumer_transform_pipe.pop_eventually();
        batch = co_await _engine->transform(std::move(batch), _probe);
        co_await _transform_producer_pipe.push_eventually(std::move(batch));
    }
}

ss::future<> processor::run_producer_loop() {
    while (!_as.abort_requested()) {
        auto batch = co_await _transform_producer_pipe.pop_eventually();
        _probe->increment_write_bytes(batch.size_bytes());
        ss::chunked_fifo<model::record_batch> batches;
        batches.push_back(std::move(batch));
        co_await _sinks[0]->write(std::move(batches));
    }
}

template<typename... T>
ss::future<> processor::when_all_shutdown(T&&... futs) {
    return ss::when_all_succeed(handle_run_loop(std::forward<T>(futs))...)
      .discard_result();
}

ss::future<> processor::handle_run_loop(ss::future<> fut) {
    try {
        co_await std::move(fut);
    } catch (const processor_shutdown_exception&) {
        // Do nothing, this is an expected error on shutdown
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform: {}", ex);
        _error_callback(_id, _ntp, _meta);
    }
}

model::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }
const model::transform_metadata& processor::meta() const { return _meta; }
bool processor::is_running() const { return !_task.available(); }
} // namespace transform
