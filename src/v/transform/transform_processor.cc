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

processor::processor(
  model::transform_id id,
  model::ntp ntp,
  model::transform_metadata meta,
  std::unique_ptr<wasm::engine> engine,
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
  , _task(ss::now())
  , _logger(tlog, ss::format("{}/{}", meta.name(), _ntp.tp.partition)) {
    vassert(
      _sinks.size() == 1,
      "expected only a single sink, got: {}",
      _sinks.size());
}

ss::future<> processor::start() {
    try {
        co_await _engine->start();
        co_await _engine->initialize();
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error starting processor engine: {}", ex);
        _error_callback(_id, _ntp, _meta);
    }
    _task = run_transform_loop();
}

ss::future<> processor::stop() {
    _as.request_abort();
    co_await std::exchange(_task, ss::now());
    co_await _engine->stop();
}

ss::future<> processor::run_transform_loop() {
    try {
        co_await do_run_transform_loop();
    } catch (const ss::abort_requested_exception&) {
        // do nothing, we wanted to stop.
    } catch (...) {
        vlog(
          _logger.warn,
          "error running processor: {}",
          std::current_exception());
        _error_callback(_id, _ntp, _meta);
    }
}
ss::future<> processor::do_run_transform_loop() {
    auto offset = co_await _source->load_latest_offset();
    vlog(_logger.trace, "starting at offset {}", offset);
    // TODO(rockwood): Optimize this loop, we should not wait for the transform
    // or for writing before attempting to read more data.
    while (!_as.abort_requested()) {
        auto reader = co_await _source->read_batch(offset, &_as);
        auto batches = co_await model::consume_reader_to_memory(
          std::move(reader), model::no_timeout);
        if (batches.empty()) {
            vlog(
              _logger.trace,
              "received no results, sleeping before polling at offset {}",
              offset);
            constexpr auto fallback_poll_interval = std::chrono::seconds(1);
            simple_time_jitter<ss::lowres_clock> jitter(fallback_poll_interval);
            try {
                co_await ss::sleep_abortable<ss::lowres_clock>(
                  jitter.next_duration(), _as);
            } catch (const ss::sleep_aborted&) {
                // do nothing, the next iteration of the loop will exit
            }
            continue;
        }
        offset = model::next_offset(batches.back().last_offset());
        vlog(_logger.trace, "consumed upto offset {}", offset);
        ss::chunked_fifo<model::record_batch> transformed_batches;
        transformed_batches.reserve(batches.size());
        for (auto& batch : batches) {
            _probe->increment_read_bytes(batch.size_bytes());
            auto transformed = co_await _engine->transform(
              std::move(batch), _probe);
            _probe->increment_write_bytes(transformed.size_bytes());
            transformed_batches.push_back(std::move(transformed));
        }
        co_await _sinks[0]->write(std::move(transformed_batches));
    }
}

model::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }
} // namespace transform
