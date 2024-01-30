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

#include "base/units.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "prometheus/prometheus_sanitize.h"
#include "random/simple_time_jitter.h"
#include "ssx/future-util.h"
#include "transform/logger.h"
#include "wasm/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/variant_utils.hh>

#include <algorithm>
#include <iterator>
#include <optional>

namespace transform {

namespace {

class queue_output_consumer {
    static constexpr size_t buffer_chunk_size = 8;

public:
    queue_output_consumer(
      transfer_queue<model::record_batch, buffer_chunk_size>* output,
      ss::abort_source* as,
      probe* probe)
      : _output(output)
      , _as(as)
      , _probe(probe) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        // This is a "safe" cast as all our offsets come from the translating
        // reader, but we don't have a seperate type for kafka::record_batch vs
        // model::record_batch.
        _last_offset = model::offset_cast(b.last_offset());
        _probe->increment_read_bytes(b.size_bytes());
        co_await _output->push(std::move(b), _as);
        co_return ss::stop_iteration::no;
    }
    std::optional<kafka::offset> end_of_stream() const { return _last_offset; }

private:
    std::optional<kafka::offset> _last_offset;
    transfer_queue<model::record_batch, buffer_chunk_size>* _output;
    ss::abort_source* _as;
    probe* _probe;
};

struct drain_result {
    ss::chunked_fifo<model::record_batch> batches;
    kafka::offset latest_offset;
};

/**
 * A specific exception to throw on processor::stop so that we're not accidently
 * swallowing unexpected exceptions.
 */
class processor_shutdown_exception : public std::exception {
    const char* what() const noexcept override {
        return "processor shutting down";
    }
};

// TODO(rockwood): This is an arbitrary value, we should instead be
// limiting the size based on the amount of memory in the transform subsystem.
constexpr size_t max_buffer_size = 64_KiB;

} // namespace

processor::processor(
  model::transform_id id,
  model::ntp ntp,
  model::transform_metadata meta,
  ss::shared_ptr<wasm::engine> engine,
  state_callback cb,
  std::unique_ptr<source> source,
  std::vector<std::unique_ptr<sink>> sinks,
  std::unique_ptr<offset_tracker> offset_tracker,
  probe* p)
  : _id(id)
  , _ntp(std::move(ntp))
  , _meta(std::move(meta))
  , _engine(std::move(engine))
  , _source(std::move(source))
  , _offset_tracker(std::move(offset_tracker))
  , _state_callback(std::move(cb))
  , _probe(p)
  , _consumer_transform_pipe(max_buffer_size)
  , _outputs()
  , _task(ss::now())
  , _logger(tlog, ss::format("{}/{}", _meta.name(), _ntp.tp.partition())) {
    vassert(
      sinks.size() == 1, "expected only a single sink, got: {}", sinks.size());
    const auto& outputs = _meta.output_topics;
    vassert(
      outputs.size() == sinks.size(),
      "expected the same number of output topics and sinks");
    _outputs.reserve(outputs.size());
    for (size_t i : boost::irange(outputs.size())) {
        _outputs.emplace(
          outputs[i].tp,
          output{
            .queue = transfer_queue<transformed_output>(max_buffer_size),
            .sink = std::move(sinks[i])});
    }
    // The processor needs to protect against multiple stops (or calling stop
    // before start), to do that we use the state in _as. Start out with an
    // abort so that we handle being stopped before we're started.
    _as.request_abort_ex(processor_shutdown_exception());
}

ss::future<> processor::start() {
    // Don't allow double starts of this module - we use the abort_requested
    // flag to determine if the module is "running" or not.
    if (!_as.abort_requested()) {
        co_return;
    }
    _as = {};
    co_await _source->start();
    co_await _offset_tracker->start();
    _task = handle_processor_task(_engine->start().then([this] {
        return load_start_offset().then([this](kafka::offset start_offset) {
            // Mark that we're running now that the start offset is loaded.
            _state_callback(_id, _ntp, state::running);
            return when_all_shutdown(
              run_consumer_loop(start_offset),
              run_transform_loop(),
              run_all_producers());
        });
    }));
}

ss::future<> processor::stop() {
    // We reset the abort source when being started, so protect against double
    // stops by checking if we've already requested being stopped.
    if (_as.abort_requested()) {
        co_return;
    }
    auto ex = std::make_exception_ptr(processor_shutdown_exception());
    _as.request_abort_ex(ex);
    co_await std::exchange(_task, ss::now());
    _consumer_transform_pipe.clear();
    for (auto& [_, output] : _outputs) {
        output.queue.clear();
    }
    co_await _source->stop();
    co_await _offset_tracker->stop();
    co_await _engine->stop();
    // reset lag now that we've stopped
    report_lag(0);
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

ss::future<kafka::offset> processor::load_start_offset() {
    co_await _offset_tracker->wait_for_previous_flushes(&_as);
    auto latest_committed = co_await _offset_tracker->load_committed_offset();
    auto latest = _source->latest_offset();
    if (!latest_committed) {
        // If we have never committed, mark the end of the log as our starting
        // place, and start processing from the next record that is produced.
        co_await _offset_tracker->commit_offset(latest);
        co_return kafka::next_offset(latest);
    }
    // The latest record is inclusive of the last record, so we want to start
    // reading from the following record.
    auto last_processed_offset = latest_committed.value();
    if (
      latest != kafka::offset::min()
      && last_processed_offset == kafka::offset::min()) {
        // In cases where we committed the start of the log without any records,
        // then the log has added records, we will overflow computing
        // small_offset - min_offset. Instead normalize last processed to -1 so
        // that the computed lag is correct (these ranges are inclusive).
        // For example: latest(1) - last_processed(-1) = lag(2)
        last_processed_offset = kafka::offset(-1);
    }
    report_lag(latest - last_processed_offset);
    co_return kafka::next_offset(last_processed_offset);
}

ss::future<> processor::run_consumer_loop(kafka::offset offset) {
    vlog(_logger.debug, "starting at offset {}", offset);
    while (!_as.abort_requested()) {
        auto reader = co_await _source->read_batch(offset, &_as);
        auto last_offset = co_await std::move(reader).consume(
          queue_output_consumer(&_consumer_transform_pipe, &_as, _probe),
          model::no_timeout);
        if (!last_offset) {
            vlog(
              _logger.trace,
              "received no results, sleeping before polling at offset {}",
              offset);
            co_await poll_sleep();
            continue;
        }
        offset = kafka::next_offset(*last_offset);
        vlog(_logger.trace, "consumed up to offset {}", offset);
    }
}

ss::future<> processor::run_transform_loop() {
    while (!_as.abort_requested()) {
        auto batch = co_await _consumer_transform_pipe.pop_one(&_as);
        if (!batch) {
            continue;
        }
        auto offset = model::offset_cast(batch->last_offset());
        ss::chunked_fifo<model::transformed_data> transformed;
        vlog(_logger.trace, "transforming offset {}", offset);
        co_await _engine->transform(
          std::move(*batch),
          _probe,
          [this](
            std::optional<model::topic_view> topic,
            model::transformed_data data) {
              auto output_topic = topic.value_or(
                model::topic_view(_meta.output_topics.front().tp));
              auto it = _outputs.find(output_topic);
              if (it == _outputs.end()) {
                  return ssx::now(wasm::write_success::no);
              }
              return it->second.queue.push({std::move(data)}, &_as).then([] {
                  return wasm::write_success::yes;
              });
          });
        vlog(_logger.trace, "transformed offset {}", offset);
        // Mark all queues as processsed up to this point.
        for (auto& [_, output] : _outputs) {
            co_await output.queue.push({offset}, &_as);
        }
    }
}

ss::future<> processor::run_all_producers() {
    std::vector<ss::future<>> futures;
    for (auto& [topic, output] : _outputs) {
        futures.push_back(run_producer_loop(&output.queue, output.sink.get()));
    }
    return ss::when_all_succeed(
      std::make_move_iterator(futures.begin()),
      std::make_move_iterator(futures.end()));
}

ss::future<> processor::run_producer_loop(
  transfer_queue<transformed_output>* queue, sink* sink) {
    while (!_as.abort_requested()) {
        auto popped = co_await queue->pop_all(&_as);
        if (popped.empty()) {
            continue;
        }
        kafka::offset latest_offset;
        ss::chunked_fifo<model::transformed_data> datas;
        for (auto& entry : popped) {
            ss::visit(
              entry.data,
              [&datas](model::transformed_data& b) {
                  datas.push_back(std::move(b));
              },
              [&latest_offset](kafka::offset o) { latest_offset = o; });
        }
        ss::chunked_fifo<model::record_batch> batches;
        if (!datas.empty()) {
            // TODO(rockwood): Limit batch sizes so we don't overshoot
            // max batch size limits.
            batches.push_back(model::transformed_data::make_batch(
              model::timestamp::now(), std::move(datas)));
        }
        co_await sink->write(std::move(batches));
        if (latest_offset != kafka::offset()) {
            co_await _offset_tracker->commit_offset(latest_offset);
            report_lag(_source->latest_offset() - latest_offset);
        }
    }
}

template<typename... T>
ss::future<> processor::when_all_shutdown(T&&... futs) {
    return ss::when_all_succeed(handle_processor_task(std::forward<T>(futs))...)
      .discard_result();
}

ss::future<> processor::handle_processor_task(ss::future<> fut) {
    try {
        co_await std::move(fut);
    } catch (const processor_shutdown_exception&) {
        // Do nothing, this is an expected error on shutdown
    } catch (const std::exception& ex) {
        vlog(_logger.warn, "error running transform: {}", ex);
        _state_callback(_id, _ntp, state::errored);
    }
}

void processor::report_lag(int64_t lag) {
    int64_t delta = lag - _last_reported_lag;
    _probe->report_lag(delta);
    _last_reported_lag = lag;
}

model::transform_id processor::id() const { return _id; }
const model::ntp& processor::ntp() const { return _ntp; }
const model::transform_metadata& processor::meta() const { return _meta; }
bool processor::is_running() const {
    // Only mark this as running if we've called start without calling stop.
    return !_as.abort_requested();
}
int64_t processor::current_lag() const { return _last_reported_lag; }

size_t transformed_output::memory_usage() const {
    return sizeof(transformed_output)
           + ss::visit(
             data,
             [](const model::transformed_data& d) {
                 // Account for the size of this struct, but don't double count
                 // sizeof(model::transformed_data), which d.memory_usage()
                 // accounts for.
                 return d.memory_usage() - sizeof(model::transformed_data);
             },
             [](kafka::offset) { return size_t(0); });
}

} // namespace transform
