/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/tests/scheduler_fixture.h"

#include "datalake/logger.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "test_utils/randoms.h"

#include <seastar/util/defer.hh>

using namespace std::chrono_literals;

namespace datalake::translation::scheduling {

model::ntp scheduler_fixture::make_ntp() {
    return model::ntp{
      model::ns("kafka"),
      model::topic("test"),
      model::partition_id(_partition_counter++)};
}

scheduler_fixture::translator_spec scheduler_fixture::make_random_spec() {
    return {
      .max_target_lag = random_generators::random_choice(
        {large_target_lag, medium_target_lag, small_lag_duration}),
      .translation_throughput_bytes_per_sec = random_generators::random_choice(
        {large_translation_throughput,
         medium_translation_throughput,
         small_writers_per_translator}),
      .num_concurrent_writers = random_generators::random_choice(
        {large_concurrent_writers,
         medium_concurrent_writers,
         small_writers_per_translator})};
}

scheduler_fixture::translator_spec scheduler_fixture::make_default_spec() {
    return {
      .max_target_lag = medium_target_lag,
      .translation_throughput_bytes_per_sec = small_partition_throughput,
      .num_concurrent_writers = small_writers_per_translator};
}

size_t mock_translator::total_bytes_reserved() const {
    size_t result = 0;
    for (auto& writer : _writers) {
        result += writer.total_bytes_reserved;
    }
    return result;
}

ss::future<> mock_translator::flush_writers() {
    for (auto& writer : _writers) {
        writer.flush();
    }
    return ss::now();
}

ss::future<> mock_translator::maybe_checkpoint() {
    co_await flush_writers();
    auto now = clock::now();
    if (_finish_now || now >= _next_checkpoint) {
        // time to checkpoint
        co_await ss::sleep(100ms);
        // Begin of new interval
        _next_checkpoint = clock::now() + _max_target_lag;
        _last_finish_time = clock::now();
        _finish_now = false;
    }
}

ss::future<> mock_translator::mock_partition_writer::write(
  size_t bytes, ss::abort_source& as) {
    while (remaining_free_bytes < bytes) {
        auto result = co_await mem_tracker->reserve_memory(as);
        remaining_free_bytes += result.count();
        total_bytes_reserved += result.count();
        memory_units.push_back(std::move(result));
    }
    remaining_free_bytes -= bytes;
    total_bytes_translated += bytes;
}

void mock_translator::mock_partition_writer::flush() {
    total_bytes_reserved = 0;
    remaining_free_bytes = 0;
    memory_units.clear();
}

void mock_translator::mock_partition_writer::checkpoint() {
    flush();
    checkpointed_files.push_back(total_bytes_translated);
    total_bytes_translated = 0;
}

mock_translator::mock_translator(
  model::ntp ntp,
  clock::duration max_target_lag,
  size_t throughput_bytes_per_sec,
  size_t num_writers)
  : _ntp(std::move(ntp))
  , _max_target_lag(max_target_lag)
  , _translation_tput_bytes_per_sec(throughput_bytes_per_sec)
  , _num_writers(num_writers) {
    _translation_timer.set_callback([this] {
        if (_translation_state) {
            _translation_state->as.request_abort();
        }
    });
}

const translator_id& mock_translator::id() const { return _ntp; }

ss::future<> mock_translator::init(
  scheduling_notifications& notifier, reservations_tracker& mem_tracker) {
    auto holder = _gate.hold();
    _notifier = &notifier;
    _mem_tracker = &mem_tracker;
    for (size_t i = 0; i < _num_writers; i++) {
        _writers.push_back(mock_partition_writer{.mem_tracker = _mem_tracker});
    }
    _started = true;
    _next_checkpoint = clock::now() + _max_target_lag;
    ssx::spawn_with_gate(_gate, [this]() { return translation_loop(); });
    co_return;
}

ss::future<> mock_translator::close() noexcept {
    auto f = _gate.close();
    _wait_for_scheduler_cb.broken();
    _as.request_abort();
    _translation_timer.cancel();
    if (_translation_state) {
        vlog(
          datalake_log.debug,
          "[{}] request to stop translator, aborting translation",
          _ntp);
        _translation_state->as.request_abort();
    }
    co_return co_await std::move(f);
}

ss::future<> mock_translator::notify_ready() {
    // mock wait for enough data to accumulate.
    co_await ss::sleep_abortable(
      std::chrono::milliseconds{random_generators::get_int(10, 25)}, _as);
    _notifier->notify_ready(id());
}

ss::future<> mock_translator::notify_done() {
    co_await ss::sleep(100ms);
    _notifier->notify_done(id());
}

ss::future<> mock_translator::translation_loop() {
    auto holder = _gate.hold();
    while (!_gate.is_closed() && !_as.abort_requested()) {
        co_await notify_ready();
        {
            co_await _wait_for_scheduler_cb.wait(
              [this] { return _translation_state.has_value(); });
            auto holder = _translation_state->gate.hold();
            auto deadline = _translation_state->translate_for;
            auto start_time = clock::now();
            _translation_state->start_time = start_time;
            _translation_timer.rearm(start_time + deadline);
            static constexpr auto iteration_duration = 100ms;
            auto tput_per_iteration = _translation_tput_bytes_per_sec
                                      / iteration_duration.count();
            // simulate the case where the translation runs quickly.
            // and does not use the full window.
            auto adjustment_factor = random_generators::get_real(0.5, 0.8);
            // Factor in any checkpointing and reserve 20% time to it.
            auto adjusted_deadline = _translation_state->start_time
                                     + adjustment_factor * deadline;
            vlog(
              datalake_log.trace,
              "[{}], starting translation, quota: {} adjusted_quota_ms : "
              "{}",
              id(),
              std::chrono::duration_cast<std::chrono::milliseconds>(deadline),
              std::chrono::duration_cast<std::chrono::milliseconds>(
                0.8f * deadline));
            try {
                while (!_translation_state->as.abort_requested()
                       && clock::now() < adjusted_deadline) {
                    // split the iteration randomly among writers
                    chunked_vector<ss::future<>> writers;
                    auto remaining = tput_per_iteration;
                    for (size_t i = 0; i < _num_writers; i++) {
                        auto writer_bytes = random_generators::get_int<size_t>(
                          0, remaining);
                        writers.push_back(_writers[i].write(
                          writer_bytes, _translation_state->as));
                        remaining -= writer_bytes;
                    }
                    co_await ss::when_all_succeed(
                      writers.begin(), writers.end());
                    co_await ss::sleep_abortable(100ms, _translation_state->as);
                }
                _translation_timer.cancel();
            } catch (...) {
                // log details
            }
        }

        try {
            co_await maybe_checkpoint();
        } catch (...) {
            // log details
        }
        _translation_state.reset();
        co_await notify_done();
        vlog(datalake_log.trace, "[{}], ending translation", id());
    }
}

void mock_translator::start_translation(clock::duration translate_for) {
    auto holder = _gate.hold();
    vassert(_started, "Translator should be started first");
    vassert(!_translation_state, "Translation already in progress");
    _translation_state = mock_inflight_translation_state{};
    _translation_state->translate_for = translate_for;
    _wait_for_scheduler_cb.signal();
}

translation_status mock_translator::status() const {
    translation_status status;
    status.target_lag = _max_target_lag;
    status.next_checkpoint_deadline = _next_checkpoint;
    if (_translation_state) {
        status.memory_bytes_reserved = total_bytes_reserved();
        status.disk_bytes_flushed = total_bytes_reserved();
    }
    status.last_finish_time = _last_finish_time;
    return status;
}

void mock_translator::stop_translation(translator::stop_request request) {
    auto holder = _gate.hold();
    vassert(_started, "Translator should be started first");
    if (!_translation_state) {
        return;
    }
    _finish_now = request.cleanup_staged_data;
    vlog(datalake_log.debug, "[{}] request to stop translation", _ntp);
    _translation_state->as.request_abort();
    ssx::spawn_with_gate(
      _gate, [this]() { return _translation_state->gate.close(); });
}

void delaying_translator::start_translation(clock::duration deadline) {
    auto min_wait = std::chrono::duration_cast<clock::duration>(10s);
    mock_translator::start_translation(std::max(3 * deadline, min_wait));
}

ss::future<> exceptional_translator::init(
  scheduling_notifications& notifications, reservations_tracker& reservations) {
    if (tests::random_bool()) {
        throw ss::gate_closed_exception();
    }
    return mock_translator::init(notifications, reservations);
}

void exceptional_translator::start_translation(clock::duration translate_for) {
    if (tests::random_bool()) {
        throw ss::gate_closed_exception();
    }
    return mock_translator::start_translation(translate_for);
}

void exceptional_translator::stop_translation(
  translator::stop_request stop_request) {
    if (tests::random_bool()) {
        throw ss::gate_closed_exception();
    }
    return mock_translator::stop_translation(stop_request);
}

std::unique_ptr<translator>
scheduler_fixture::make_normal_translator(translator_spec spec) {
    return std::make_unique<mock_translator>(
      make_ntp(),
      spec.max_target_lag,
      spec.translation_throughput_bytes_per_sec,
      spec.num_concurrent_writers);
}

std::unique_ptr<translator>
scheduler_fixture::make_delaying_translator(translator_spec spec) {
    return std::make_unique<delaying_translator>(
      make_ntp(),
      spec.max_target_lag,
      spec.translation_throughput_bytes_per_sec,
      spec.num_concurrent_writers);
}

std::unique_ptr<translator>
scheduler_fixture::make_exceptional_translator(translator_spec spec) {
    return std::make_unique<exceptional_translator>(
      make_ntp(),
      spec.max_target_lag,
      spec.translation_throughput_bytes_per_sec,
      spec.num_concurrent_writers);
}

std::unique_ptr<translator>
scheduler_fixture::make_random_translator(translator_spec spec) {
    switch (random_generators::get_int<int>() % 3) {
    case 0:
        return make_normal_translator(spec);
    case 1:
        return make_delaying_translator(spec);
    case 2:
        return make_exceptional_translator(spec);
    default:
        return make_normal_translator(spec);
    }
}
} // namespace datalake::translation::scheduling
