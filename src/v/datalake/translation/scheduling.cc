/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/logger.h"
#include "datalake/translation/scheduling_policies.h"
#include "ssx/future-util.h"
#include "utils/human.h"
#include "utils/to_string.h"

namespace datalake::translation::scheduling {

using namespace std::literals::chrono_literals;

class default_reservations_tracker : public reservations_tracker {
public:
    explicit default_reservations_tracker(
      size_t total_memory,
      size_t block_size,
      scheduling_notifications& notifier,
      disk_manager& disk_manager)
      : _total_memory((total_memory / block_size) * block_size)
      , _available_memory{_total_memory, "dl/translation/memory"}
      , _reservation_block_size(block_size)
      , _notifier(notifier)
      , _shard_available_disk{0, "dl/translation/disk"}
      , _disk_manager(disk_manager) {
        auto blocks = _total_memory / block_size;
        vassert(
          blocks > 0,
          "Atleast one block of memory is needed for translation to make "
          "progress, available memory: {}, block_size: {}",
          total_memory,
          block_size);
        vlog(
          datalake_log.info,
          "starting reservation tracker with available memory: {} bytes, block "
          "size: {} bytes",
          _total_memory,
          _reservation_block_size);
    }

    bool memory_exhausted() const override {
        auto units = _available_memory.available_units();
        vassert(
          units >= 0,
          "Invalid memory reservation state, reservations cannot exceed memory "
          "limit, current: {}",
          units);
        return static_cast<size_t>(units) < _reservation_block_size;
    }

    ss::future<reservation> reserve_memory(ss::abort_source& as) override {
        auto nr = _reservation_block_size;
        // fast path
        auto opt_units = ss::try_get_units(_available_memory, nr);
        if (opt_units) {
            co_return std::move(opt_units.value());
        }
        _notifier.notify_memory_exhausted();
        try {
            co_return co_await ss::get_units(_available_memory, nr, as);
        } catch (...) {
            // propagate the exception in abort source, if any
            as.check();
            // else, rethrow generic exception
            std::rethrow_exception(std::current_exception());
        }
    }

    size_t allocated_memory() const override {
        return _total_memory - _available_memory.available_units();
    }

    size_t reservation_block_size() const override {
        return _reservation_block_size;
    }

    /*
     * disk reservation overages are handled by requesting units from the global
     * disk manager, and then waiting for sufficient disk resources to become
     * available.
     */
    ss::future<reservation>
    reserve_disk(size_t, ss::abort_source& as) override {
        const auto init_backoff = 100ms;
        const auto max_backoff = 1000ms;

        /*
         * Having some block reservation acquired by each translator is nice
         * because even though the hot path for tiny, frequent allocations would
         * usually return an immediately ready future in this method, we'd still
         * be subject to seastar scheduler preemptions and these allocations are
         * made in the translation loop.
         *
         * However, it is much less clear what a good value is for the size of a
         * memory block reservation that a schedulers gives to a translator.
         * Currently we use the same size as the memory reservation (on the
         * order of single digit MBs). There are a few reasons for this. First,
         * even modestly sized reservations will reduce small allocations
         * round-trips to coroutine invocations and thus reduce preemption
         * checks for the translation loop.
         *
         * But the primary reasons for re-using the memory block size is
         * that (1) such reservations are unavailable to other translators so
         * choosing a smaller size allows more fine grained control and (2)
         * using a larger size is unlikely to provide any real value since disk
         * will generally be found in more abundance than memory, and both disk
         * and memory are decremented in lock step by the serde parquet reader.
         */
        const auto block_size = _reservation_block_size;

        auto backoff = init_backoff;
        while (true) {
            /*
             * first we attempt to acquire disk reservation from what is
             * available local on this shard. when a translator on this core
             * finishes it will return its units to this pool. we use up all of
             * the available units, so check that get_units returns a positive
             * value because when requesting 0 units it will be granted but the
             * system won't make progress.
             */
            auto opt_units = ss::try_get_units(
              _shard_available_disk,
              std::min(block_size, _shard_available_disk.current()));
            if (opt_units.has_value() && opt_units.value().count() > 0) {
                if (datalake_log.is_enabled(ss::log_level::trace)) {
                    vlog(
                      datalake_log.trace,
                      "Allocated {} disk reservation from shard-local pool. "
                      "Total "
                      "available {}",
                      human::bytes(opt_units.value().count()),
                      human::bytes(_shard_available_disk.current()));
                }
                co_return std::move(opt_units.value());
            }

            /*
             * if we are unable to acquire units locally, then pop on over
             * to core-0 and ask the disk manager if there are available
             * units that we can have. deposit any units locally and try
             * again. note that below we add a very short, intentional delay
             * to prevent accidentally spinning. we avoid immediately taking the
             * units and returning to avoid the possibility that the translators
             * on a core continually take units from each other rather than
             * giving the system an opportunity to redistribute the units. this
             * concern, however, is speculative.
             *
             * unlike the smaller block reserved for a translators, here we
             * will get back a much larger reservations, say 50mb or 100mb,
             * which helps reduce x-core communication. The size of the block of
             * memory reserved by a scheduler from the datalake manager is
             * governed by datalake_disk_reservation_block_size.
             */
            auto units = co_await _disk_manager.reserve();
            if (units > 0) {
                _shard_available_disk.signal(units);
                if (datalake_log.is_enabled(ss::log_level::debug)) {
                    vlog(
                      datalake_log.debug,
                      "Received {} disk reservation from global pool. Total "
                      "available {}",
                      human::bytes(units),
                      human::bytes(_shard_available_disk.current()));
                }
            }

            /*
             * for the case in which we are unable to get enough units to
             * continue we go into a polling loop where we again check
             * locally and then check with the disk manager. the reason that
             * this works is that the contract with the disk manager is such
             * that if it cannot satisfy a request for additional units that
             * it will actively work towards acquiring more units. this is
             * done by requesting translators to finish. the released disk
             * reservations which will first be available to the finishing
             * translator's core, and then to the global pool.
             */
            backoff = std::min(backoff, max_backoff);
            try {
                co_await ss::sleep_abortable(backoff, as);
            } catch (...) {
                as.check();
                std::rethrow_exception(std::current_exception());
            }
            backoff *= 2;

            /*
             * waiting until max backoff limits noise from normal retries, and
             * limiting the logging rate itself helps when multiple translators
             * are in this same retry loop.
             */
            if (backoff >= max_backoff) {
                static constexpr auto freq = 5s;
                thread_local static ss::logger::rate_limit rate(freq);
                vloglr(
                  datalake_log,
                  ss::log_level::info,
                  rate,
                  "Waiting on shard-local disk reservation. Total available {}",
                  human::bytes(_shard_available_disk.current()));
            }
        }
    }

    size_t release_unused_disk_units() override {
        const auto units = _shard_available_disk.current();
        if (units > 0) {
            _shard_available_disk.consume(units);
        }
        return units;
    }

private:
    const size_t _total_memory;
    // note: the semaphore should be alive until all the reserved units are
    // deposited back.
    ssx::semaphore _available_memory;
    const size_t _reservation_block_size;
    scheduling_notifications& _notifier;
    ssx::semaphore _shard_available_disk;
    disk_manager& _disk_manager;
};

std::ostream& operator<<(std::ostream& os, const translation_status& status) {
    fmt::print(
      os,
      "{{target_lag: {}, next_checkpoint: {}, memory_reserved: {}}}",
      std::chrono::duration_cast<std::chrono::milliseconds>(status.target_lag),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        status.next_checkpoint_deadline - clock::now()),
      status.memory_bytes_reserved);
    return os;
}

std::ostream& operator<<(std::ostream& os, const translator& t) {
    fmt::print(os, "{{id: {}, status: {}}}", t.id(), t.status());
    return os;
}

translator_executable::translator_executable(
  translator_executable&& other) noexcept {
    if (this != &other) {
        *this = std::move(other);
    }
}

translator_executable&
translator_executable::operator=(translator_executable&& other) noexcept {
    _waiting_hook.swap_nodes(other._waiting_hook);
    _running_hook.swap_nodes(other._running_hook);
    _translator = std::move(other._translator);
    _start_time = other._start_time;
    _total_waiting_time = other._total_waiting_time;
    _total_running_time = other._total_running_time;
    _translations_scheduled = other._translations_scheduled;
    _current_wait_begin_time = other._current_wait_begin_time;
    _current_running_begin_time = other._current_running_begin_time;
    _stop_in_progress = other._stop_in_progress;
    return *this;
}

translation_status translator_executable::status() const {
    return _translator->status();
}

std::ostream& operator<<(std::ostream& os, const translator_executable& state) {
    std::optional<std::chrono::milliseconds> current_wait_time;
    if (state._current_wait_begin_time) {
        current_wait_time
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            clock::now() - state._current_wait_begin_time.value());
    }
    std::optional<std::chrono::milliseconds> current_running_time;
    if (state._current_running_begin_time) {
        current_running_time
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            clock::now() - state._current_running_begin_time.value());
    }
    fmt::print(
      os,
      "{{translator: {}, time_since_start: {}, current_wait_time: {}, "
      "total_wait_time: {}, current_running_time: {}, total_running_time: {},  "
      "translations_scheduled: {}, stop_in_progress: {}, "
      "running: {}, waiting: {}}}",
      *state._translator,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        clock::now() - state._start_time),
      current_wait_time,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.total_wait_time()),
      current_running_time,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        state.total_running_time()),
      state._translations_scheduled,
      state._stop_in_progress,
      state._running_hook.is_linked(),
      state._waiting_hook.is_linked());
    return os;
}

clock::duration translator_executable::total_running_time() const {
    auto result = _total_running_time;
    if (_current_running_begin_time) {
        result += (clock::now() - _current_running_begin_time.value());
    }
    return result;
}

clock::duration translator_executable::total_wait_time() const {
    auto result = _total_waiting_time;
    if (_current_wait_begin_time) {
        result += (clock::now() - _current_wait_begin_time.value());
    }
    return result;
}

void translator_executable::mark_waiting() {
    vassert(
      !_running_hook.is_linked(),
      "Unexpected state transition to waiting: {}",
      *this);
    // force unlink if it is already waiting.
    _waiting_hook.unlink();
    _current_wait_begin_time = clock::now();
}

void translator_executable::mark_running() {
    vassert(
      _waiting_hook.is_linked() && !_running_hook.is_linked(),
      "Unexpected state translation to running: {}",
      *this);
    _waiting_hook.unlink();
    if (_current_wait_begin_time) {
        _total_waiting_time
          += (clock::now() - _current_wait_begin_time.value());
        _current_wait_begin_time.reset();
    }
    _current_running_begin_time = clock::now();
    _translations_scheduled++;
}

void translator_executable::mark_idle() {
    vassert(
      !_waiting_hook.is_linked() && _running_hook.is_linked(),
      "Unexpected state transition to idle: {}",
      *this);
    _running_hook.unlink();
    if (_current_running_begin_time) {
        _total_running_time
          += (clock::now() - _current_running_begin_time.value());
        _current_running_begin_time.reset();
    }
    _stop_in_progress = false;
}

void translator_executable::mark_stopping(translator::stop_reason reason) {
    vassert(
      !_waiting_hook.is_linked() && _running_hook.is_linked()
        && !_stop_in_progress,
      "Invalid request to stop translation: {}",
      *this);
    _translator->stop_translation(reason);
    _stop_in_progress = true;
}

void executor::start_translation(
  translator_executable& state, clock::duration time_slice) {
    auto holder = gate.hold();
    try {
        state._translator->start_translation(time_slice);
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Exception {} starting translation: {}",
          std::current_exception(),
          state);
        // push it back to the end of the queue to try again later.
        state.mark_waiting();
        waiting.push_back(state);
        return;
    }
    state.mark_running();
    running.push_back(state);
}

void executor::stop_translation(
  translator_executable& state, translator::stop_reason reason) {
    auto holder = gate.hold();
    vlog(datalake_log.debug, "stopping translator: {}", state);
    try {
        state.mark_stopping(reason);
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Exception {} stopping translation: {}",
          std::current_exception(),
          state);
    }
}

scheduler::scheduler(
  size_t total_memory,
  size_t memory_block_size,
  std::unique_ptr<scheduling_policy> policy,
  disk_manager& disk_monitor)
  : _scheduling_policy(std::move(policy))
  , _disk_monitor(disk_monitor)
  , _mem_tracker(
      reservations_tracker::make_default(
        total_memory, memory_block_size, *this, _disk_monitor)) {
    ssx::repeat_until_gate_closed_or_aborted(
      _executor.gate, _executor.as, [this] {
          return main().handle_exception([](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                datalake_log,
                log_level,
                "Encountered exception in main loop: {}",
                e);
          });
      });
}

void scheduler::notify_ready(const translator_id& id) noexcept {
    if (_executor.gate.is_closed()) {
        return;
    }
    vlog(datalake_log.trace, "ready notification from translator: {}", id);
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        return;
    }
    auto& translator = it->second;
    if (
      translator._waiting_hook.is_linked()
      || translator._running_hook.is_linked()) {
        vlog(
          datalake_log.warn,
          "Invalid ready notification from translator, ignoring",
          translator);
        return;
    }
    vlog(datalake_log.trace, "Marking the translator ready: {}", translator);
    translator.mark_waiting();
    _executor.waiting.push_back(translator);
    _state_changed_cvar.signal();
}

void scheduler::notify_done(const translator_id& id) noexcept {
    if (_executor.gate.is_closed()) {
        return;
    }
    vlog(datalake_log.trace, "done notification from translator: {}", id);
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        return;
    }
    auto& translator = it->second;
    if (
      translator._waiting_hook.is_linked()
      || !translator._running_hook.is_linked()) {
        vlog(
          datalake_log.warn,
          "Invalid done notification from waiting translator, ignoring",
          translator);
        return;
    }
    vlog(datalake_log.trace, "Marking the translator done: {}", translator);
    translator.mark_idle();
    _state_changed_cvar.signal();
}

bool scheduler::requires_scheduling_actions() const {
    return !_executor.waiting.empty() || _mem_tracker->memory_exhausted()
           || !_executor.translators_for_immediate_finish.empty();
}

void scheduler::notify_memory_exhausted() {
    vlog(datalake_log.debug, "memory exhausted notification");
    _state_changed_cvar.signal();
}

ss::future<> scheduler::stop() {
    vlog(datalake_log.debug, "Stopping scheduler");
    _executor.as.request_abort();
    _state_changed_cvar.broken();
    co_await _executor.gate.close();
    co_await ss::max_concurrent_for_each(
      _executor.translators, 32, [](auto& it) mutable {
          return it.second._translator->close();
      });
}

ss::future<bool>
scheduler::add_translator(std::unique_ptr<translator> translator) {
    auto holder = _executor.gate.hold();
    const auto& id = translator->id();
    const auto& translators = _executor.translators;
    vlog(
      datalake_log.trace,
      "request to add translator with id: {}, current_translators: {}",
      id,
      translators.size());
    auto it = translators.find(id);
    if (it != translators.end()) {
        vlog(
          datalake_log.error,
          "duplicate translator registration: {}",
          it->second);
        co_return false;
    }
    it = _executor.translators
           .insert(
             std::make_pair(id, translator_executable{std::move(translator)}))
           .first;
    try {
        co_await it->second._translator->init(*this, *_mem_tracker);
    } catch (...) {
        vlog(
          datalake_log.error,
          "[{}] error initing translator: {}",
          id,
          std::current_exception());
        co_return false;
    }
    co_return true;
}

ss::future<> scheduler::remove_translator(const translator_id& id) {
    auto holder = _executor.gate.hold();
    vlog(
      datalake_log.trace,
      "request to remove translator with id: {}, current_translators: {}",
      id,
      _executor.translators.size());
    auto it = _executor.translators.find(id);
    if (it == _executor.translators.end()) {
        co_return;
    }
    it->second._waiting_hook.unlink();
    it->second._running_hook.unlink();
    auto translator = std::move(it->second);
    _executor.translators.erase(it);
    co_await translator._translator->close();
}

size_t scheduler::running_translators() const {
    return _executor.running.size();
}

size_t scheduler::release_unused_disk_units() {
    return _mem_tracker->release_unused_disk_units();
}

void scheduler::request_immediate_finish(
  chunked_vector<finish_request> translators) {
    _executor.translators_for_immediate_finish.clear();
    // `i` captures both priority (lowest is highest), and serves as a unique
    // identifer. see `on_resource_exhaustion` for how this property is used.
    for (size_t i = 0; i < translators.size(); ++i) {
        _executor.translators_for_immediate_finish.emplace(
          i,
          executor::finish_request(
            std::move(translators[i].id), translators[i].reason));
    }
    // there is no mechanism for backing out a request to finish, so there isn't
    // any additional work that can be done if the requests are cleared.
    if (!_executor.translators_for_immediate_finish.empty()) {
        _state_changed_cvar.signal();
    }
}

ss::future<> scheduler::main() {
    vlog(datalake_log.trace, "Starting scheduling loop");
    auto holder = _executor.gate.hold();
    while (!_executor.as.abort_requested()) {
        co_await _state_changed_cvar.wait(
          [this] { return requires_scheduling_actions(); });
        vlog(
          datalake_log.trace,
          "scheduler tick,  memory_exhausted: {} finish: {}",
          _mem_tracker->memory_exhausted(),
          _executor.translators_for_immediate_finish.size());
        if (
          _mem_tracker->memory_exhausted()
          || !_executor.translators_for_immediate_finish.empty()) {
            co_await _scheduling_policy->on_resource_exhaustion(
              _executor, *_mem_tracker);
        }
        co_await _scheduling_policy->schedule_one_translation(
          _executor, *_mem_tracker);
    }
}

// temporary default until a proper scheduling policy is implemented.
std::unique_ptr<scheduling_policy> scheduling_policy::make_default(
  config::binding<size_t> max_concurrent_translators,
  clock::duration translation_time_quota) {
    return std::make_unique<fair_scheduling_policy>(
      std::move(max_concurrent_translators), translation_time_quota);
}

std::unique_ptr<reservations_tracker> reservations_tracker::make_default(
  size_t total_memory,
  size_t memory_block_size,
  scheduling_notifications& notifier,
  disk_manager& disk_manager) {
    return std::make_unique<default_reservations_tracker>(
      total_memory, memory_block_size, notifier, disk_manager);
}

} // namespace datalake::translation::scheduling
