/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/logging/log_manager.h"

#include "absl/container/flat_hash_map.h"
#include "base/vassert.h"
#include "event.h"
#include "logger.h"
#include "probes.h"
#include "random/simple_time_jitter.h"
#include "strings/utf8.h"
#include "transform/logging/errc.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/algorithm/container.h>
#include <absl/strings/escaping.h>

namespace transform::logging {
namespace {
using namespace std::chrono_literals;

bool contains_invalid_characters(std::string_view str) {
    // Only allow valid utf8: non-control characters or newlines or tabs.
    return !is_valid_utf8(str) || absl::c_any_of(str, [](char c) {
        return is_control_char(c) && c != '\n' && c != '\t';
    });
}

} // namespace

namespace detail {

struct buffer_entry {
    buffer_entry() = delete;
    explicit buffer_entry(event event, ssx::semaphore_units units)
      : event(std::move(event))
      , units(std::move(units)) {}
    event event;
    ssx::semaphore_units units;
};

template<typename ClockType>
class flusher {
    static constexpr ClockType::duration jitter_amt{50ms};
    static constexpr int MAX_CONCURRENCY = 10;

public:
    flusher() = delete;
    explicit flusher(
      ss::abort_source* as,
      transform::logging::client* c,
      config::binding<std::chrono::milliseconds> fi,
      manager_probe* probe)
      : _as(as)
      , _client(c)
      , _interval_ms(std::move(fi))
      , _probe(probe)
      , _jitter(_interval_ms(), jitter_amt) {
        _interval_ms.watch([this]() {
            _jitter = simple_time_jitter<ClockType>{_interval_ms(), jitter_amt};
            wakeup();
        });
    }

    template<typename BuffersT>
    ss::future<> start(BuffersT* bufs) {
        vassert(
          bufs != nullptr, "Pointer to transform log buffers must not be null");
        ssx::spawn_with_gate(
          _gate, [this, bufs]() -> ss::future<> { return flush_loop(bufs); });
        return ss::now();
    }

    void wakeup() { _wakeup_signal.signal(); }

    ss::future<> stop() {
        _wakeup_signal.broken();
        return (!_gate.is_closed() ? _gate.close() : ss::now());
    }

private:
    bool _inited{false};
    template<typename BuffersT>
    ss::future<> flush_loop(BuffersT* bufs) {
        while (!_as->abort_requested()) {
            try {
                // the duration overload passes now() + dur to the timepoint
                // overload, but the now() calculation is tied to an underlying
                // clock type, so it doesn't work with ss::manual_clock. the
                // timepoint overload template has a clocktype parameter, so we
                // use that one to get the behavior we want for testing
                if constexpr (std::is_same_v<ClockType, ss::manual_clock>) {
                    co_await _wakeup_signal.wait<ClockType>(
                      ClockType::now() + _jitter.base_duration());
                } else {
                    co_await _wakeup_signal.wait<ClockType>(
                      ClockType::now() + _jitter.next_duration());
                }
            } catch (const ss::broken_condition_variable&) {
                break;
            } catch (const ss::condition_variable_timed_out&) {
            }
            if (!_inited) {
                _inited = co_await _client->initialize()
                          == logging::errc::success;
            }
            co_await flush(bufs);
        }
    }

    template<typename BuffersT>
    ss::future<> flush(BuffersT* bufs) {
        size_t n_events = 0;
        absl::flat_hash_map<model::partition_id, io::json_batches> batches{};
        BuffersT local_bufs;
        for (auto& [name, buf] : *bufs) {
            if (buf.empty()) {
                continue;
            }
            n_events += buf.size();
            local_bufs.emplace(name, std::exchange(buf, {}));
        }

        co_await ss::max_concurrent_for_each(
          local_bufs, MAX_CONCURRENCY, [this, &batches](auto& pr) {
              auto& [name, events] = pr;
              model::transform_name_view nv{name};
              auto pid_res = _client->compute_output_partition(nv);
              if (pid_res.has_error()) {
                  vlog(
                    tlg_log.warn,
                    "Partition lookup failed for transform '{}': {}",
                    name,
                    std::error_code{pid_res.assume_error()}.message());
                  return ss::now();
              }

              return do_serialize_log_events(nv, std::move(events))
                .then(
                  [pid = pid_res.assume_value(),
                   &batches](auto batch) -> ss::future<> {
                      batches[pid].emplace_back(std::move(batch));
                      return ss::now();
                  });
          });

        co_await ss::max_concurrent_for_each(
          batches, MAX_CONCURRENCY, [this](auto& pr) mutable -> ss::future<> {
              auto& [pid, evs] = pr;
              return do_flush(pid, std::move(evs));
          });

        vlog(tlg_log.trace, "Processed {} log events", n_events);
    }

    ss::future<io::json_batch>
    do_serialize_log_events(model::transform_name_view name, auto events) {
        vassert(!events.empty(), "Attempt to serialize empty buffer");
        ss::chunked_fifo<iobuf> ev_json;
        ev_json.reserve(events.size());
        std::optional<ssx::semaphore_units> buffer_units;

        while (!events.empty()) {
            auto e = std::move(events.front());
            events.pop_front();
            iobuf b;
            e.event.to_json(name, b);
            ev_json.emplace_back(std::move(b));
            if (!buffer_units) {
                buffer_units.emplace(std::move(e.units));
            } else {
                buffer_units->adopt(std::move(e.units));
            }

            // reactor will stall if we try to serialize a whole lot of
            // messages all at once
            co_await ss::maybe_yield();
        }

        iobuf nb;
        nb.append(name().data(), name().size());
        co_return io::json_batch{
          std::move(nb), std::move(ev_json), std::move(*buffer_units)};
    }

    ss::future<> do_flush(model::partition_id pid, io::json_batches events) {
        // NOTE(oren): At this point, semaphore units for occupied buffer
        // capacity have been adopted by `json_batch`es. These units will be
        // released (i.e. buffer capacity freed) only once those records have
        // been produced.
        std::error_code ec = co_await _client->write(pid, std::move(events));
        if (ec != errc::success) {
            _probe->write_error();
            vlog(tlg_log.warn, "Flush failed: {}", ec.message());
        }
    }
    ss::abort_source* _as = nullptr;
    transform::logging::client* _client = nullptr;
    config::binding<std::chrono::milliseconds> _interval_ms;
    manager_probe* _probe;
    simple_time_jitter<ClockType> _jitter;
    ss::condition_variable _wakeup_signal{};
    ss::gate _gate{};
};

template class flusher<ss::lowres_clock>;
template class flusher<ss::manual_clock>;

} // namespace detail

template<typename ClockType>
manager<ClockType>::manager(
  model::node_id self,
  std::unique_ptr<client> c,
  size_t bc,
  config::binding<size_t> ll,
  config::binding<std::chrono::milliseconds> fi)
  : _self(self)
  , _client(std::move(c))
  , _line_limit_bytes(std::move(ll))
  , _buffer_limit_bytes(bc)
  , _buffer_low_water_mark(_buffer_limit_bytes / lwm_denom)
  , _buffer_sem(_buffer_limit_bytes, "Log manager buffer semaphore")
  , _flush_interval(std::move(fi)) {}

template<typename ClockType>
manager<ClockType>::~manager() = default;

template<typename ClockType>
ss::future<> manager<ClockType>::start() {
    _probe = std::make_unique<manager_probe>();
    _probe->setup_metrics([this] {
        return 1.0
               - (static_cast<double>(_buffer_sem.available_units()) / static_cast<double>(_buffer_limit_bytes));
    });
    _flusher = std::make_unique<detail::flusher<ClockType>>(
      &_as, _client.get(), _flush_interval, _probe.get());
    co_return co_await _flusher->template start<>(&_log_buffers);
}

template<typename ClockType>
ss::future<> manager<ClockType>::stop() {
    _as.request_abort();
    if (_flusher != nullptr) {
        co_await _flusher->stop();
    }
    _flusher.reset(nullptr);
    _probe.reset(nullptr);
    std::exchange(_logger_probes, probe_map_t{});
}

template<typename ClockType>
bool manager<ClockType>::check_lwm() const {
    return _buffer_sem.available_units() <= _buffer_low_water_mark;
}

template<typename ClockType>
void manager<ClockType>::enqueue_log(
  ss::log_level level,
  model::transform_name_view transform_name,
  std::string_view message) {
    auto msg_len =
      [lim = _line_limit_bytes()](std::string_view message) -> size_t {
        return std::min(lim, message.size());
    };

    auto validate_msg =
      [&msg_len](std::string_view message) -> std::optional<ss::sstring> {
        auto sub_view = message.substr(0, msg_len(message));
        if (contains_invalid_characters(sub_view)) {
            return std::nullopt;
        }
        return ss::sstring{sub_view.data(), sub_view.size()};
    };

    auto get_queue = [this](std::string_view name) {
        auto res = _log_buffers.find(name);
        if (res == _log_buffers.end()) {
            auto [it, ins] = _log_buffers.emplace(
              ss::sstring{name.data(), name.size()}, buffer_t{});
            if (ins) {
                return it;
            }
            // otherwise something went badly wrong. we'll return the end
            // iterator and fail the operation
            vlog(
              tlg_log.error, "Failed to enqueue transform log: Buffer error");
        }
        return res;
    };

    auto get_probe = [this](std::string_view name) -> logger_probe* {
        auto res = _logger_probes.find(name);
        if (res == _logger_probes.end()) {
            auto [it, _] = _logger_probes.emplace(
              ss::sstring{name.data(), name.size()},
              std::make_unique<logger_probe>());
            it->second->setup_metrics(model::transform_name_view{name});
            return it->second.get();
        }
        return res->second.get();
    };

    auto it = get_queue(transform_name);
    if (it == _log_buffers.end()) {
        return;
    }

    auto probe = get_probe(transform_name);
    probe->log_event();

    // Unfortunately, we don't know how long an escaped string will be
    // until we've allocated memory for it. So we optimistically grab
    // units for the unmodified log message here, hoping that, in the
    // common case, no control chars are present. If the message fails
    // validation, we will simply return the units to _buffer_sem on
    // function return.
    // NOTE(oren): we can still truncate a view up front w/o allocating
    message = message.substr(0, msg_len(message));
    auto units = ss::try_get_units(_buffer_sem, message.size());
    if (!units) {
        probe->dropped_log_event();
        vlog(tlg_log.debug, "Failed to enqueue transform log: Buffer full");
        return;
    }

    auto b = validate_msg(message);
    if (!b.has_value()) {
        vlog(
          tlg_log.debug,
          "Failed to enqueue transform log: Message validation failed");
        return;
    }

    it->second.emplace_back(
      event{_self, event::clock_type::now(), level, std::move(*b)},
      std::move(*units));

    if (check_lwm()) {
        _flusher->wakeup();
    }
}

template class manager<ss::lowres_clock>;
template class manager<ss::manual_clock>;

} // namespace transform::logging
