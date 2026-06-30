/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "base/vlog.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <chrono>
#include <list>
#include <optional>
#include <utility>

namespace ssx {

using namespace std::chrono_literals;

/// Function to run on notify().  Returning is success.  Exceptions are retried
/// with a backoff.  A shutdown exception ends the loop and propagates to
/// waiters.
using reconcile_fn = ss::noncopyable_function<ss::future<>()>;

struct backoff_policy {
    std::chrono::milliseconds base{100ms};
    std::chrono::milliseconds max{5s};
};

/// Ensures that the reconcile_fn runs at least once after each call to
/// notify().
///
/// notify() records that desired state may have moved and starts the loop if it
/// is not already running. A notification that arrives mid-attempt is not lost:
/// it causes another attempt rather than a second concurrent loop.
///
/// A closed gate, abort source, or a shutdown exception from the reconcile_fn
/// results in giving up on all pending notifies and notifying waiters of
/// failure.
///
/// User is responsible for triggering the passed abort source and closing the
/// passed gate before anything the step touches is torn down.
class reconciler {
public:
    reconciler(
      ss::gate& gate,
      ss::logger& log,
      ss::sstring name,
      reconcile_fn step,
      ss::abort_source& as,
      backoff_policy backoff = {})
      : _gate(gate)
      , _log(log)
      , _name(std::move(name))
      , _step(std::move(step))
      , _backoff(backoff)
      , _as(as) {}

    reconciler(const reconciler&) = delete;
    reconciler& operator=(const reconciler&) = delete;
    reconciler(reconciler&&) = delete;
    reconciler& operator=(reconciler&&) = delete;
    ~reconciler() = default;

    /// Notify to trigger a reconciliation.  Never blocks and never throws.
    void notify() {
        _dirty = true;
        if (_running) {
            return;
        }
        if (check_give_up()) {
            return;
        }
        // Set before spawning rather than inside run(), so the single-loop
        // guarantee does not rest on the coroutine body starting eagerly.
        _running = true;
        // Holds the gate for the duration of the loop
        ssx::spawn_with_gate(_gate, [this] { return run(); });
    }

    /// notify(), and resolve once an attempt that began after this call has
    /// succeeded.
    ///
    /// Fails with the reason the loop gave up (abort, closed gate).
    /// A caller which does not care should detach with
    /// ssx::ignore_shutdown_exceptions.
    ss::future<> notify_and_wait() {
        if (!_pending) {
            _pending.emplace();
        }
        auto f = _pending->get_shared_future();
        notify();
        return f;
    }

    bool idle() const { return !_running; }

private:
    using waiters = std::list<ss::shared_promise<>>;

    bool check_give_up(waiters* covering = nullptr) {
        if (_gate.is_closed()) {
            give_up(
              covering, std::make_exception_ptr(ss::gate_closed_exception{}));
            return true;
        }
        if (aborted()) {
            give_up(
              covering,
              std::make_exception_ptr(ss::abort_requested_exception{}));
            return true;
        }
        return false;
    }

    void give_up(waiters* covering, const std::exception_ptr& e) {
        waiters to_fail;
        if (covering != nullptr) {
            to_fail.splice(to_fail.end(), *covering);
        }
        if (_pending) {
            to_fail.push_back(std::move(*_pending));
            _pending.reset();
        }
        for (auto& w : to_fail) {
            w.set_exception(e);
        }
    }

    bool aborted() const { return _as.abort_requested(); }

    bool stopping() const { return _gate.is_closed() || aborted(); }

    ss::future<> sleep(std::chrono::milliseconds delay) {
        return ss::sleep_abortable(delay, _as);
    }

    ss::future<> run() {
        auto mark_idle = ss::defer([this]() noexcept { _running = false; });

        auto delay = _backoff.base;
        // waiters covered by the current attempt
        waiters covering;
        while (_dirty && !stopping()) {
            _dirty = false;
            if (_pending) {
                covering.push_back(std::move(*_pending));
                _pending.reset();
            }
            auto f = co_await ss::coroutine::as_future(
              ss::futurize_invoke(_step));
            if (!f.failed()) {
                for (auto& w : covering) {
                    w.set_value();
                }
                covering.clear();
                delay = _backoff.base;
                continue;
            }
            auto e = f.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                give_up(&covering, e);
                co_return;
            }
            vlog(
              _log.warn,
              "{}: reconciliation attempt failed: {}; retrying in {}ms",
              _name,
              e,
              delay / 1ms);
            _dirty = true;
            auto slept = co_await ss::coroutine::as_future(sleep(delay));
            if (slept.failed()) {
                give_up(&covering, slept.get_exception());
                co_return;
            }
            delay = std::min(delay * 2, _backoff.max);
        }
        check_give_up(&covering);
    }

    ss::gate& _gate;
    ss::logger& _log;
    ss::sstring _name;
    reconcile_fn _step;
    backoff_policy _backoff;
    ss::abort_source& _as;
    // Waiters whose notification no attempt has begun to answer yet.
    std::optional<ss::shared_promise<>> _pending;
    bool _dirty{false};
    bool _running{false};
};

} // namespace ssx
