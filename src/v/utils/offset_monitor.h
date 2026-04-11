/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

/// Offset monitor.
///
/// Utility for managing waiters based on a threshold offset. Supports multiple
/// waiters on the same offset, as well as timeout and abort source methods of
/// aborting a wait.
template<class Offset>
class offset_monitor {
public:
    /// Existing waiters receive abort_requested exception.
    void stop() {
        for (auto& waiter : _waiters) {
            waiter.second->done.set_exception(ss::abort_requested_exception());
        }
        _waiters.clear();
        _stopped = true;
    }

    /// Wait until at least the given offset has been notified, a timeout
    /// occurs, or an abort has been requested.
    ss::future<> wait(
      Offset offset,
      model::timeout_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as) {
        // the offset has already been applied
        if (offset <= _last_applied) {
            return ss::now();
        }
        if (unlikely(_stopped)) {
            return ss::make_exception_future<>(ss::abort_requested_exception());
        }
        auto w = std::make_unique<waiter>(this, deadline, as);
        auto f = w->done.get_future();
        if (!f.available()) {
            // the future may already be available, for example if an abort had
            // already be requested. in that case, skip adding as a waiter.
            _waiters.emplace(offset, std::move(w));
        }
        return f;
    }

    /// Returns true if there are no waiters.
    bool empty() const { return _waiters.empty(); }

    /// Returns the last notified offset.
    Offset last_applied() const { return _last_applied; }

    /// Notify waiters of an offset.
    void notify(Offset offset) {
        _last_applied = std::max(offset, _last_applied);

        while (true) {
            auto it = _waiters.begin();
            if (it == _waiters.end() || offset < it->first) {
                return;
            }
            it->second->done.set_value();
            // when the waiter is destroyed here by erase, then if they are
            // active, the timer is cancelled and the abort source subscription
            // is removed.
            _waiters.erase(it);
        }
    }

private:
    struct waiter {
        offset_monitor* mon;
        ss::promise<> done;
        ss::timer<model::timeout_clock> timer;
        ss::abort_source::subscription sub;

        waiter(
          offset_monitor* mon,
          model::timeout_clock::time_point timeout,
          std::optional<std::reference_wrapper<ss::abort_source>> as)
          : mon(mon) {
            if (as) {
                auto opt_sub = as->get().subscribe(
                  [this]() noexcept { handle_abort(false); });
                if (opt_sub) {
                    sub = std::move(*opt_sub);
                } else {
                    done.set_exception(ss::abort_requested_exception());
                    return;
                }
            }
            if (timeout != model::no_timeout) {
                timer.set_callback([this] { handle_abort(true); });
                timer.arm(timeout);
            }
        }

        void handle_abort(bool is_timeout) {
            if (is_timeout) {
                done.set_exception(ss::timed_out_error());
            } else {
                // Use the generic seastar abort_requested_exception, because
                // in many locations we handle this gracefully and do not log
                // it as an error during shutdown.
                done.set_exception(ss::abort_requested_exception());
            }
            auto it = std::find_if(
              mon->_waiters.begin(),
              mon->_waiters.end(),
              [this](const waiters_type::value_type& w) {
                  return w.second.get() == this;
              });
            vassert(it != mon->_waiters.end(), "waiter not found");
            // when the waiter is destroyed here by erase, then if they are
            // active, the timer is cancelled and the abort source subscription
            // is removed.
            mon->_waiters.erase(it); // *this is no longer valid after erase
        }
    };

    friend waiter;

    using waiters_type = absl::btree_multimap<Offset, std::unique_ptr<waiter>>;

    waiters_type _waiters;
    Offset _last_applied;
    bool _stopped{false};
};
