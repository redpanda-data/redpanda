// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/offset_monitor.h"

#include "vassert.h"

#include <seastar/core/future-util.hh>

namespace raft {

void offset_monitor::stop() {
    for (auto& waiter : _waiters) {
        waiter.second->done.set_exception(wait_aborted());
    }
    _waiters.clear();
}

ss::future<> offset_monitor::wait(
  model::offset offset,
  model::timeout_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    // the offset has already been applied
    if (offset <= _last_applied) {
        return ss::now();
    }
    auto w = std::make_unique<waiter>(this, timeout, as);
    auto f = w->done.get_future();
    if (!f.available()) {
        // the future may already be available, for example if an abort had
        // already be requested. in that case, skip adding as a waiter.
        _waiters.emplace(offset, std::move(w));
    }
    return f;
}

void offset_monitor::notify(model::offset offset) {
    _last_applied = std::max(offset, _last_applied);

    while (true) {
        auto it = _waiters.begin();
        if (it == _waiters.end() || offset < it->first) {
            return;
        }
        it->second->done.set_value();
        // when the waiter is destroyed here by erase, then if they are active,
        // the timer is cancelled and the abort source subscription is removed.
        _waiters.erase(it);
    }
}

offset_monitor::waiter::waiter(
  offset_monitor* mon,
  model::timeout_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as)
  : mon(mon) {
    if (as) {
        auto opt_sub = as->get().subscribe(
          [this]() noexcept { handle_abort(); });
        if (opt_sub) {
            sub = std::move(*opt_sub);
        } else {
            done.set_exception(wait_aborted());
            return;
        }
    }
    if (timeout != model::no_timeout) {
        timer.set_callback([this] { handle_abort(); });
        timer.arm(timeout);
    }
}

void offset_monitor::waiter::handle_abort() {
    done.set_exception(wait_aborted());
    auto it = std::find_if(
      mon->_waiters.begin(),
      mon->_waiters.end(),
      [this](const waiters_type::value_type& w) {
          return w.second.get() == this;
      });
    vassert(it != mon->_waiters.end(), "waiter not found");
    // when the waiter is destroyed here by erase, then if they are active,
    // the timer is cancelled and the abort source subscription is removed.
    mon->_waiters.erase(it); // *this is no longer valid after erase
}

} // namespace raft
