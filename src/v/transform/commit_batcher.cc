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

#include "commit_batcher.h"

#include "cluster/errc.h"
#include "config/property.h"
#include "logger.h"
#include "rpc/backoff_policy.h"
#include "ssx/semaphore.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <iterator>
#include <utility>

namespace transform {

template<typename ClockType>
commit_batcher<ClockType>::commit_batcher(
  config::binding<std::chrono::milliseconds> commit_interval,
  std::unique_ptr<offset_committer> oc)
  : _offset_committer(std::move(oc))
  , _commit_interval(std::move(commit_interval))
  , _timer(
      [this] { ssx::spawn_with_gate(_gate, [this] { return flush(); }); }) {}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::start() {
    ssx::spawn_with_gate(_gate, [this] { return find_coordinator_loop(); });
    return ss::now();
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::find_coordinator_loop() {
    using namespace std::chrono;
    constexpr static auto base_backoff = 100ms;
    constexpr static auto max_backoff = 5000ms;
    auto backoff = ::rpc::make_exponential_backoff_policy<ClockType>(
      base_backoff, max_backoff);
    while (!_gate.is_closed()) {
        co_await _unbatched_cond_var.wait(
          [this] { return !_unbatched.empty() || _gate.is_closed(); });
        bool was_all_failures = co_await assign_coordinators();
        if (!was_all_failures) {
            backoff.reset();
            continue;
        }
        backoff.next_backoff();
        try {
            co_await ss::sleep_abortable<ClockType>(
              backoff.current_backoff_duration(), _as);
        } catch (const ss::sleep_aborted&) {
            // do nothing, we're shutting down.
        }
    }
}

template<typename ClockType>
ss::future<bool> commit_batcher<ClockType>::assign_coordinators() {
    bool was_all_failures = !_unbatched.empty();
    auto it = _unbatched.begin();
    while (it != _unbatched.end() && !_gate.is_closed()) {
        model::transform_offsets_key key = it->first;
        auto fut = co_await ss::coroutine::as_future<
          result<model::partition_id, cluster::errc>>(
          _offset_committer->find_coordinator(key));
        if (fut.failed()) {
            vlog(
              tlog.warn,
              "unable to determine key ({}) coordinator: {}",
              key,
              fut.get_exception());
            it = _unbatched.upper_bound(key);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              tlog.warn,
              "unable to determine key ({}) coordinator: {}",
              key,
              cluster::error_category().message(int(result.error())));
            it = _unbatched.upper_bound(key);
            continue;
        }
        was_all_failures = false;
        model::partition_id coordinator = result.value();
        auto entry = _unbatched.extract(key);
        // It's possible that the value was removed while the request was being
        // made, in that case just skip the update.
        if (!entry) {
            it = _unbatched.upper_bound(key);
            continue;
        }
        _batched[coordinator].insert_or_assign(entry.key(), entry.mapped());
        _coordinator_cache[key] = coordinator;
        if (!_timer.armed()) {
            _timer.arm(_commit_interval());
        }
        it = _unbatched.upper_bound(key);
    }
    co_return was_all_failures;
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::stop() {
    _timer.cancel();
    _as.request_abort();
    _unbatched_cond_var.signal();
    co_await _gate.close();
    co_await flush();
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::wait_for_previous_flushes(
  model::transform_offsets_key, ss::abort_source* as) {
    try {
        co_await ssx::sleep_abortable<ClockType>(_commit_interval(), *as, _as);
    } catch (const ss::sleep_aborted&) {
        // do nothing as we're shutting down, callers will do the right thing
    }
}

template<typename ClockType>
void commit_batcher<ClockType>::preload(model::transform_offsets_key) {
    // For now, don't do anything, we will resolve it first time they key needs
    // to commit a batch. In the future we can preload this, mostly to surface
    // any errors (like too many keys, etc) before reading and processing any
    // batches.
}

template<typename ClockType>
void commit_batcher<ClockType>::unload(model::transform_offsets_key key) {
    // Erase the key from the coordinator cache so that we don't
    _coordinator_cache.erase(key);
    _unbatched.erase(key);
    // We don't need to clear batched, we can allow it to be flushed async.
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::commit_offset(
  model::transform_offsets_key k, model::transform_offsets_value v) {
    auto _ = _gate.hold();
    auto it = _coordinator_cache.find(k);
    if (it == _coordinator_cache.end()) {
        _unbatched.insert_or_assign(k, v);
        _unbatched_cond_var.signal();
    } else {
        _batched[it->second].insert_or_assign(k, v);
        if (!_timer.armed()) {
            _timer.arm(_commit_interval());
        }
    }
    return ss::now();
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::flush() {
    absl::btree_map<model::partition_id, kv_map> batched;
    _batched.swap(batched);
    constexpr static size_t max_concurrent_flushes = 10;
    co_await ss::max_concurrent_for_each(
      std::make_move_iterator(batched.begin()),
      std::make_move_iterator(batched.end()),
      max_concurrent_flushes,
      [this](auto entry) {
          return do_flush(entry.first, std::move(entry.second));
      });
}

template<typename ClockType>
ss::future<> commit_batcher<ClockType>::do_flush(
  model::partition_id coordinator, kv_map batch) {
    auto fut = co_await ss::coroutine::as_future<cluster::errc>(
      _offset_committer->batch_commit(coordinator, std::move(batch)));
    if (fut.failed()) {
        vlog(
          tlog.warn,
          "unable to commit batch (coordinator={}): {}",
          coordinator,
          fut.get_exception());
        co_return;
    }
    cluster::errc ec = fut.get();
    if (ec == cluster::errc::success) {
        co_return;
    }
    vlog(
      tlog.warn,
      "unable to commit batch (coordinator={}): {}",
      coordinator,
      cluster::error_category().message(int(ec)));
}

template class commit_batcher<ss::lowres_clock>;
template class commit_batcher<ss::manual_clock>;

} // namespace transform
