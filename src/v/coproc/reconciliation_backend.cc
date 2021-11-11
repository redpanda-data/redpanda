/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/reconciliation_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<storage::api>& storage) noexcept
  : _self(model::node_id(config::node().node_id))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _storage(storage) {
    _retry_timer.set_callback([this] {
        (void)within_context([this]() { return fetch_and_reconcile(); });
    });
}

template<typename Fn>
ss::future<> reconciliation_backend::within_context(Fn&& fn) {
    try {
        return ss::with_gate(
          _gate, [this, fn = std::forward<Fn>(fn)]() mutable {
              if (!_as.abort_requested()) {
                  return _mutex.with(
                    [fn = std::forward<Fn>(fn)]() mutable { return fn(); });
              }
              return ss::now();
          });
    } catch (const ss::gate_closed_exception& ex) {
        vlog(coproclog.debug, "Timer fired during shutdown: {}", ex);
    }
    return ss::now();
}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](std::vector<update_t> deltas) {
          return within_context([this, deltas = std::move(deltas)]() mutable {
              for (auto& d : deltas) {
                  auto ntp = d.ntp;
                  _topic_deltas[ntp].push_back(std::move(d));
              }
              return fetch_and_reconcile();
          });
      });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<std::vector<reconciliation_backend::update_t>>
reconciliation_backend::process_events_for_ntp(
  model::ntp ntp, std::vector<update_t> updates) {
    std::vector<update_t> retries;
    for (auto& d : updates) {
        vlog(coproclog.trace, "executing ntp: {} op: {}", d.ntp, d);
        auto err = co_await process_update(d);
        vlog(coproclog.info, "partition operation {} result {}", d, err);
        if (err != errc::success) {
            /// In this case the source topic exists but its
            /// associated partition doesn't, so try again
            retries.push_back(std::move(d));
        }
    }
    co_return retries;
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    using deltas_cache = decltype(_topic_deltas);
    auto deltas = std::exchange(_topic_deltas, {});
    deltas_cache retry_cache;
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this, &retry_cache](deltas_cache::value_type& p) -> ss::future<> {
          auto retries = co_await process_events_for_ntp(p.first, p.second);
          if (!retries.empty()) {
              retry_cache[p.first] = std::move(retries);
          }
      });
    if (!retry_cache.empty()) {
        vlog(
          coproclog.warn,
          "There were recoverable errors when processing events, retrying");
        std::swap(_topic_deltas, retry_cache);
        if (!_retry_timer.armed()) {
            _retry_timer.arm(
              model::timeout_clock::now() + retry_timeout_interval);
        }
    }
}

ss::future<std::error_code> reconciliation_backend::process_update(update_t) {
    co_return errc::success;
}

} // namespace coproc
