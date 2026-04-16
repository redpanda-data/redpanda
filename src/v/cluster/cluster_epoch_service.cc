/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/cluster_epoch_service.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "raft/notification.h"
#include "ssx/abort_source.h"
#include "ssx/future-util.h"
#include "ssx/work_queue.h"
#include "utils/backoff_policy.h"

#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <fmt/chrono.h>

#include <exception>
#include <type_traits>

using namespace std::chrono_literals;

namespace cluster {

// raft0_state is a service that runs on shard0 and is responsible for
// managing the current epoch. The current epoch is a frozen point in time of
// the committed offset of the controller log. We need to use the controller log
// so that we can correlate partition creation with the current epoch.
//
// We periodically "freeze" and refresh the epoch so that we control the epoch
// change and don't have to worry about random cluster operations causing more
// frequent epoch changes (which have the potential for distrupting L0
// operations in cloud topics).
template<typename Clock>
class cluster_epoch_service<Clock>::raft0_state {
public:
    raft0_state(
      ss::lw_shared_ptr<raft::consensus> raft0,
      ss::sharded<controller_stm>& stm,
      ss::sharded<raft::group_manager>& group_manager)
      : _raft0(std::move(raft0))
      , _stm(stm)
      , _group_manager(group_manager)
      , _queue([](const std::exception_ptr& err) {
          vlog(clusterlog.error, "Error in cluster epoch service: {}", err);
      })
      , _leadership_notification_id(
          group_manager.local().register_leadership_notification(
            [this](
              raft::group_id group_id,
              model::term_id,
              std::optional<model::node_id>) noexcept {
                if (group_id != _raft0->group()) {
                    return;
                }
                if (_raft0->is_leader()) {
                    start_service_loop();
                } else {
                    stop_service_loop();
                }
            })) {}

    std::optional<model::node_id> current_leader() const noexcept {
        return _raft0->get_leader_id();
    }

    void start() {
        if (_raft0->is_leader()) {
            start_service_loop();
        } else {
            stop_service_loop();
        }
    }

    // Shutdown this state, stopping the service loop if it's running.
    //
    // Must be called before destruction.
    ss::future<> stop() {
        _group_manager.local().unregister_leadership_notification(
          _leadership_notification_id);
        co_await _queue.shutdown();
        // Now that the queue won't accept new tasks, we can safely stop the
        // service loop if it's running.
        _abort_source.request_abort();
        co_await std::exchange(_loop, std::nullopt)
          .or_else([]() { return std::make_optional(ss::now()); })
          .value()
          .handle_exception([](const std::exception_ptr& ex) {
              vlog(
                clusterlog.error,
                "Error in shutting down cluster epoch service loop: {}",
                ex);
          });
    }

    // Get the current epoch - only set if this node is the leader of raft0.
    std::optional<model::offset> current_epoch() const noexcept {
        return _current_epoch;
    }

private:
    void start_service_loop() noexcept {
        _queue.submit([this] {
            if (_loop) {
                return ss::now();
            }
            _abort_source = {};
            _loop = service_loop();
            return ss::now();
        });
    }
    void stop_service_loop() noexcept {
        _queue.submit([this] {
            _abort_source.request_abort();
            return std::exchange(_loop, std::nullopt)
              .or_else([]() { return std::make_optional(ss::now()); })
              .value();
        });
    }

    ss::future<> service_loop();
    ss::future<> update_epoch();

    ss::lw_shared_ptr<raft::consensus> _raft0;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<raft::group_manager>& _group_manager;
    ssx::work_queue _queue;
    raft::group_manager_notification_id _leadership_notification_id;

    // The following members are used to manage the service loop iff this is
    // shard0 and the current leader of the controller.
    ss::abort_source _abort_source;
    std::optional<ss::future<>> _loop;
    std::optional<model::offset> _current_epoch;
};

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::raft0_state::service_loop() {
    _current_epoch = co_await _stm.local().bootstrap_committed_offset();
    auto cleanup = ss::defer([this]() noexcept { _current_epoch.reset(); });
    while (!_abort_source.abort_requested()) {
        try {
            co_await ss::sleep_abortable<Clock>(
              config::shard_local_cfg()
                .cloud_topics_epoch_service_epoch_increment_interval(),
              _abort_source);
        } catch (const ss::sleep_aborted& e) {
            std::ignore = e;
        }
        if (_abort_source.abort_requested()) {
            co_return;
        }
        co_await update_epoch();
    }
}

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::raft0_state::update_epoch() {
    constexpr std::chrono::milliseconds base_backoff = 100ms;
    constexpr std::chrono::milliseconds max_backoff = 10s;
    constexpr std::chrono::milliseconds rpc_timeout = 3s;
    auto policy = make_exponential_backoff_policy<Clock>(
      base_backoff, max_backoff);
    while (!_abort_source.abort_requested()) {
        try {
            co_await ss::sleep_abortable<Clock>(
              policy.current_backoff_duration(), _abort_source);
            policy.next_backoff(); // if we get a failure increment the backoff
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
        auto deadline = ss::lowres_clock::now() + rpc_timeout;
        auto result_fut
          = co_await ss::coroutine::as_future<result<raft::replicate_result>>(
            _stm.local().quorum_write_empty_batch(deadline));
        if (result_fut.failed()) {
            std::exception_ptr ex = result_fut.get_exception();
            vlog(clusterlog.warn, "Failed to force increment epoch: {}", ex);
            continue;
        }
        auto result = result_fut.get();
        if (result) {
            _current_epoch = result.value().last_offset;
            vlog(
              clusterlog.debug,
              "Forced increment of cluster epoch to: {}",
              _current_epoch);
            co_return;
        } else if (result.error() != raft::errc::not_leader) {
            vlog(
              clusterlog.warn,
              "Failed to force increment epoch: {}",
              result.error());
        }
    }
}

namespace {

template<typename Func>
ss::future<std::expected<int64_t, std::error_code>> do_fetch_leader_epoch_impl(
  model::node_id self,
  ss::sharded<rpc::connection_cache>* rpc_conn,
  ss::abort_source* as,
  Func get_raft0_leader) {
    constexpr std::chrono::milliseconds base_backoff = 100ms;
    constexpr std::chrono::milliseconds max_backoff = 1s;
    constexpr static std::chrono::milliseconds rpc_timeout = 3s;
    auto policy = make_exponential_backoff_policy<model::timeout_clock>(
      base_backoff, max_backoff);
    const auto deadline = model::timeout_clock::now() + 10s;
    std::optional<std::error_code> last_error;
    while (!as->abort_requested() && model::timeout_clock::now() < deadline) {
        try {
            co_await ss::sleep_abortable<model::timeout_clock>(
              policy.current_backoff_duration(), *as);
            policy.next_backoff(); // if we get a failure increment the backoff
        } catch (const ss::sleep_aborted&) {
            break;
        }
        std::optional<model::node_id> raft0_leader = get_raft0_leader();
        if (!raft0_leader) {
            vlog(
              clusterlog.trace,
              "Failed to get cluster epoch, retrying... - no controller "
              "leader");
            last_error = make_error_code(errc::no_leader_controller);
            continue;
        }
        try {
            auto result = co_await rpc_conn->local()
                            .with_node_client<controller_client_protocol>(
                              self,
                              ss::this_shard_id(),
                              *raft0_leader,
                              rpc_timeout,
                              [](controller_client_protocol client) {
                                  return client.get_current_cluster_epoch(
                                    get_current_cluster_epoch_request{
                                      .timeout = rpc_timeout},
                                    rpc::client_opts(rpc_timeout));
                              });
            if (!result) {
                last_error = result.error();
                vlog(
                  clusterlog.trace,
                  "Failed to get cluster epoch, retrying... - {}",
                  result.error());
                continue;
            }
            auto resp = result.value().data;
            if (resp.ec != errc::success) {
                co_return std::unexpected(make_error_code(resp.ec));
            }
            co_return resp.epoch;
        } catch (...) {
            vlog(
              clusterlog.error,
              "Unknown error fetching current cluster epoch from leader {}: {}",
              raft0_leader,
              std::current_exception());
            co_return std::unexpected(make_error_code(errc::timeout));
        }
    }
    co_return std::unexpected(
      last_error.value_or(make_error_code(errc::timeout)));
}

} // namespace

template<typename Clock>
cluster_epoch_service<Clock>::cluster_epoch_service(
  model::node_id self, ss::sharded<rpc::connection_cache>* cc) noexcept
  : cluster_epoch_service<Clock>([this, self, cc](ss::abort_source* as) {
      vassert(
        _shard0_state,
        "expected shard0 state to be set and this only called on shard0");
      return do_fetch_leader_epoch_impl(
        self, cc, as, [this]() { return _shard0_state->current_leader(); });
  }) {}

template<typename Clock>
cluster_epoch_service<Clock>::cluster_epoch_service(
  ss::noncopyable_function<ss::future<std::expected<int64_t, std::error_code>>(
    ss::abort_source*)> fn) noexcept
  : _do_fetch_leader_epoch_fn(std::move(fn)) {}

template<typename Clock>
cluster_epoch_service<Clock>::~cluster_epoch_service() noexcept = default;

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::start() {
    co_return;
}
template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::stop() {
    _abort_source.request_abort();
    co_await _gate.close();
    if (!_shard0_state) {
        co_return;
    }
    co_await _shard0_state->stop();
    _shard0_state = nullptr;
}

template<typename Clock>
ss::future<> cluster_epoch_service<Clock>::invalidate_epoch_cache(
  int64_t epoch_causing_monotonicity_violation) {
    auto holder = _gate.hold();
    // Don't do the cross shard calls if our local shard is up to date or we
    // already are going to get a new epoch from another invalidation.
    if (
      _cached_epoch > epoch_causing_monotonicity_violation
      || _cached_epoch_time == Clock::time_point::min()) {
        co_return;
    }
    co_await this->container().invoke_on_all(
      [epoch_causing_monotonicity_violation](cluster_epoch_service<Clock>& s) {
          s._gate.check();
          // This is safe to check without the lock, because we only use the
          // lock to limit the number of cross shard calls and RPCs.
          //
          // We do want <= here because the epoch passed in is the epoch
          // returned from `get_cached_epoch`, that caused the sequence
          // violation (so it's not the epoch that could have came from another
          // node, and since it's local we know *another node* observed a higher
          // epoch and we need to refetch the epoch).
          if (s._cached_epoch <= epoch_causing_monotonicity_violation) {
              s._cached_epoch_time = Clock::time_point::min();
              // We can't set the update time because we only advance that if
              // the epoch changes, so we may run into a situation where we
              // don't update the epoch and that causes epoch requests to fail
          }
      });
}

template<typename Clock>
ss::future<std::expected<int64_t, std::error_code>>
cluster_epoch_service<Clock>::get_cached_epoch(seastar::abort_source* as) {
    auto holder = _gate.hold();
    // If the cache entry is needing an update, then block until
    // that update is complete.
    if (cache_entry_needs_updated()) {
        auto units = co_await _mu.get_units(*as);
        if (cache_entry_needs_updated()) {
            auto ec = co_await do_update_epoch(as);
            if (ec) {
                co_return std::unexpected(ec);
            }
            if (cache_entry_needs_updated()) {
                if (_epoch_updated_time != Clock::time_point::min()) {
                    vlog(
                      clusterlog.warn,
                      "epoch too old, has not successfully updated in {}",
                      std::chrono::duration_cast<std::chrono::seconds>(
                        Clock::now() - _epoch_updated_time));
                } else {
                    vlog(
                      clusterlog.warn,
                      "epoch invalid, has not successfully updated");
                }
                co_return std::unexpected(errc::timeout);
            }
        }
    }
    // If the cache entry is expired, then we can spawn an update in the
    // background to update the epoch. Use the lock to ensure we only have
    // a single
    if (cache_entry_expired()) {
        auto maybe_units = _mu.try_get_units();
        if (maybe_units) {
            ssx::spawn_with_gate(
              _gate, [this, units = std::move(*maybe_units)]() mutable {
                  return do_update_epoch(&_abort_source)
                    .then([](std::error_code ec) {
                        if (ec) {
                            vlog(
                              clusterlog.warn,
                              "Failed to update cluster epoch async: {}",
                              ec);
                        }
                    })
                    .finally([units = std::move(units)] {});
              });
        }
    }
    co_return _cached_epoch;
}

template<typename Clock>
ss::future<std::error_code>
cluster_epoch_service<Clock>::do_update_epoch(ss::abort_source* as) {
    std::optional<int64_t> maybe_epoch;
    typename Clock::time_point update_time;
    if (ss::this_shard_id() == controller_stm_shard) {
        // If we are the leader, get our epoch
        maybe_epoch = co_await get_current_epoch();
        update_time = Clock::now();
        // Otherwise go fetch from the leader the epoch
        if (!maybe_epoch) {
            auto epoch_result = co_await fetch_leader_epoch(as);
            if (!epoch_result) {
                co_return epoch_result.error();
            }
            maybe_epoch = epoch_result.value();
            update_time = Clock::now();
        }
    } else {
        // If we're not shard0 we have to ask shard0 for it's epoch, we strictly
        // follow the epoch on shard0.
        ssx::sharded_abort_source sharded_as;
        co_await sharded_as.start(*as);
        using result_t = std::invoke_result_t<
          decltype(&cluster_epoch_service<Clock>::shard0_get_epoch),
          decltype(this),
          ssx::sharded_abort_source*>::value_type;
        auto result_fut = co_await ss::coroutine::as_future<result_t>(
          this->container().invoke_on(
            controller_stm_shard,
            &cluster_epoch_service<Clock>::shard0_get_epoch,
            &sharded_as));
        co_await sharded_as.stop();
        if (result_fut.failed()) {
            co_await ss::coroutine::return_exception_ptr(
              result_fut.get_exception());
        }
        auto result = result_fut.get();
        if (!result) {
            co_return result.error();
        }
        auto [epoch0, update_time0] = result.value();
        maybe_epoch = epoch0;
        // Change our update_time to be the time from shard0, this prevents
        // cases where non-shard0 could have an epoch twice as stale as the
        // configuration knob for cache expiration.
        update_time = update_time0;
    }
    vlog(clusterlog.debug, "updated cluster epoch to {}", maybe_epoch);
    int64_t new_epoch = maybe_epoch.value();
    vassert(
      new_epoch >= _cached_epoch && new_epoch >= 0,
      "epochs must monotonically increase and be non-negative, but new "
      "epoch {} is less than the cached epoch of {}",
      new_epoch,
      _cached_epoch);
    _cached_epoch_time = Clock::now();
    if (new_epoch > _cached_epoch) {
        _epoch_updated_time = _cached_epoch_time;
    }
    _cached_epoch = new_epoch;
    co_return std::error_code{};
}

template<typename Clock>
ss::future<std::expected<
  std::tuple<int64_t, typename Clock::time_point>,
  std::error_code>>
cluster_epoch_service<Clock>::shard0_get_epoch(ssx::sharded_abort_source* as) {
    vassert(
      ss::this_shard_id() == controller_stm_shard,
      "must be called from shard0");
    if (!cache_entry_expired()) {
        co_return std::make_tuple(_cached_epoch, _cached_epoch_time);
    }
    auto units = co_await _mu.get_units();
    if (!cache_entry_expired()) {
        co_return std::make_tuple(_cached_epoch, _cached_epoch_time);
    }
    ssx::composite_abort_source combined(as->local(), _abort_source);
    auto ec = co_await do_update_epoch(&combined.as());
    if (ec) {
        co_return std::unexpected(ec);
    }
    co_return std::make_tuple(_cached_epoch, _cached_epoch_time);
}

template<typename Clock>
ss::future<std::expected<int64_t, std::error_code>>
cluster_epoch_service<Clock>::fetch_leader_epoch(ss::abort_source* as) {
    vlog(clusterlog.trace, "Making RPC to fetch leader epoch");
    auto maybe_epoch = co_await _do_fetch_leader_epoch_fn(as);
    if (maybe_epoch) {
        co_return maybe_epoch;
    }
    vlog(
      clusterlog.debug,
      "Failed to fetch leader epoch due to: {}",
      maybe_epoch.error().message());
    co_return maybe_epoch;
}

template<typename Clock>
ss::future<std::optional<int64_t>>
cluster_epoch_service<Clock>::get_current_epoch() {
    _gate.check();
    if (!_shard0_state) {
        // Normally this should not happen, but to allow testing,
        // we just return std::nullopt here.
        co_return std::nullopt;
    }
    co_return _shard0_state->current_epoch().transform(
      [](auto offset) { return offset(); });
}

template<typename Clock>
void cluster_epoch_service<Clock>::set_raft0(
  ss::lw_shared_ptr<raft::consensus> raft0,
  ss::sharded<controller_stm>& stm,
  ss::sharded<raft::group_manager>& group_manager) noexcept {
    _gate.check();
    vassert(
      ss::this_shard_id() == controller_stm_shard && !_shard0_state,
      "raft0 should only be set on shard0, but was set on {}",
      ss::this_shard_id());
    _shard0_state = std::make_unique<raft0_state>(
      std::move(raft0), stm, group_manager);
    _shard0_state->start();
}

template<typename Clock>
bool cluster_epoch_service<Clock>::cache_entry_expired() const noexcept {
    return Clock::now()
           > (_cached_epoch_time + config::shard_local_cfg().cloud_topics_epoch_service_local_epoch_cache_duration());
}
template<typename Clock>
bool cluster_epoch_service<Clock>::cache_entry_needs_updated() const noexcept {
    const auto max_same_epoch_cache_duration
      = config::shard_local_cfg()
          .cloud_topics_epoch_service_max_same_epoch_duration();
    return Clock::now() > (_epoch_updated_time + max_same_epoch_cache_duration)
           || Clock::now()
                > (_cached_epoch_time + max_same_epoch_cache_duration);
}

template class cluster_epoch_service<ss::lowres_clock>;
template class cluster_epoch_service<ss::manual_clock>;

} // namespace cluster
