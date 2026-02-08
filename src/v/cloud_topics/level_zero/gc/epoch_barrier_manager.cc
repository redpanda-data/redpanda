/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier_manager.h"

#include "base/vlog.h"
#include "cloud_topics/level_zero/gc/epoch_barrier_coordinator.h"
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/level_zero/gc/rpc_service.h"
#include "cloud_topics/logger.h"
#include "cluster/members_table.h"
#include "rpc/connection_cache.h"
#include "rpc/errc.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0::gc {

using proto_t = rpc::impl::epoch_barrier_rpc_client_protocol;

// Inner loop that runs the barrier protocol periodically.
class epoch_barrier_manager::barrier_loop {
public:
    barrier_loop(
      model::node_id self,
      ss::sharded<cluster::members_table>* members,
      ss::sharded<::rpc::connection_cache>* connections,
      ss::sharded<epoch_barrier_coordinator>* coordinator,
      std::unique_ptr<level_zero_gc::epoch_source> epoch_source,
      std::optional<cluster_epoch>& safe_epoch)
      : _self(self)
      , _members(members)
      , _connections(connections)
      , _coordinator(coordinator)
      , _epoch_source(std::move(epoch_source))
      , _safe_epoch(safe_epoch) {}

    void start() {
        ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
    }

    ss::future<> stop_and_wait() {
        vlog(cd_log.debug, "Epoch barrier loop stopping...");
        _as.request_abort();
        _sem.broken();
        co_await _gate.close();
        vlog(cd_log.debug, "Epoch barrier loop stopped");
    }

private:
    static constexpr auto rpc_timeout = std::chrono::seconds(10);
    // The reconciler polls partitions on an adaptive interval up to 10s,
    // so polling faster than that just burns RPC round-trips for no gain.
    static constexpr auto poll_interval = std::chrono::seconds(2);
    static constexpr auto loop_interval = std::chrono::seconds(5);
    static constexpr size_t max_poll_attempts = 30;

    ss::future<> run_loop() {
        while (!_as.abort_requested()) {
            auto res = co_await ss::coroutine::as_future(run_once());
            if (res.failed()) {
                auto ex = res.get_exception();
                auto log_lvl = ssx::is_shutdown_exception(ex)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(cd_log, log_lvl, "Epoch barrier round failed: {}", ex);
            }

            if (_as.abort_requested()) {
                break;
            }

            try {
                // TODO(oren): sleep abortable instead probaby
                co_await _sem.wait(
                  loop_interval, std::max(_sem.current(), size_t(1)));
            } catch (const ss::semaphore_timed_out&) {
                // Normal wakeup.
                std::ignore = std::current_exception();
            } catch (...) {
                auto eptr = std::current_exception();
                auto log_lvl = ssx::is_shutdown_exception(eptr)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(
                  cd_log,
                  log_lvl,
                  "Epoch barrier loop sleep interrupted: {}",
                  eptr);
            }
        }
    }

    ss::future<> run_once() {
        // Step 1: Get the candidate epoch from the epoch source.
        auto candidate_result
          = co_await _epoch_source->max_barrier_candidate_epoch(&_as);
        if (!candidate_result.has_value()) {
            vlog(
              cd_log.debug,
              "Epoch barrier: could not get candidate epoch: {}",
              candidate_result.error());
            co_return;
        }
        auto candidate_opt = candidate_result.value();
        if (!candidate_opt.has_value()) {
            vlog(
              cd_log.trace, "Epoch barrier: no candidate epoch available yet");
            co_return;
        }
        auto candidate = candidate_opt.value();

        vlog(
          cd_log.info,
          "Epoch barrier: starting round for candidate {}",
          candidate);

        // Step 2: Fan-out Invalidate(C) to all nodes.
        auto invalidate_ok = co_await fan_out_invalidate(candidate);
        if (!invalidate_ok) {
            vlog(
              cd_log.warn,
              "Epoch barrier: invalidate fan-out failed for candidate {}",
              candidate);
            co_return;
        }

        // Step 3: Poll until all nodes report drained.
        auto drained = co_await poll_until_drained(candidate);
        if (!drained) {
            vlog(
              cd_log.warn,
              "Epoch barrier: poll_drain did not converge for candidate {}",
              candidate);
            co_return;
        }

        // Step 4: Fan out the safe epoch to all coordinators.
        auto publish_ok = co_await fan_out_publish(candidate);
        if (!publish_ok) {
            // TODO(oren): should we try again at some point? cached safe_epoch
            // should remain safe forever, right?
            vlog(
              cd_log.warn,
              "Epoch barrier: publish fan-out failed for candidate {}, "
              "setting local safe epoch only",
              candidate);
        }
        _safe_epoch = candidate;
        vlog(
          cd_log.info,
          "Epoch barrier: published safe-to-GC epoch {}",
          candidate);
    }

    ss::future<bool> fan_out_invalidate(cluster_epoch candidate) {
        auto nodes = _members->local().node_ids();
        rpc::invalidate_epoch_request req{.candidate = candidate};

        vlog(
          cd_log.debug,
          "Fan out invalidate to nodes: {{{}}}",
          fmt::join(nodes, ","));

        for (auto node_id : nodes) {
            if (_as.abort_requested()) {
                co_return false;
            }

            vlog(cd_log.debug, "Invalidating cached epoch on {}", node_id);

            if (node_id == _self) {
                auto result = co_await _coordinator->local().invalidate(
                  candidate);
                if (!result.has_value()) {
                    vlog(
                      cd_log.warn,
                      "Epoch barrier: local invalidate failed: {}",
                      result.error());
                    co_return false;
                }
                continue;
            }

            auto timeout = model::timeout_clock::now() + rpc_timeout;
            auto res
              = co_await _connections->local()
                  .with_node_client<proto_t>(
                    _self,
                    ss::this_shard_id(),
                    node_id,
                    rpc_timeout,
                    [req, timeout](proto_t client) mutable {
                        return client.invalidate_epoch(
                          std::move(req), ::rpc::client_opts{timeout});
                    })
                  .then(&::rpc::get_ctx_data<rpc::invalidate_epoch_response>);
            if (res.has_error()) {
                vlog(
                  cd_log.warn,
                  "Epoch barrier: invalidate RPC to node {} failed: {}",
                  node_id,
                  res.error().message());
                co_return false;
            }
            if (res.value().ec != ::rpc::errc::success) {
                vlog(
                  cd_log.warn,
                  "Epoch barrier: invalidate on node {} returned error: {}",
                  node_id,
                  res.value().ec);
                co_return false;
            }
            vlog(cd_log.warn, "Successfully invalidaded epoch on {}", node_id);
        }

        co_return true;
    }

    // TODO(oren):  I'm not crazy about the polling here.
    ss::future<bool> poll_until_drained(cluster_epoch candidate) {
        for (size_t attempt = 0; attempt < max_poll_attempts; ++attempt) {
            if (_as.abort_requested()) {
                co_return false;
            }

            auto all_drained = co_await poll_all_nodes(candidate);
            if (all_drained) {
                co_return true;
            }

            // TODO(oren): add jitter
            co_await ss::sleep_abortable(poll_interval, _as);
        }

        co_return false;
    }

    ss::future<bool> poll_all_nodes(cluster_epoch candidate) {
        auto nodes = _members->local().node_ids();
        rpc::poll_drain_request req{.candidate = candidate};

        for (auto node_id : nodes) {
            if (_as.abort_requested()) {
                co_return false;
            }

            if (node_id == _self) {
                auto result = co_await _coordinator->local().poll_drain(
                  candidate);
                if (!result.has_value() || !result.value()) {
                    co_return false;
                }
                continue;
            }

            auto timeout = model::timeout_clock::now() + rpc_timeout;
            auto res = co_await _connections->local()
                         .with_node_client<proto_t>(
                           _self,
                           ss::this_shard_id(),
                           node_id,
                           rpc_timeout,
                           [req, timeout](proto_t client) mutable {
                               return client.poll_drain(
                                 std::move(req), ::rpc::client_opts{timeout});
                           })
                         .then(&::rpc::get_ctx_data<rpc::poll_drain_response>);
            if (res.has_error()) {
                vlog(
                  cd_log.debug,
                  "Epoch barrier: poll_drain RPC to node {} failed: {}",
                  node_id,
                  res.error().message());
                co_return false;
            }
            if (
              res.value().ec != ::rpc::errc::success || !res.value().drained) {
                co_return false;
            }
        }

        co_return true;
    }

    ss::future<bool> fan_out_publish(cluster_epoch safe_epoch) {
        auto nodes = _members->local().node_ids();
        rpc::publish_safe_epoch_request req{.safe_epoch = safe_epoch};

        for (auto node_id : nodes) {
            if (_as.abort_requested()) {
                co_return false;
            }

            if (node_id == _self) {
                auto result = co_await _coordinator->local().publish_safe_epoch(
                  safe_epoch);
                if (!result.has_value()) {
                    vlog(
                      cd_log.warn,
                      "Epoch barrier: local publish_safe_epoch failed: {}",
                      result.error());
                    co_return false;
                }
                continue;
            }

            auto timeout = model::timeout_clock::now() + rpc_timeout;
            auto res
              = co_await _connections->local()
                  .with_node_client<proto_t>(
                    _self,
                    ss::this_shard_id(),
                    node_id,
                    rpc_timeout,
                    [req, timeout](proto_t client) mutable {
                        return client.publish_safe_epoch(
                          std::move(req), ::rpc::client_opts{timeout});
                    })
                  .then(&::rpc::get_ctx_data<rpc::publish_safe_epoch_response>);
            if (res.has_error()) {
                vlog(
                  cd_log.warn,
                  "Epoch barrier: publish_safe_epoch RPC to node {} "
                  "failed: {}",
                  node_id,
                  res.error().message());
                co_return false;
            }
            if (res.value().ec != ::rpc::errc::success) {
                vlog(
                  cd_log.warn,
                  "Epoch barrier: publish_safe_epoch on node {} returned "
                  "error: {}",
                  node_id,
                  res.value().ec);
                co_return false;
            }
        }

        co_return true;
    }

    model::node_id _self;
    ss::sharded<cluster::members_table>* _members;
    ss::sharded<::rpc::connection_cache>* _connections;
    ss::sharded<epoch_barrier_coordinator>* _coordinator;
    std::unique_ptr<level_zero_gc::epoch_source> _epoch_source;
    std::optional<cluster_epoch>& _safe_epoch;
    ss::gate _gate;
    ss::abort_source _as;
    ssx::semaphore _sem{0, "barrier_loop"};
};

epoch_barrier_manager::epoch_barrier_manager(
  model::node_id self,
  ss::sharded<cluster::members_table>* members,
  ss::sharded<::rpc::connection_cache>* connections,
  ss::sharded<epoch_barrier_coordinator>* coordinator,
  ss::sharded<cluster::health_monitor_frontend>* health_monitor,
  ss::sharded<cluster::controller_stm>* controller_stm,
  ss::sharded<cluster::topic_table>* topic_table)
  : _self(self)
  , _members(members)
  , _connections(connections)
  , _coordinator(coordinator)
  , _health_monitor(health_monitor)
  , _controller_stm(controller_stm)
  , _topic_table(topic_table) {}

epoch_barrier_manager::~epoch_barrier_manager() = default;

void epoch_barrier_manager::enqueue_loop_reset(needs_loop needs) {
    tell(needs);
}

ss::future<> epoch_barrier_manager::process(needs_loop needs) {
    return reset_loop(needs);
}

void epoch_barrier_manager::on_error(std::exception_ptr ex) noexcept {
    vlog(cd_log.error, "Unexpected epoch barrier manager error: {}", ex);
}

ss::future<> epoch_barrier_manager::reset_loop(needs_loop needs) {
    if (!needs) {
        if (_loop) {
            auto loop = std::exchange(_loop, nullptr);
            auto stop_fut = co_await ss::coroutine::as_future(
              loop->stop_and_wait());
            if (stop_fut.failed()) {
                auto ex = stop_fut.get_exception();
                vlog(
                  cd_log.error, "Stopping epoch barrier loop failed: {}", ex);
            }
        }
        co_return;
    }
    if (_loop) {
        co_return;
    }
    // Create the epoch source on demand. This reuses the same
    // epoch_source_impl that level_zero_gc uses internally.
    auto epoch_source = level_zero_gc::make_epoch_source(
      _health_monitor, _controller_stm, _topic_table);
    auto loop = std::make_unique<barrier_loop>(
      _self,
      _members,
      _connections,
      _coordinator,
      std::move(epoch_source),
      _safe_epoch);
    loop->start();
    _loop = std::move(loop);
}

ss::future<> epoch_barrier_manager::stop() {
    co_await actor::stop();
    if (_loop) {
        auto fut = co_await ss::coroutine::as_future(_loop->stop_and_wait());
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(cd_log.error, "Error stopping epoch barrier manager: {}", ex);
        }
    }
}

} // namespace cloud_topics::l0::gc
