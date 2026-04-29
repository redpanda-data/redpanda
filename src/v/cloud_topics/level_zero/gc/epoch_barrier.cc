/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier.h"

#include "cloud_topics/inflight_write_tracker.h"
#include "cloud_topics/level_zero/gc/rpc_service.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/logger.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/members_table.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"
#include "ssx/when_all.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0::gc {

// ---------------------------------------------------------------------------
// Production implementations of partition_source and node_source,
// backed by cluster::partition_manager and cluster::members_table.
// ---------------------------------------------------------------------------

namespace {

class partition_manager_source : public epoch_barrier::partition_source {
public:
    explicit partition_manager_source(cluster::partition_manager& pm)
      : _pm(pm) {}

    ss::shared_ptr<cloud_topics::ctp_stm>
    get_ctp_stm(cluster::partition& p) const {
        return p.raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    }

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, partition] : _pm.partitions()) {
            if (ntp.ns != model::kafka_namespace) {
                continue;
            }
            auto ctp_stm = get_ctp_stm(*partition);
            if (!ctp_stm) {
                continue;
            }
            result.emplace_back(
              ntp,
              info{
                .term = partition->term(),
                .is_leader = partition->is_leader(),
                .has_epoch
                = ctp_stm->state().get_max_applied_epoch().has_value(),
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto p = _pm.get(ntp);
        if (!p) {
            return std::nullopt;
        }
        auto ctp_stm = get_ctp_stm(*p);
        if (!ctp_stm) {
            return std::nullopt;
        }
        return info{
          .term = p->term(),
          .is_leader = p->is_leader(),
          .has_epoch = ctp_stm->state().get_max_applied_epoch().has_value(),
        };
    }

    ss::future<bool> write_gc_epoch(
      const model::ntp& ntp,
      cluster_epoch epoch,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as) override {
        auto p = _pm.get(ntp);
        if (!p || !p->is_leader()) {
            co_return false;
        }
        auto stm = get_ctp_stm(*p);
        if (!stm) {
            co_return false;
        }
        cloud_topics::ctp_stm_api api(stm);
        auto result = co_await api.advance_gc_epoch(epoch, deadline, as);
        co_return result.has_value();
    }

private:
    cluster::partition_manager& _pm;
};

class members_table_node_source : public epoch_barrier::node_source {
public:
    members_table_node_source(
      model::node_id self, cluster::members_table& members)
      : _self(self)
      , _members(members) {}

    model::node_id self() const override { return _self; }
    std::vector<model::node_id> node_ids() const override {
        return _members.node_ids();
    }

private:
    model::node_id _self;
    cluster::members_table& _members;
};

} // namespace

// ---------------------------------------------------------------------------
// Leader-side background loop. Runs on shard 0 when this node holds L1
// metastore partition 0 leadership. Each iteration computes a candidate
// epoch, fans out advance_barrier to all nodes, and polls until
// convergence or max attempts.
// ---------------------------------------------------------------------------

using proto_t = rpc::impl::epoch_barrier_rpc_client_protocol;
static constexpr auto rpc_timeout = std::chrono::seconds(10);

class epoch_barrier::barrier_loop {
public:
    explicit barrier_loop(epoch_barrier& parent)
      : _parent(parent) {
        ssx::spawn_with_gate(_gate, [this] { return run_loop(); });
    }

    ss::future<> stop() noexcept {
        vlog(cd_log.debug, "Epoch barrier loop stopping...");
        _as.request_abort();
        if (!_gate.is_closed()) {
            co_await _gate.close();
        }
        vlog(cd_log.debug, "Epoch barrier loop stopped");
    }

private:
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

            (co_await ss::coroutine::as_future(
               ss::sleep_abortable(
                 config::shard_local_cfg()
                   .cloud_topics_gc_barrier_loop_interval(),
                 _as)))
              .ignore_ready_future();
        }
    }

    ss::future<> run_once() {
        // Step 1: Get the candidate epoch from the cluster epoch service.
        // The candidate is the current cluster epoch. Cache invalidation
        // during the round ensures new writes get epoch > candidate.
        auto epoch_result
          = co_await _parent._epoch_service.local().get_cached_epoch(&_as);
        if (!epoch_result.has_value()) {
            vlog(
              cd_log.debug,
              "Epoch barrier: could not get cluster epoch: {}",
              epoch_result.error().message());
            co_return;
        }
        auto candidate = cluster_epoch(epoch_result.value());

        vlog(
          cd_log.info,
          "Epoch barrier: starting round for candidate {}",
          candidate);

        // Step 2: Fan out advance_barrier to all nodes until all ready
        // or max attempts exhausted. The confirmed epoch from the previous
        // round is piggybacked so nodes can publish it.
        for (size_t attempt = 0; attempt < max_poll_attempts; ++attempt) {
            if (_as.abort_requested()) {
                co_return;
            }

            auto res = co_await ss::coroutine::as_future(
              _parent.fan_out_advance_barrier(candidate, _confirmed_epoch));
            if (res.failed()) {
                auto ex = res.get_exception();
                if (ssx::is_shutdown_exception(ex)) {
                    co_return;
                }
                vlog(
                  cd_log.warn,
                  "Epoch barrier: fan-out failed for {}: {}",
                  candidate,
                  ex);
            } else if (res.get()) {
                // All nodes drained. Confirm this epoch as safe.
                _confirmed_epoch = candidate;
                vlog(
                  cd_log.info,
                  "Epoch barrier: round complete, confirmed_epoch={}",
                  candidate);
                co_return;
            }

            co_await ss::sleep_abortable(
              config::shard_local_cfg().cloud_topics_gc_barrier_poll_interval(),
              _as);
        }

        vlog(
          cd_log.warn,
          "Epoch barrier: did not converge for candidate {} after {} attempts",
          candidate,
          max_poll_attempts);
    }

    epoch_barrier& _parent;
    std::optional<cluster_epoch> _confirmed_epoch;
    ss::gate _gate;
    ss::abort_source _as;
};

epoch_barrier::~epoch_barrier() = default;

std::unique_ptr<epoch_barrier::node_source>
epoch_barrier::make_default_node_source(
  model::node_id self, cluster::members_table& members) {
    return std::make_unique<members_table_node_source>(self, members);
}

// ---------------------------------------------------------------------------
// Lifecycle: constructor, factories, stop.
// ---------------------------------------------------------------------------

std::unique_ptr<epoch_barrier::partition_source>
epoch_barrier::make_default_partition_source(cluster::partition_manager& pm) {
    return std::make_unique<partition_manager_source>(pm);
}

epoch_barrier::epoch_barrier(
  ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>& epoch_service,
  inflight_write_tracker& tracker,
  std::unique_ptr<partition_source> partitions,
  std::unique_ptr<node_source> nodes,
  ss::sharded<::rpc::connection_cache>* connections)
  : _epoch_service(epoch_service)
  , _tracker(tracker)
  , _partitions(std::move(partitions))
  , _nodes(std::move(nodes))
  , _connections(connections) {}

ss::future<> epoch_barrier::stop() {
    co_await _gate.close();
    if (_loop) {
        co_await _loop->stop();
    }
    if (_drain_future.has_value()) {
        (co_await ss::coroutine::as_future(std::move(*_drain_future)))
          .ignore_ready_future();
        _drain_future.reset();
    }
}

// ---------------------------------------------------------------------------
// Barrier protocol: handle_barrier (per-node RPC handler) and the
// advance_local/advance_remote/fan_out methods that the leader loop
// calls to drive the protocol.
// ---------------------------------------------------------------------------

ss::future<epoch_barrier::barrier_status> epoch_barrier::handle_barrier(
  cluster_epoch candidate, std::optional<cluster_epoch> prev_safe_epoch) {
    vassert(
      ss::this_shard_id() == 0,
      "handle_barrier must run on shard 0, got shard {}",
      ss::this_shard_id());

    // 1. If no round in progress for this candidate, start a new round:
    //    publish the previous safe epoch, invalidate the cache, and kick
    //    off a drain.
    if (!_round || _round->candidate != candidate) {
        vlog(
          cd_log.debug,
          "Epoch barrier: starting round for candidate {}",
          candidate);

        // Publish the safe epoch from the previous round. This is the
        // deferred-publish mechanism: the epoch was confirmed after global
        // drain completion, so no writes at epoch <= prev_safe_epoch are
        // in flight. Fire-and-forget — partitions that fail to receive
        // the command will be retried on the next round. Failures are
        // conservative (gc_safe_epoch stays stale, GC deletes less).
        if (prev_safe_epoch.has_value()) {
            auto res = co_await ss::coroutine::as_future(
              publish_safe_epoch(*prev_safe_epoch));
            if (res.failed()) {
                vlog(
                  cd_log.warn,
                  "Epoch barrier: publish_safe_epoch({}) failed: {}",
                  *prev_safe_epoch,
                  res.get_exception());
            }
        }

        // Reset round state on all shards.
        co_await container().invoke_on_all(
          [](epoch_barrier& b) { b._round.reset(); });

        // Ensure the cluster epoch is past our candidate so new writes
        // get epoch > candidate.
        co_await _epoch_service.local().force_epoch_update(candidate());
        co_await _epoch_service.local().invalidate_epoch_cache(candidate());

        // Drain inflight writes. If a previous round's drain is still
        // in progress, we must wait for it too — those tokens may not
        // have reached raft yet (the batcher pipeline yields during L0
        // upload).
        if (_drain_future.has_value()) {
            _drain_future = ss::when_all(
                              std::move(
                                *std::exchange(_drain_future, std::nullopt)),
                              _tracker.drain())
                              .discard_result();
        } else {
            _drain_future = _tracker.drain();
        }
        _round = round_state{.candidate = candidate};

        co_return barrier_status::pending;
    }

    // 2. If draining, check whether the drain has completed.
    if (_drain_future.has_value()) {
        if (!_drain_future->available()) {
            co_return barrier_status::pending;
        }

        // Drain finished — consume the future and check for errors.
        auto fut = std::exchange(_drain_future, std::nullopt);
        auto res = co_await ss::coroutine::as_future(std::move(*fut));
        if (res.failed()) {
            auto ex = res.get_exception();
            vlog(
              cd_log.warn,
              "Epoch barrier: drain failed for {}: {}",
              candidate,
              ex);
            co_await container().invoke_on_all(
              [](epoch_barrier& b) { b._round.reset(); });
            co_return barrier_status::pending;
        }

        vlog(
          cd_log.debug,
          "Epoch barrier: drain complete for candidate {}",
          candidate);
        co_return barrier_status::ready;
    }

    // Drain already completed on a previous poll — still ready.
    co_return barrier_status::ready;
}

ss::future<bool> epoch_barrier::advance_local(
  cluster_epoch candidate, std::optional<cluster_epoch> prev_safe_epoch) {
    // Dispatch to shard 0 for the same reason as the RPC path:
    // handle_barrier's round state must live on a single shard.
    auto result_f = co_await ss::coroutine::as_future(
      container().invoke_on(0, [candidate, prev_safe_epoch](epoch_barrier& b) {
          return b.handle_barrier(candidate, prev_safe_epoch);
      }));
    if (result_f.failed()) {
        auto ex = result_f.get_exception();
        vlog(cd_log.warn, "handle_barrier({}) failed: {}", candidate, ex);
        co_return false;
    }
    co_return result_f.get() == barrier_status::ready;
}

ss::future<bool> epoch_barrier::advance_remote(
  model::node_id node_id,
  cluster_epoch candidate,
  std::optional<cluster_epoch> prev_safe_epoch) {
    auto timeout = model::timeout_clock::now() + rpc_timeout;
    rpc::barrier_request req{
      .candidate = candidate, .prev_safe_epoch = prev_safe_epoch};
    auto res = co_await _connections->local()
                 .with_node_client<proto_t>(
                   _nodes->self(),
                   ss::this_shard_id(),
                   node_id,
                   rpc_timeout,
                   [req, timeout](proto_t client) mutable {
                       return client.advance_barrier(
                         std::move(req), ::rpc::client_opts{timeout});
                   })
                 .then(&::rpc::get_ctx_data<rpc::barrier_response>);
    if (res.has_error()) {
        vlog(
          cd_log.warn,
          "Epoch barrier: advance_barrier RPC to node {} failed: {}",
          node_id,
          res.error().message());
        co_return false;
    }
    co_return res.value().s == rpc::barrier_response::status::ready;
}

ss::future<bool> epoch_barrier::fan_out_advance_barrier(
  cluster_epoch candidate, std::optional<cluster_epoch> prev_safe_epoch) {
    auto nodes = _nodes->node_ids();
    auto self = _nodes->self();

    auto results = co_await ssx::when_all_succeed<chunked_vector<uint8_t>>(
      std::views::transform(
        nodes,
        [this, self, candidate, prev_safe_epoch](auto node_id) {
            return node_id == self
                     ? advance_local(candidate, prev_safe_epoch)
                     : advance_remote(node_id, candidate, prev_safe_epoch);
        })
      | std::ranges::to<chunked_vector<ss::future<bool>>>());

    co_return std::ranges::all_of(results, [](uint8_t r) { return r != 0; });
}

// ---------------------------------------------------------------------------
// Publish safe epoch: write advance_gc_epoch to all local leader
// partitions. Each shard writes to its own partitions to avoid
// cross-shard partition access.
// ---------------------------------------------------------------------------

ss::future<bool> epoch_barrier::publish_safe_epoch(cluster_epoch epoch) {
    auto deadline = model::timeout_clock::now() + rpc_timeout;
    auto results = co_await container().map(
      [epoch, deadline](epoch_barrier& b) {
          return b.publish_safe_epoch_local(epoch, deadline);
      });
    co_return std::ranges::all_of(results, [](bool ok) { return ok; });
}

ss::future<bool> epoch_barrier::publish_safe_epoch_local(
  cluster_epoch epoch, model::timeout_clock::time_point deadline) {
    ss::abort_source as;
    bool all_ok = true;
    for (const auto& [ntp, pinfo] : _partitions->cloud_topic_partitions()) {
        if (!pinfo.is_leader) {
            continue;
        }
        auto ok = co_await _partitions->write_gc_epoch(
          ntp, epoch, deadline, as);
        if (!ok) {
            vlog(
              cd_log.debug,
              "Epoch barrier: failed to publish safe epoch to {}",
              ntp);
            all_ok = false;
        }
    }
    co_return all_ok;
}

void epoch_barrier::notify_leadership_change(bool is_leader) noexcept {
    ssx::spawn_with_gate(
      _gate, [this, is_leader] { return set_leader(is_leader); });
}

// ---------------------------------------------------------------------------
// Leadership transitions. Starts or stops the barrier_loop when L1
// metastore partition 0 leadership changes on this node.
// ---------------------------------------------------------------------------

ss::future<> epoch_barrier::set_leader(bool is_leader) {
    if (!is_leader) {
        if (_loop) {
            auto loop = std::exchange(_loop, nullptr);
            co_await loop->stop();
        }
        co_return;
    }

    // Already running.
    if (_loop) {
        co_return;
    }

    _loop = std::make_unique<barrier_loop>(*this);
}

} // namespace cloud_topics::l0::gc
