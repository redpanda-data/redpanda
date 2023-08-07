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
#include "cluster/scheduling/leader_balancer.h"

#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/leader_balancer_greedy.h"
#include "cluster/scheduling/leader_balancer_random.h"
#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/rpc_client_protocol.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <memory>

namespace cluster {

leader_balancer::leader_balancer(
  topic_table& topics,
  partition_leaders_table& leaders,
  members_table& members,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<ss::abort_source>& as,
  config::binding<bool>&& enabled,
  config::binding<std::chrono::milliseconds>&& idle_timeout,
  config::binding<std::chrono::milliseconds>&& mute_timeout,
  config::binding<std::chrono::milliseconds>&& node_mute_timeout,
  config::binding<size_t>&& transfer_limit_per_shard,
  consensus_ptr raft0)
  : _enabled(std::move(enabled))
  , _idle_timeout(std::move(idle_timeout))
  , _mute_timeout(std::move(mute_timeout))
  , _node_mute_timeout(std::move(node_mute_timeout))
  , _transfer_limit_per_shard(std::move(transfer_limit_per_shard))
  , _topics(topics)
  , _leaders(leaders)
  , _members(members)
  , _connections(connections)
  , _shard_table(shard_table)
  , _partition_manager(partition_manager)
  , _as(as)
  , _raft0(std::move(raft0))
  , _timer([this] { trigger_balance(); }) {
    if (!config::shard_local_cfg().disable_metrics()) {
        _probe.setup_metrics();
    }
}

void leader_balancer::check_if_controller_leader(
  model::ntp, model::term_id, std::optional<model::node_id>) {
    // Don't bother doing anything if it's not enabled
    if (!_enabled()) {
        return;
    }

    // Active leader balancer again if leadership of
    // raft0 is transfered to this node.
    if (_raft0->is_elected_leader()) {
        vlog(
          clusterlog.info,
          "Leader balancer: controller leadership detected. "
          "Starting "
          "rebalancer in {} seconds",
          std::chrono::duration_cast<std::chrono::seconds>(
            leader_activation_delay)
            .count());

        _timer.cancel();
        _timer.arm(leader_activation_delay);
    } else {
        vlog(clusterlog.info, "Leader balancer: node is not controller leader");
        _need_controller_refresh = true;
        _timer.cancel();
        _timer.arm(_idle_timeout());
    }
}

void leader_balancer::on_leadership_change(
  model::ntp ntp, model::term_id, std::optional<model::node_id>) {
    if (!_enabled()) {
        return;
    }

    if (!_raft0->is_elected_leader()) {
        return;
    }

    const auto assignment = _topics.get_partition_assignment(ntp);

    if (!assignment.has_value()) {
        return;
    }

    const auto& group = assignment->group;

    // Update in flight state
    if (auto it = _in_flight_changes.find(group);
        it != _in_flight_changes.end()) {
        _in_flight_changes.erase(it);
        check_unregister_leadership_change_notification();

        if (_throttled) {
            _throttled = false;
            _timer.cancel();
            _timer.arm(throttle_reactivation_delay);
        }
    }
}

void leader_balancer::on_maintenance_change(
  model::node_id, model::maintenance_state ms) {
    if (!_enabled()) {
        return;
    }

    if (!_raft0->is_elected_leader()) {
        return;
    }

    // if a node transitions out of maintenance wake up the balancer early to
    // transfer leadership back to it.
    if (ms == model::maintenance_state::inactive) {
        _timer.cancel();
        _timer.arm(leader_activation_delay);
    }
}

void leader_balancer::check_register_leadership_change_notification() {
    if (!_leadership_change_notify_handle && _in_flight_changes.size() > 0) {
        _leadership_change_notify_handle
          = _leaders.register_leadership_change_notification(std::bind_front(
            std::mem_fn(&leader_balancer::on_leadership_change), this));
    }
}

void leader_balancer::check_unregister_leadership_change_notification() {
    if (_leadership_change_notify_handle && _in_flight_changes.size() == 0) {
        _leaders.unregister_leadership_change_notification(
          *_leadership_change_notify_handle);
        _leadership_change_notify_handle.reset();
    }
}

ss::future<> leader_balancer::start() {
    /*
     * register for raft0 leadership change notifications. shutdown the balancer
     * when we lose leadership, and start it when we gain leadership.
     */
    _leader_notify_handle = _leaders.register_leadership_change_notification(
      _raft0->ntp(),
      std::bind_front(
        std::mem_fn(&leader_balancer::check_if_controller_leader), this));

    _maintenance_state_notify_handle
      = _members.register_maintenance_state_change_notification(std::bind_front(
        std::mem_fn(&leader_balancer::on_maintenance_change), this));

    /*
     * register_leadership_notification above may run callbacks synchronously
     * during registration, so make sure the timer is unarmed before arming.
     */
    if (!_timer.armed()) {
        _timer.arm(_idle_timeout());
    }

    _enabled.watch([this]() { on_enable_changed(); });

    co_return;
}

ss::future<> leader_balancer::stop() {
    vlog(clusterlog.info, "Stopping Leader Balancer...");
    _leaders.unregister_leadership_change_notification(
      _raft0->ntp(), _leader_notify_handle);
    _members.unregister_maintenance_state_change_notification(
      _maintenance_state_notify_handle);
    _timer.cancel();
    return _gate.close();
}

/**
 * Hook for changes to enable_leader_balancer config property
 */
void leader_balancer::on_enable_changed() {
    if (_gate.is_closed()) {
        return;
    }

    if (_enabled()) {
        vlog(clusterlog.info, "Leader balancer enabled");
        if (!_timer.armed()) {
            _timer.arm(_idle_timeout());
        }
    } else {
        vlog(clusterlog.info, "Leader balancer disabled");
        _timer.cancel();
    }
}

void leader_balancer::trigger_balance() {
    /*
     * there are many ways in which the rebalance fiber may not exit quickly
     * (e.g. rpc timeouts) because of this, it is possible for leadership to
     * change (e.g. lose, and then regain) to trigger an upcall that starts the
     * rebalance process before the previous fiber has exited. when it appears
     * that this has happened avoid starting a new balancer fiber. however,
     * schedule a retry because it could be that we were also racing with the
     * previous fiber on its way out.
     */
    if (_gate.get_count()) {
        vlog(
          clusterlog.info, "Cannot start rebalance until previous fiber exits");
        _timer.arm(_idle_timeout());
        return;
    }

    if (!_enabled()) {
        return;
    }

    // If the balancer is resumed after being throttled
    // reset the flag.
    if (_throttled) {
        _throttled = false;
    }

    ssx::spawn_with_gate(_gate, [this] {
        return ss::repeat([this] {
                   if (_as.local().abort_requested()) {
                       return ss::make_ready_future<ss::stop_iteration>(
                         ss::stop_iteration::yes);
                   }
                   return balance();
               })
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([](const ss::sleep_aborted&) {})
          .handle_exception_type([this](const std::exception& e) {
              vlog(
                clusterlog.info,
                "Leadership rebalance experienced an unhandled error: {}. "
                "Retrying in {} seconds",
                e,
                std::chrono::duration_cast<std::chrono::seconds>(
                  _idle_timeout())
                  .count());
              _timer.cancel();
              _timer.arm(_idle_timeout());
          });
    });
}

bool leader_balancer::should_stop_balance() const {
    return !_enabled() || _as.local().abort_requested();
}

static bool validate_indexes(
  const leader_balancer_types::group_id_to_topic_revision_t& group_to_topic,
  const leader_balancer_types::index_type& index) {
    // Ensure every group in the shard index has a
    // topic mapping in the group_to_topic index.
    // It's an implicit assumption of the even_topic_distributon_constraint
    // that this is the case.
    for (const auto& broker_shard : index) {
        for (const auto& group_p : broker_shard.second) {
            auto topic_id_opt = group_to_topic.find(group_p.first);
            if (topic_id_opt == group_to_topic.end()) {
                vlog(
                  clusterlog.warn,
                  "no topic mapping in group_to_topic index for group: {}",
                  group_p.first);
                return false;
            }
        }
    }

    return true;
}

ss::future<ss::stop_iteration> leader_balancer::balance() {
    if (should_stop_balance()) {
        co_return ss::stop_iteration::yes;
    }

    /*
     * GC the muted, last leader, and in flight changes indices.
     */
    absl::erase_if(
      _muted, [now = clock_type::now()](auto g) { return now >= g.second; });

    absl::erase_if(_last_leader, [now = clock_type::now()](auto g) {
        return now >= g.second.expires;
    });

    absl::erase_if(
      _in_flight_changes, [this, now = clock_type::now()](const auto& c) {
          const auto& [group, change] = c;

          if (now >= change.expires) {
              _probe.leader_transfer_timeout();
              vlog(
                clusterlog.info,
                "Metadata propagation for leadership movement of "
                "group {} from {} to {} timed out",
                change.value.group,
                change.value.from,
                change.value.to);

              return true;
          }

          return false;
      });
    check_unregister_leadership_change_notification();

    if (!_raft0->is_elected_leader()) {
        vlog(clusterlog.debug, "Leadership balancer tick: not leader");
        if (!_timer.armed()) {
            _timer.arm(_idle_timeout());
        }
        _need_controller_refresh = true;
        co_return ss::stop_iteration::yes;
    } else if (_members.node_count() == 1) {
        vlog(clusterlog.trace, "Leadership balancer tick: single node cluster");
        co_return ss::stop_iteration::yes;
    }

    /*
     * if we are running the rebalancer after having lost leadership, then
     * inject a barrier into the raft0 group which will cause us to wait
     * until the controller has replayed its log. if an error occurs, retry
     * after a short delay to account for transient issues on startup.
     */
    if (_need_controller_refresh) {
        // Invalidate whatever in flight changes exist.
        // They would not be accurate post-leadership loss.
        _in_flight_changes.clear();
        check_unregister_leadership_change_notification();

        auto res = co_await _raft0->linearizable_barrier();
        if (!res) {
            vlog(
              clusterlog.debug,
              "Leadership balancer tick: failed to wait on controller "
              "update: {}. Retrying in {} seconds",
              res.error().message(),
              std::chrono::duration_cast<std::chrono::seconds>(
                leader_activation_delay)
                .count());
            if (!_timer.armed()) {
                _timer.arm(leader_activation_delay);
            }
            co_return ss::stop_iteration::yes;
        }
        _need_controller_refresh = false;
    }

    auto index = build_index();
    auto group_id_to_topic = build_group_id_to_topic_rev();

    if (!validate_indexes(group_id_to_topic, index)) {
        vlog(clusterlog.warn, "Leadership balancer tick: invalid indexes.");
        co_return ss::stop_iteration::no;
    }

    auto mode = config::shard_local_cfg().leader_balancer_mode();
    std::unique_ptr<leader_balancer_strategy> strategy;

    switch (mode) {
    case model::leader_balancer_mode::random_hill_climbing:
        vlog(clusterlog.debug, "using random_hill_climbing");
        strategy = std::make_unique<
          leader_balancer_types::random_hill_climbing_strategy>(
          std::move(index),
          std::move(group_id_to_topic),
          leader_balancer_types::muted_index{muted_nodes(), {}});
        break;
    case model::leader_balancer_mode::greedy_balanced_shards:
        vlog(clusterlog.debug, "using greedy_balanced_shards");
        strategy = std::make_unique<greedy_balanced_shards>(
          std::move(index), muted_nodes());
        break;
    default:
        vlog(clusterlog.error, "unexpected mode value: {}", mode);
        co_return ss::stop_iteration::no;
    }

    auto cores = strategy->stats();

    if (clusterlog.is_enabled(ss::log_level::trace)) {
        for (const auto& core : cores) {
            vlog(
              clusterlog.trace,
              "Leadership balancing stats: core {} leaders {}",
              core.shard,
              core.leaders);
        }
    }

    if (cores.size() == 0) {
        vlog(
          clusterlog.debug, "Leadership balancer tick: no topics to balance.");

        if (!_timer.armed()) {
            _timer.arm(_idle_timeout());
        }

        co_return ss::stop_iteration::yes;
    }

    size_t allowed_change_cnt = 0;
    size_t max_inflight_changes = _transfer_limit_per_shard() * cores.size();
    if (_in_flight_changes.size() < max_inflight_changes) {
        allowed_change_cnt = max_inflight_changes - _in_flight_changes.size();
    }

    if (allowed_change_cnt == 0) {
        vlog(
          clusterlog.debug,
          "Leadership balancer tick: number of in flight changes is at max "
          "allowable. Current in flight {}. Max allowable {}.",
          _in_flight_changes.size(),
          max_inflight_changes);

        _throttled = true;

        if (_timer.armed()) {
            co_return ss::stop_iteration::yes;
        }

        // Find change that will time out the soonest and wait for it to timeout
        // before running the balancer again.
        auto min_timeout = std::min_element(
          _in_flight_changes.begin(),
          _in_flight_changes.end(),
          [](const auto& a, const auto& b) {
              return a.second.expires < b.second.expires;
          });

        if (min_timeout != _in_flight_changes.end()) {
            _timer.arm(std::chrono::abs(
              min_timeout->second.expires - clock_type::now()));
        } else {
            _timer.arm(_mute_timeout());
        }

        co_return ss::stop_iteration::yes;
    }

    for (size_t i = 0; i < allowed_change_cnt; i++) {
        if (should_stop_balance()) {
            co_return ss::stop_iteration::yes;
        }

        auto transfer = strategy->find_movement(muted_groups());
        if (!transfer) {
            vlog(
              clusterlog.debug,
              "No leadership balance improvements found with total delta {}, "
              "number of muted groups {}",
              strategy->error(),
              _muted.size());
            if (!_timer.armed()) {
                _timer.arm(_idle_timeout());
            }
            _probe.leader_transfer_no_improvement();
            co_return ss::stop_iteration::yes;
        }

        _in_flight_changes[transfer->group] = {
          *transfer, clock_type::now() + _mute_timeout()};
        check_register_leadership_change_notification();

        auto success = co_await do_transfer(*transfer);
        if (!success) {
            vlog(
              clusterlog.info,
              "Error transferring leadership group {} from {} to {}",
              transfer->group,
              transfer->from,
              transfer->to);

            _in_flight_changes.erase(transfer->group);
            check_unregister_leadership_change_notification();

            /*
             * a common scenario is that a node loses all its leadership (e.g.
             * restarts) and then it is recognized as having lots of extra
             * capacity (which it does). but the balancer doesn't consider node
             * health when making decisions. so when we fail transfer we inject
             * a short delay to avoid spinning on sending transfer requests to a
             * failed node. of course failure can happen for other reasons, so
             * don't delay a lot.
             */
            _probe.leader_transfer_error();
            co_await ss::sleep_abortable(5s, _as.local());
            co_return ss::stop_iteration::no;

        } else {
            _probe.leader_transfer_succeeded();
            strategy->apply_movement(*transfer);
        }

        /*
         * if leadership moved, or it timed out we'll mute the group for a while
         * and continue to avoid any thrashing. notice that we don't check for
         * movement to the exact shard we requested. this is because we want to
         * avoid thrashing (we'll still mute the group), but also because we may
         * have simply been racing with organic leadership movement.
         */
        _muted.try_emplace(
          transfer->group, clock_type::now() + _mute_timeout());
    }

    co_return ss::stop_iteration::no;
}

absl::flat_hash_set<model::node_id> leader_balancer::muted_nodes() const {
    absl::flat_hash_set<model::node_id> nodes;
    const auto now = raft::clock_type::now();
    for (const auto& follower : _raft0->get_follower_metrics()) {
        auto last_hbeat_age = now - follower.last_heartbeat;
        if (last_hbeat_age > _node_mute_timeout()) {
            nodes.insert(follower.id);
            vlog(
              clusterlog.info,
              "Leadership rebalancer muting node {} last heartbeat {} ms",
              follower.id,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                last_hbeat_age)
                .count());
        }

        if (auto nm = _members.get_node_metadata_ref(follower.id); nm) {
            auto maintenance_state = (*nm).get().state.get_maintenance_state();

            if (maintenance_state == model::maintenance_state::active) {
                nodes.insert(follower.id);
                vlog(
                  clusterlog.info,
                  "Leadership rebalancer muting node {} in a maintenance "
                  "state.",
                  follower.id);
            }
        }
    }
    return nodes;
}

absl::flat_hash_set<raft::group_id> leader_balancer::muted_groups() const {
    absl::flat_hash_set<raft::group_id> res;
    res.reserve(_muted.size());
    for (const auto& e : _muted) {
        res.insert(e.first);
    }
    return res;
}

leader_balancer_types::group_id_to_topic_revision_t
leader_balancer::build_group_id_to_topic_rev() const {
    leader_balancer_types::group_id_to_topic_revision_t group_id_to_topic_rev;

    // for each ntp in the cluster
    for (const auto& topic : _topics.topics_map()) {
        for (const auto& partition : topic.second.get_assignments()) {
            if (partition.replicas.empty()) {
                vlog(
                  clusterlog.warn,
                  "Leadership encountered partition with no partition "
                  "assignment: {}",
                  model::ntp(topic.first.ns, topic.first.tp, partition.id));
                continue;
            }

            group_id_to_topic_rev.try_emplace(
              partition.group, topic.second.get_revision());
        }
    }

    return group_id_to_topic_rev;
}

/*
 * builds an index that maps each core in the cluster to the set of replica
 * groups such that the leader of each mapped replica group is on the given
 * core. the index is used by a balancing strategy to compute metrics and to
 * search for leader movements that improve overall balance in the cluster. the
 * index is computed from controller metadata.
 */
leader_balancer::index_type leader_balancer::build_index() {
    absl::flat_hash_set<model::broker_shard> cores;
    index_type index;

    // for each ntp in the cluster
    for (const auto& topic : _topics.topics_map()) {
        for (const auto& partition : topic.second.get_assignments()) {
            if (partition.replicas.empty()) {
                vlog(
                  clusterlog.warn,
                  "Leadership encountered partition with no partition "
                  "assignment: {}",
                  model::ntp(topic.first.ns, topic.first.tp, partition.id));
                continue;
            }

            /*
             * skip balancing for the controller partition, otherwise we might
             * just constantly move ourselves around.
             */
            if (
              topic.first.ns == model::controller_ntp.ns
              && topic.first.tp == model::controller_ntp.tp.topic) {
                continue;
            }

            /*
             * if the partition group is a part of our in flight changes
             * then assume that leadership will be transferred to the target
             * node and balance based off of that.
             */
            if (auto it = _in_flight_changes.find(partition.group);
                it != _in_flight_changes.end()) {
                const auto& assignment = it->second.value;

                std::vector<model::broker_shard> replicas = partition.replicas;
                // Swap to and from in the replicas
                if (auto r_it = std::find(
                      replicas.begin(), replicas.end(), assignment.to);
                    r_it != replicas.end()) {
                    *r_it = assignment.from;
                }

                index[assignment.to][partition.group] = std::move(replicas);
                continue;
            }

            /*
             * map the ntp to its leader's shard. first we look up the leader
             * node, then find the corresponding shard from its replica set.
             * it's possible that we don't find this information because of
             * transient states or inconsistencies from joining metadata.
             */
            std::optional<model::broker_shard> leader_core;
            auto leader_node = _leaders.get_leader(topic.first, partition.id);
            if (leader_node) {
                auto it = std::find_if(
                  partition.replicas.cbegin(),
                  partition.replicas.cend(),
                  [node = *leader_node](const auto& replica) {
                      return replica.node_id == node;
                  });

                if (it != partition.replicas.cend()) {
                    leader_core = *it;
                    _last_leader.insert_or_assign(
                      partition.group,
                      last_known_leader{
                        *leader_core, clock_type::now() + _mute_timeout()});
                } else {
                    vlog(
                      clusterlog.info,
                      "Group {} has leader node but no leader shard: {}",
                      partition.group,
                      partition.replicas);
                }
            }

            /*
             * if no leader node or core was found then we still want to
             * represent the resource in the index to avoid an artificial
             * imbalance. use the last known assignment if available, or
             * otherwise a random replica choice.
             */
            bool needs_mute = false;
            if (!leader_core) {
                if (auto it = _last_leader.find(partition.group);
                    it != _last_leader.end()) {
                    leader_core = it->second.shard;
                } else {
                    /*
                     * if there is no leader core then select a random broker
                     * shard assignment. there is no point in trying to
                     * constrain the choice if leader_node is known because
                     * otherwise it would have been found above!
                     */
                    std::vector<model::broker_shard> leader;
                    std::sample(
                      partition.replicas.cbegin(),
                      partition.replicas.cend(),
                      std::back_inserter(leader),
                      1,
                      random_generators::internal::gen);
                    // partition.replicas.empty() is checked above
                    vassert(!leader.empty(), "Failed to select replica");
                    leader_core = leader.front();
                }
                needs_mute = true;
            }

            // track superset of cores
            for (const auto& replica : partition.replicas) {
                cores.emplace(replica);
            }

            if (needs_mute) {
                auto it = _muted.find(partition.group);
                if (
                  it == _muted.end()
                  || (it->second - clock_type::now())
                       < leader_activation_delay) {
                    _muted.insert_or_assign(
                      it,
                      partition.group,
                      clock_type::now() + leader_activation_delay);
                }
            }

            index[*leader_core][partition.group] = partition.replicas;
        }
    }

    /*
     * ensure that the resulting index contains all cores by adding missing
     * cores (with empty replica sets) from the observed superset.
     *
     * the reason this is important is because if a node loses all its
     * leaderships (e.g. it is offline for some time) then all its cores
     * should still be present in the index represented as having full
     * available capacity. if no leadership is found on the core, the
     * accounting above will ignore core.
     */
    for (const auto& core : cores) {
        index.try_emplace(core);
    }

    return index;
}

ss::future<bool> leader_balancer::do_transfer(reassignment transfer) {
    vlog(
      clusterlog.debug,
      "Transferring leadership for group {} from {} to {}",
      transfer.group,
      transfer.from,
      transfer.to);

    if (transfer.from.node_id == _raft0->self().id()) {
        co_return co_await do_transfer_local(transfer);
    } else {
        co_return co_await do_transfer_remote(transfer);
    }
}

ss::future<bool>
leader_balancer::do_transfer_local(reassignment transfer) const {
    auto shard = _shard_table.local().shard_for(transfer.group);
    if (!shard) {
        vlog(
          clusterlog.info,
          "Cannot complete group {} leader transfer: shard not found",
          transfer.group);
        co_return false;
    }

    auto func = [transfer, shard = *shard](cluster::partition_manager& pm) {
        auto partition = pm.partition_for(transfer.group);
        if (!partition) {
            vlog(
              clusterlog.info,
              "Cannot complete group {} leader transfer: group instance "
              "not found on shard {}",
              transfer.group,
              shard);
            return ss::make_ready_future<bool>(false);
        }

        transfer_leadership_request req{
          .group = transfer.group,
          .target = transfer.to.node_id,
          .timeout = transfer_leadership_recovery_timeout};

        return partition->transfer_leadership(req).then(
          [group = transfer.group](std::error_code err) {
              if (err) {
                  vlog(
                    clusterlog.info,
                    "Leadership transfer of group {} failed with error: {}",
                    group,
                    err.message());
                  return ss::make_ready_future<bool>(false);
              }
              return ss::make_ready_future<bool>(true);
          });
    };
    co_return co_await _partition_manager.invoke_on(*shard, std::move(func));
}

/**
 * Deprecated: this method may be removed when we no longer require
 * compatibility with Redpanda <= 22.3
 */
ss::future<bool>
leader_balancer::do_transfer_remote_legacy(reassignment transfer) {
    raft::transfer_leadership_request req{
      .group = transfer.group,
      .target = transfer.to.node_id,
      .timeout = transfer_leadership_recovery_timeout};

    vlog(
      clusterlog.debug,
      "Leadership transfer of group {} using legacy RPC",
      transfer.group);

    auto raft_client = raft::make_rpc_client_protocol(
      _raft0->self().id(), _connections);
    auto res = co_await raft_client.transfer_leadership(
      transfer.from.node_id,
      std::move(req), // NOLINT(hicpp-move-const-arg,performance-move-const-arg)
      rpc::client_opts(leader_transfer_rpc_timeout));

    if (!res) {
        vlog(
          clusterlog.info,
          "Leadership transfer of group {} failed with error: {}",
          transfer.group,
          res.error().message());
        co_return false;
    }

    if (res.value().success) {
        co_return true;
    }

    vlog(
      clusterlog.info,
      "Leadership transfer of group {} failed with error: {}",
      transfer.group,
      raft::make_error_code(res.value().result).message());

    co_return false;
}

ss::future<bool> leader_balancer::do_transfer_remote(reassignment transfer) {
    transfer_leadership_request req{
      .group = transfer.group,
      .target = transfer.to.node_id,
      .timeout = transfer_leadership_recovery_timeout};

    auto res = co_await _connections.local()
                 .with_node_client<controller_client_protocol>(
                   _raft0->self().id(),
                   ss::this_shard_id(),
                   transfer.from.node_id,
                   leader_transfer_rpc_timeout,
                   [req](controller_client_protocol ccp) mutable {
                       return ccp.transfer_leadership(
                         std::move(req),
                         rpc::client_opts(leader_transfer_rpc_timeout));
                   });

    if (res.has_error() && res.error() == rpc::errc::method_not_found) {
        // Cluster leadership transfer unavailable: use legacy raw raft leader
        // transfer API
        co_return co_await do_transfer_remote_legacy(std::move(transfer));
    } else if (res.has_error()) {
        vlog(
          clusterlog.info,
          "Leadership transfer of group {} failed with error: {}",
          transfer.group,
          res.error().message());
        co_return false;
    } else if (res.value().data.success) {
        vlog(
          clusterlog.trace,
          "Leadership transfer of group {} succeeded",
          transfer.group);
        co_return true;
    } else {
        vlog(
          clusterlog.info,
          "Leadership transfer of group {} failed with error: {}",
          transfer.group,
          raft::make_error_code(res.value().data.result).message());
        co_return false;
    }
}

} // namespace cluster
