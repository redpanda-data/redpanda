/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/scheduling/leader_balancer.h"

#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/leader_balancer_greedy.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>

namespace cluster {

leader_balancer::leader_balancer(
  topic_table& topics,
  partition_leaders_table& leaders,
  raft::consensus_client_protocol client,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<raft::group_manager>& group_manager,
  ss::sharded<ss::abort_source>& as,
  std::chrono::milliseconds idle_timeout,
  std::chrono::milliseconds mute_timeout,
  std::chrono::milliseconds node_mute_timeout,
  consensus_ptr raft0)
  : _idle_timeout(idle_timeout)
  , _mute_timeout(mute_timeout)
  , _node_mute_timeout(node_mute_timeout)
  , _topics(topics)
  , _leaders(leaders)
  , _client(std::move(client))
  , _shard_table(shard_table)
  , _partition_manager(partition_manager)
  , _group_manager(group_manager)
  , _as(as)
  , _raft0(std::move(raft0))
  , _timer([this] { trigger_balance(); }) {
    if (!config::shard_local_cfg().disable_metrics()) {
        _probe.setup_metrics();
    }
}

ss::future<> leader_balancer::start() {
    /*
     * register for raft0 leadership change notifications. shutdown the balancer
     * when we lose leadership, and start it when we gain leadership.
     */
    _leader_notify_handle
      = _group_manager.local().register_leadership_notification(
        [this](
          raft::group_id group, model::term_id, std::optional<model::node_id>) {
            if (group != _raft0->group()) {
                return;
            }
            if (_raft0->is_leader()) {
                vlog(
                  clusterlog.info,
                  "Leader balancer: controller leadership detected. Starting "
                  "rebalancer in {} seconds",
                  std::chrono::duration_cast<std::chrono::seconds>(
                    leader_activation_delay)
                    .count());
                _timer.cancel();
                _timer.arm(leader_activation_delay);
            } else {
                vlog(
                  clusterlog.info,
                  "Leader balancer: controller leadership lost");
                _need_controller_refresh = true;
                _timer.cancel();
                _timer.arm(_idle_timeout);
            }
        });

    /*
     * register_leadership_notification above may run callbacks synchronously
     * during registration, so make sure the timer is unarmed before arming.
     */
    if (!_timer.armed()) {
        _timer.arm(_idle_timeout);
    }

    co_return;
}

ss::future<> leader_balancer::stop() {
    _group_manager.local().unregister_leadership_notification(
      _leader_notify_handle);
    _timer.cancel();
    return _gate.close();
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
        _timer.arm(_idle_timeout);
    }

    (void)ss::try_with_gate(_gate, [this] {
        return ss::repeat([this] {
                   if (_as.local().abort_requested()) {
                       return ss::make_ready_future<ss::stop_iteration>(
                         ss::stop_iteration::yes);
                   }
                   return balance();
               })
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([this](const std::exception& e) {
              vlog(
                clusterlog.info,
                "Leadership rebalance experienced an unhandled error: {}. "
                "Retrying in {} seconds",
                e,
                std::chrono::duration_cast<std::chrono::seconds>(_idle_timeout)
                  .count());
              _timer.cancel();
              _timer.arm(_idle_timeout);
          });
    }).handle_exception_type([](const ss::gate_closed_exception&) {});
}

ss::future<ss::stop_iteration> leader_balancer::balance() {
    /*
     * GC the muted and last leader indices
     */
    absl::erase_if(
      _muted, [now = clock_type::now()](auto g) { return now >= g.second; });

    absl::erase_if(_last_leader, [now = clock_type::now()](auto g) {
        return now >= g.second.expires;
    });

    if (!_raft0->is_leader()) {
        vlog(clusterlog.debug, "Leadership balancer tick: not leader");
        if (!_timer.armed()) {
            _timer.arm(_idle_timeout);
        }
        _need_controller_refresh = true;
        co_return ss::stop_iteration::yes;
    } else if (_raft0->config().brokers().size() == 0) {
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

    if (_as.local().abort_requested()) {
        co_return ss::stop_iteration::yes;
    }

    /*
     * For simplicity the current implementation rebuilds the index on each
     * rebalancing tick. Testing shows that this takes up to a couple
     * hundred microseconds for up to 1000s of raft groups. This can be
     * optimized later by attempting to minimize the number of rebuilds
     * (e.g. on average little should change between ticks) and bounding the
     * search for leader moves.
     */
    greedy_balanced_shards strategy(build_index(), muted_nodes());

    if (clusterlog.is_enabled(ss::log_level::trace)) {
        auto cores = strategy.stats();
        for (const auto& core : cores) {
            vlog(
              clusterlog.trace,
              "Leadership balancing stats: core {} leaders {}",
              core.shard,
              core.leaders);
        }
    }

    auto error = strategy.error();
    auto transfer = strategy.find_movement(muted_groups());
    if (!transfer) {
        vlog(
          clusterlog.info,
          "No leadership balance improvements found with total delta {}, "
          "number of muted groups {}",
          error,
          _muted.size());
        if (!_timer.armed()) {
            _timer.arm(_idle_timeout);
        }
        _probe.leader_transfer_no_improvement();
        co_return ss::stop_iteration::yes;
    }

    auto success = co_await do_transfer(*transfer);
    if (!success) {
        vlog(
          clusterlog.info,
          "Error transferring leadership group {} from {} to {}",
          transfer->group,
          transfer->from,
          transfer->to);
        /*
         * a common scenario is that a node loses all its leadership (e.g.
         * restarts) and then it is recognized as having lots of extra capacity
         * (which it does). but the balancer doesn't consider node health when
         * making decisions. so when we fail transfer we inject a short delay
         * to avoid spinning on sending transfer requests to a failed node. of
         * course failure can happen for other reasons, so don't delay a lot.
         */
        _probe.leader_transfer_error();
        co_await ss::sleep_abortable(5s, _as.local());
        _muted.try_emplace(transfer->group, clock_type::now() + _mute_timeout);
        co_return ss::stop_iteration::no;
    }

    if (_as.local().abort_requested()) {
        co_return ss::stop_iteration::yes;
    }

    /*
     * reverse the mapping from raft::group_id to model::ntp so that we can
     * look up its leadership information in the metadata table. we should
     * fix this with better indexing / interfaces.
     *
     *   See: https://github.com/vectorizedio/redpanda/issues/2032
     */
    std::optional<model::ntp> ntp;
    for (const auto& topic : _topics.topics_map()) {
        if (!topic.second.is_topic_replicable()) {
            continue;
        }
        for (const auto& partition :
             topic.second.get_configuration().assignments) {
            if (partition.group == transfer->group) {
                ntp = model::ntp(topic.first.ns, topic.first.tp, partition.id);
                break;
            }
        }
    }

    /*
     * if the group is now not found in the topics table it is probably
     * something benign like we were racing with topic deletion.
     */
    if (!ntp) {
        vlog(
          clusterlog.info,
          "Leadership balancer muting group {} not found in topics table",
          transfer->group);
        co_await ss::sleep_abortable(5s, _as.local());
        _muted.try_emplace(transfer->group, clock_type::now() + _mute_timeout);
        co_return ss::stop_iteration::no;
    }

    /*
     * leadership reported success. wait for the event to be propogated to the
     * cluster and reflected in the local metadata.
     */
    int retries = 6;
    auto delay = 300ms; // w/ doubling, about 20 second
    std::optional<model::broker_shard> leader_shard;
    while (retries--) {
        co_await ss::sleep_abortable(delay, _as.local());
        leader_shard = find_leader_shard(*ntp);

        // no leader shard (possibly intermediate state) or no movement yet
        if (!leader_shard || *leader_shard == transfer->from) {
            delay *= 2;
            continue;
        }

        break;
    }

    if (leader_shard && *leader_shard != transfer->from) {
        _probe.leader_transfer_succeeded();
        vlog(
          clusterlog.info,
          "Leadership for group {} moved from {} to {} (target {}). Delta {}",
          transfer->group,
          transfer->from,
          *leader_shard,
          transfer->to,
          error);
    } else {
        _probe.leader_transfer_timeout();
        vlog(
          clusterlog.info,
          "Leadership movement for group {} from {} to {} timed out "
          "(leader_shard {})",
          transfer->group,
          transfer->from,
          transfer->to,
          leader_shard);
    }

    /*
     * if leadership moved, or it timed out we'll mute the group for a while and
     * continue to avoid any thrashing. notice that we don't check for movement
     * to the exact shard we requested. this is because we want to avoid
     * thrashing (we'll still mute the group), but also because we may have
     * simply been racing with organic leadership movement.
     */
    _muted.try_emplace(transfer->group, clock_type::now() + _mute_timeout);
    co_return ss::stop_iteration::no;
}

absl::flat_hash_set<model::node_id> leader_balancer::muted_nodes() const {
    absl::flat_hash_set<model::node_id> nodes;
    const auto now = raft::clock_type::now();
    for (const auto& follower : _raft0->get_follower_metrics()) {
        auto last_hbeat_age = now - follower.last_heartbeat;
        if (last_hbeat_age > _node_mute_timeout) {
            nodes.insert(follower.id);
            vlog(
              clusterlog.info,
              "Leadership rebalancer muting node {} last heartbeat {} ms",
              follower.id,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                last_hbeat_age)
                .count());
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

std::optional<model::broker_shard>
leader_balancer::find_leader_shard(const model::ntp& ntp) {
    /*
     * look up the broker_shard for ntp's leader. we simply seem to lack
     * interfaces for making this query concise, but it is rarely needed.
     */
    auto leader_node = _leaders.get_leader(ntp);
    if (!leader_node) {
        return std::nullopt;
    }

    auto config_it = _topics.topics_map().find(
      model::topic_namespace_view(ntp));
    if (config_it == _topics.topics_map().end()) {
        return std::nullopt;
    }

    // If the inital query was for a non_replicable ntp, the get_leader() call
    // would have returned its source topic
    for (const auto& partition :
         config_it->second.get_configuration().assignments) {
        if (partition.id != ntp.tp.partition) {
            continue;
        }

        /*
         * scan through the replicas until we find the leader node and then
         * pluck out the shard from that assignment.
         */
        for (const auto& replica : partition.replicas) {
            if (replica.node_id == *leader_node) {
                return model::broker_shard{replica.node_id, replica.shard};
            }
        }
    }

    return std::nullopt;
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
        if (!topic.second.is_topic_replicable()) {
            continue;
        }
        for (const auto& partition :
             topic.second.get_configuration().assignments) {
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
                        *leader_core, clock_type::now() + _mute_timeout});
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
    if (!_shard_table.local().contains(transfer.group)) {
        vlog(
          clusterlog.info,
          "Cannot complete group {} leader transfer: shard not found",
          transfer.group);
        co_return false;
    }
    auto shard = _shard_table.local().shard_for(transfer.group);
    auto func = [transfer, shard](cluster::partition_manager& pm) {
        auto consensus = pm.consensus_for(transfer.group);
        if (!consensus) {
            vlog(
              clusterlog.info,
              "Cannot complete group {} leader transfer: group instance "
              "not found on shard {}",
              transfer.group,
              shard);
            return ss::make_ready_future<bool>(false);
        }
        return consensus->do_transfer_leadership(transfer.to.node_id)
          .then([group = transfer.group](std::error_code err) {
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
    co_return co_await _partition_manager.invoke_on(shard, std::move(func));
}

ss::future<bool> leader_balancer::do_transfer_remote(reassignment transfer) {
    raft::transfer_leadership_request req{
      .group = transfer.group, .target = transfer.to.node_id};

    auto res = co_await _client.transfer_leadership(
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
      raft::make_error_code(res.value().result));

    co_return false;
}

} // namespace cluster
