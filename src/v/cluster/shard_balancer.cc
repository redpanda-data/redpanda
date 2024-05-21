/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/shard_balancer.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "ssx/async_algorithm.h"

namespace cluster {

shard_balancer::shard_balancer(
  ss::sharded<shard_placement_table>& spt,
  ss::sharded<features::feature_table>& features,
  ss::sharded<topic_table>& topics,
  ss::sharded<controller_backend>& cb)
  : _shard_placement(spt.local())
  , _features(features.local())
  , _topics(topics)
  , _controller_backend(cb)
  , _self(*config::node().node_id())
  , _total_counts(ss::smp::count, 0) {
    _total_counts.at(0) += 1; // controller partition
}

ss::future<> shard_balancer::start() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    // We expect topic_table to remain unchanged throughout the method
    // invocation because it is supposed to be called after local controller
    // replay is finished but before we start getting new controller updates
    // from the leader.
    auto tt_version = _topics.local().topics_map_revision();

    // 1. collect the set of node-local ntps from topic_table

    chunked_hash_map<raft::group_id, model::ntp> local_group2ntp;
    chunked_hash_map<model::ntp, model::revision_id> local_ntp2log_revision;
    const auto& topics = _topics.local();
    ssx::async_counter counter;
    for (const auto& [ns_tp, md_item] : topics.all_topics_metadata()) {
        vassert(
          tt_version == topics.topics_map_revision(),
          "topic_table unexpectedly changed");

        co_await ssx::async_for_each_counter(
          counter,
          md_item.get_assignments().begin(),
          md_item.get_assignments().end(),
          [&](const partition_assignment& p_as) {
              vassert(
                tt_version == topics.topics_map_revision(),
                "topic_table unexpectedly changed");

              model::ntp ntp{ns_tp.ns, ns_tp.tp, p_as.id};
              auto replicas_view = topics.get_replicas_view(ntp, md_item, p_as);
              auto log_rev = log_revision_on_node(replicas_view, _self);
              if (log_rev) {
                  local_group2ntp.emplace(replicas_view.assignment.group, ntp);
                  local_ntp2log_revision.emplace(ntp, *log_rev);
              }
          });
    }

    // 2. restore shard_placement_table from the kvstore or from topic_table.

    if (_shard_placement.is_persistence_enabled()) {
        co_await _shard_placement.initialize_from_kvstore(local_group2ntp);
    } else if (_features.is_active(
                 features::feature::node_local_core_assignment)) {
        // joiner node? enable persistence without initializing
        co_await _shard_placement.enable_persistence();
    } else {
        // topic_table is still the source of truth
        co_await _shard_placement.initialize_from_topic_table(_topics, _self);

        if (_features.is_preparing(
              features::feature::node_local_core_assignment)) {
            // We may have joined or restarted while the feature is still in the
            // preparing state. Enable persistence here before we get new
            // controller updates to avoid races with activation of the feature.
            co_await _shard_placement.enable_persistence();
        }
    }

    // 3. Initialize shard partition counts and assign non-assigned local ntps.
    //
    // Note: old assignments for ntps not in local_group2ntp have already been
    // removed during shard_placement_table initialization.

    co_await ssx::async_for_each_counter(
      counter,
      local_ntp2log_revision.begin(),
      local_ntp2log_revision.end(),
      [&](const std::pair<const model::ntp&, model::revision_id> kv) {
          const auto& [ntp, log_revision] = kv;
          auto existing_target = _shard_placement.get_target(ntp);

          if (existing_target) {
              update_counts(
                ntp,
                _topic2data[model::topic_namespace_view{ntp}],
                std::nullopt,
                existing_target);
          }

          if (
            !existing_target || existing_target->log_revision != log_revision) {
              _to_assign.insert(ntp);
          }
      });
    co_await do_assign_ntps();

    vassert(
      tt_version == _topics.local().topics_map_revision(),
      "topic_table unexpectedly changed");

    // we shouldn't be receiving any controller updates at this point, so no
    // risk of missing a notification between initializing shard_placement_table
    // and subscribing.
    _topic_table_notify_handle = _topics.local().register_delta_notification(
      [this](topic_table::delta_range_t deltas_range) {
          for (const auto& delta : deltas_range) {
              // Filter out only deltas that might change the set of partition
              // replicas on this node.
              switch (delta.type) {
              case topic_table_delta_type::disabled_flag_updated:
              case topic_table_delta_type::properties_updated:
                  continue;
              default:
                  _to_assign.insert(delta.ntp);
                  _wakeup_event.set();
                  break;
              }
          }
      });

    ssx::background = assign_fiber();
}

ss::future<> shard_balancer::stop() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    _topics.local().unregister_delta_notification(_topic_table_notify_handle);
    _wakeup_event.set();
    return _gate.close();
}

ss::future<> shard_balancer::enable_persistence() {
    auto gate_holder = _gate.hold();
    if (_shard_placement.is_persistence_enabled()) {
        co_return;
    }
    vassert(
      _features.is_preparing(features::feature::node_local_core_assignment),
      "unexpected feature state");
    co_await _shard_placement.enable_persistence();
}

ss::future<> shard_balancer::assign_fiber() {
    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_holder = _gate.hold();

    while (true) {
        co_await _wakeup_event.wait(1s);
        if (_gate.is_closed()) {
            co_return;
        }

        co_await do_assign_ntps();
    }
}

using ntp2target_t
  = chunked_hash_map<model::ntp, std::optional<shard_placement_target>>;

ss::future<> shard_balancer::do_assign_ntps() {
    ntp2target_t new_targets;
    auto to_assign = std::exchange(_to_assign, {});
    co_await ssx::async_for_each(
      to_assign.begin(), to_assign.end(), [&](const model::ntp& ntp) {
          maybe_assign(ntp, new_targets);
      });

    co_await ss::max_concurrent_for_each(
      new_targets, 128, [this](const decltype(new_targets)::value_type& kv) {
          const auto& [ntp, target] = kv;
          return set_target(ntp, target)
            .handle_exception([this, &ntp](const std::exception_ptr&) {
                // Retry on the next tick.
                _to_assign.insert(ntp);
            });
      });
}

void shard_balancer::maybe_assign(
  const model::ntp& ntp, ntp2target_t& new_targets) {
    std::optional<shard_placement_target> prev_target
      = _shard_placement.get_target(ntp);

    std::optional<shard_placement_target> target;
    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (replicas_view) {
        // Has value if the partition is expected to exist on this node.
        target = placement_target_on_node(replicas_view.value(), _self);
    }

    if (prev_target == target) {
        return;
    }

    auto& topic_data = _topic2data[model::topic_namespace_view{ntp}];

    vlog(
      clusterlog.debug,
      "[{}] assigning shard {} (prev: {}, topic counts: {}, total counts: {})",
      ntp,
      target ? std::optional(target->shard) : std::nullopt,
      prev_target ? std::optional(prev_target->shard) : std::nullopt,
      topic_data.shard2count,
      _total_counts);

    update_counts(ntp, topic_data, prev_target, target);
    new_targets.emplace(ntp, target);
}

ss::future<> shard_balancer::set_target(
  const model::ntp& ntp, const std::optional<shard_placement_target>& target) {
    auto shard_callback = [this](const model::ntp& ntp) {
        _controller_backend.local().notify_reconciliation(ntp);
    };

    try {
        co_await _shard_placement.set_target(ntp, target, shard_callback);
    } catch (...) {
        auto ex = std::current_exception();
        if (!ssx::is_shutdown_exception(ex)) {
            vlog(
              clusterlog.warn,
              "[{}] exception while setting target: {}",
              ntp,
              ex);
        }

        // revert shard counts update if needed
        auto cur_target = _shard_placement.get_target(ntp);
        if (cur_target != target) {
            update_counts(
              ntp,
              _topic2data[model::topic_namespace_view{ntp}],
              target,
              cur_target);
        }

        throw;
    }
}

void shard_balancer::update_counts(
  const model::ntp& ntp,
  topic_data_t& topic_data,
  const std::optional<shard_placement_target>& prev,
  const std::optional<shard_placement_target>& next) {
    if (prev) {
        topic_data.shard2count.at(prev->shard) -= 1;
        topic_data.total_count -= 1;
        // TODO: check negative values
        _total_counts.at(prev->shard) -= 1;
    }

    if (next) {
        topic_data.shard2count.at(next->shard) += 1;
        topic_data.total_count += 1;
        _total_counts.at(next->shard) += 1;
    }

    if (topic_data.total_count == 0) {
        _topic2data.erase(model::topic_namespace_view{ntp});
    }
}

} // namespace cluster
