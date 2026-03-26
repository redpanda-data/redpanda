// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/shard_balancer.h"
#include "cluster/shard_table.h"
#include "cluster/topic_rules.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/leaders_preference.h"
#include "data_migration_types.h"
#include "features/enterprise_feature_messages.h"
#include "features/feature_table.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "topic_configuration.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <set>
#include <system_error>

namespace {

std::vector<std::string_view>
get_enterprise_features(const cluster::topic_configuration& cfg) {
    std::vector<std::string_view> features;
    static const auto si_disabled = model::shadow_indexing_mode::disabled;
    if (config::shard_local_cfg().cloud_storage_enabled.is_restricted()) {
        if (
          (cfg.properties.shadow_indexing.value_or(si_disabled) != si_disabled)
          || (cfg.properties.storage_mode == model::redpanda_storage_mode::tiered)) {
            features.emplace_back("tiered storage");
        }
        if (cfg.is_recovery_enabled()) {
            features.emplace_back("topic recovery");
        }
        if (cfg.is_read_replica()) {
            features.emplace_back("remote read replicas");
        }
    }
    if (config::shard_local_cfg().enable_schema_id_validation.is_restricted()) {
        if (cfg.is_schema_id_validation_enabled()) {
            features.emplace_back("schema ID validation");
        }
    }
    if (const auto& leaders_pref = cfg.properties.leaders_preference;
        leaders_pref.has_value()
        && config::shard_local_cfg()
             .default_leaders_preference.check_restricted(
               leaders_pref.value())) {
        features.emplace_back("leadership pinning");
    }
    if (config::shard_local_cfg().iceberg_enabled.is_restricted()) {
        if (cfg.properties.iceberg_mode != model::iceberg_mode::disabled) {
            features.emplace_back("iceberg");
        }
    }
    if (config::shard_local_cfg().cloud_topics_enabled.is_restricted()) {
        if (
          cfg.properties.storage_mode == model::redpanda_storage_mode::cloud) {
            features.emplace_back("cloud topics");
        }
    }
    return features;
}

cluster::allocation_request make_allocation_request(
  model::ntp ntp,
  cluster::replication_factor tp_replication_factor,
  const std::vector<model::node_id>& new_replicas) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    cluster::allocation_request req(nt);
    req.partitions.reserve(1);
    cluster::allocation_constraints constraints;
    constraints.add(cluster::on_nodes(new_replicas));
    req.partitions.emplace_back(
      ntp.tp.partition, tp_replication_factor, std::move(constraints));
    return req;
}

} // namespace

namespace cluster {

errc map_errc(std::error_code ec);

namespace {
topic_result make_error_result(
  const model::topic_namespace& tp_ns,
  std::error_code ec,
  std::optional<ss::sstring> msg = std::nullopt) {
    errc error = ec.category() == cluster::error_category()
                   ? errc(ec.value())
                   : errc::topic_operation_error;
    if (msg.has_value()) {
        return {tp_ns, error, std::move(msg).value()};
    }
    return topic_result{tp_ns, error};
}
} // namespace

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  reconfiguration_policy policy,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(tout);
    if (!result) {
        co_return result.error();
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (_topics.local().is_update_in_progress(ntp)) {
        co_return errc::update_in_progress;
    }
    const auto fast_reconfiguration_active = _features.local().is_active(
      features::feature::fast_partition_reconfiguration);

    // fallback to old move command
    if (!fast_reconfiguration_active) {
        if (policy != reconfiguration_policy::full_local_retention) {
            vlog(
              clusterlog.warn,
              "Trying to move partition {} to {} with reconfiguration policy "
              "of {} but fast partition movement feature is not yet active",
              ntp,
              new_replica_set,
              policy);
        }
        move_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        co_return co_await replicate_and_wait(
          _stm, _as, std::move(cmd), tout, term);
    }
    update_partition_replicas_cmd cmd(
      0, // unused
      update_partition_replicas_cmd_data{
        .ntp = std::move(ntp),
        .replicas = std::move(new_replica_set),
        .policy = policy});

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), tout, term);
}

ss::future<std::error_code> topics_frontend::force_update_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(tout);
    if (!result) {
        co_return result.error();
    }
    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    force_partition_reconfiguration_cmd cmd{
      std::move(ntp),
      force_partition_reconfiguration_cmd_data{std::move(new_replica_set)}};

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), tout, term);
}

ss::future<result<chunked_vector<ntp_with_majority_loss>>>
topics_frontend::partitions_with_lost_majority(
  std::vector<model::node_id> dead_nodes) {
    try {
        chunked_vector<ntp_with_majority_loss> result;
        const auto& topics = _topics.local();
        for (auto it = topics.topics_iterator_begin();
             it != topics.topics_iterator_end();
             ++it) {
            const auto& tn = it->first;
            const auto& assignments = (it->second).get_assignments();
            const auto topic_revision = it->second.get_revision();
            for (const auto& [_, assignment] : assignments) {
                const auto& current = assignment.replicas;
                auto remaining = subtract_replica_sets_by_node_id(
                  current, dead_nodes);
                auto lost_majority = remaining.size()
                                     < (current.size() / 2) + 1;
                if (!lost_majority) {
                    continue;
                }
                model::ntp ntp(tn.ns, tn.tp, assignment.id);
                result.emplace_back(
                  std::move(ntp),
                  topic_revision,
                  assignment.replicas,
                  dead_nodes);
                co_await ss::coroutine::maybe_yield();
                it.check();
            }
        }
        auto validation_err
          = _topics.local().validate_force_reconfigurable_partitions(result);
        if (validation_err) {
            co_return errc::concurrent_modification_error;
        }
        co_return result;
    } catch (const concurrent_modification_error& e) {
        // state changed while generating the plan, force caller to retry;
        vlog(
          clusterlog.info,
          "Topic table state changed when generating force move plan: {}",
          e.what());
    }
    co_return errc::concurrent_modification_error;
}

ss::future<std::error_code>
topics_frontend::force_recover_partitions_from_nodes(
  std::vector<model::node_id> nodes,
  chunked_vector<ntp_with_majority_loss>
    user_approved_force_recovery_partitions,
  model::timeout_clock::time_point timeout) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }
    // check if the state of partitions to recover tallies with their
    // current state.
    const auto& topics = _topics.local();
    auto reject = false;
    for (const auto& entry : user_approved_force_recovery_partitions) {
        // check if there is an in progress movemement, reject if so.
        // this is a conservative check and can be relaxed.
        auto current_assignment = topics.get_partition_assignment(entry.ntp);
        auto assignment_match = current_assignment
                                && are_replica_sets_equal(
                                  current_assignment->replicas,
                                  entry.assignment);
        if (!assignment_match) {
            vlog(
              clusterlog.info,
              "rejecting force recovery of partitions from brokers {}, ntp: "
              "{}, expected replica set: {}, current "
              "assignment: {}, the state may have changed since the original "
              "request was made, try again.",
              nodes,
              entry.ntp,
              entry.assignment,
              current_assignment);
            reject = true;
        }
    }
    if (reject) {
        co_return errc::invalid_request;
    }
    auto validation_err
      = _topics.local().validate_force_reconfigurable_partitions(
        user_approved_force_recovery_partitions);
    if (validation_err) {
        co_return validation_err;
    }
    bulk_force_reconfiguration_cmd_data data;
    data.from_nodes = std::move(nodes);
    data.user_approved_force_recovery_partitions = std::move(
      user_approved_force_recovery_partitions);
    co_return co_await replicate_and_wait(
      _stm, _as, bulk_force_reconfiguration_cmd{0, std::move(data)}, timeout);
}

ss::future<std::error_code> topics_frontend::cancel_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }
    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }

    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::no));

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), timeout, term);
}

ss::future<std::error_code> topics_frontend::abort_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }
    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::yes));

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), timeout, term);
}

ss::future<std::vector<topic_result>> topics_frontend::create_partitions(
  std::vector<create_partitions_configuration> partitions,
  model::timeout_clock::time_point timeout) {
    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        std::vector<topic_result> results;
        results.reserve(partitions.size());
        std::transform(
          partitions.begin(),
          partitions.end(),
          std::back_inserter(results),
          [err = r.error()](const create_partitions_configuration& cfg) {
              return make_error_result(cfg.tp_ns, err);
          });
        co_return results;
    }

    auto result = co_await ssx::parallel_transform(
      partitions.begin(),
      partitions.end(),
      [this, timeout](create_partitions_configuration cfg) {
          return do_create_partition(std::move(cfg), timeout);
      });

    co_return result;
}

ss::future<std::error_code> topics_frontend::set_topic_partitions_disabled(
  model::topic_namespace_view ns_tp,
  std::optional<model::partition_id> p_id,
  bool disabled,
  model::timeout_clock::time_point timeout) {
    if (!_features.local().is_active(features::feature::disabling_partitions)) {
        co_return errc::feature_disabled;
    }

    if (!topic_rules::can_be_disabled(ns_tp)) {
        co_return errc::invalid_partition_operation;
    }

    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        co_return r.error();
    }

    // pre-replicate checks

    if (p_id) {
        if (!_topics.local().contains(ns_tp, *p_id)) {
            co_return errc::partition_not_exists;
        }
        if (_topics.local().is_disabled(ns_tp, *p_id) == disabled) {
            // no-op
            co_return errc::success;
        }
    } else {
        if (!_topics.local().contains(ns_tp)) {
            co_return errc::topic_not_exists;
        }
        if (disabled && _topics.local().is_fully_disabled(ns_tp)) {
            // no-op
            co_return errc::success;
        }
        if (!disabled && _topics.local().is_fully_enabled(ns_tp)) {
            // no-op
            co_return errc::success;
        }
    }

    // replicate the command

    set_topic_partitions_disabled_cmd cmd(
      0, // unused
      set_topic_partitions_disabled_cmd_data{
        .ns_tp = model::topic_namespace{ns_tp},
        .partition_id = p_id,
        .disabled = disabled,
      });

    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

ss::future<bool>
topics_frontend::validate_shard(model::node_id node, uint32_t shard) const {
    return _allocator.invoke_on(
      partition_allocator::shard, [node, shard](partition_allocator& al) {
          return al.state().validate_shard(node, shard);
      });
}

ss::future<topic_result> topics_frontend::do_create_partition(
  create_partitions_configuration p_cfg,
  model::timeout_clock::time_point timeout) {
    auto tp_cfg = _topics.local().get_topic_cfg(p_cfg.tp_ns);
    auto replication_factor = _topics.local().get_topic_replication_factor(
      p_cfg.tp_ns);
    if (!tp_cfg || !replication_factor) {
        co_return make_error_result(p_cfg.tp_ns, errc::topic_not_exists);
    }
    auto state = _migrated_resources.get_topic_state(p_cfg.tp_ns);
    if (state != data_migrations::migrated_resource_state::non_restricted) {
        vlog(
          clusterlog.warn,
          "can not create {} topic partitions as the topic is being migrated",
          p_cfg.tp_ns);

        co_return topic_result{
          std::move(p_cfg.tp_ns), errc::resource_is_being_migrated};
    }

    // we only support increasing number of partitions
    if (p_cfg.new_total_partition_count <= tp_cfg->partition_count) {
        co_return make_error_result(
          p_cfg.tp_ns, errc::topic_invalid_partitions_decreased);
    }
    if (_topics.local().is_fully_disabled(p_cfg.tp_ns)) {
        co_return make_error_result(p_cfg.tp_ns, errc::topic_disabled);
    }

    if (_features.local().should_sanction() && is_user_topic(tp_cfg->tp_ns)) {
        if (auto f = get_enterprise_features(*tp_cfg); !f.empty()) {
            auto msg = features::enterprise_error_message::create_partition(f);
            vlog(clusterlog.warn, "{}", msg);
            co_return make_error_result(
              p_cfg.tp_ns, errc::topic_invalid_config, std::move(msg));
        }
    }

    std::optional<node2count_t> existing_replica_counts;
    if (_partition_autobalancing_topic_aware()) {
        auto md_ref = _topics.local().get_topic_metadata_ref(p_cfg.tp_ns);
        if (!md_ref) {
            co_return make_error_result(p_cfg.tp_ns, errc::topic_not_exists);
        }

        node2count_t node2count;
        for (const auto& [_, p_as] : md_ref->get().get_assignments()) {
            for (const auto& r : p_as.replicas) {
                node2count[r.node_id] += 1;
            }
        }
        existing_replica_counts = std::move(node2count);
    }

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [p_cfg,
       current = tp_cfg->partition_count,
       existing_rc = std::move(existing_replica_counts),
       rf = replication_factor.value()](partition_allocator& al) mutable {
          const auto new_partitions_cnt = p_cfg.new_total_partition_count
                                          - current;
          const auto replication_factor = static_cast<int16_t>(rf);
          return al.allocate(
            simple_allocation_request{
              p_cfg.tp_ns,
              new_partitions_cnt,
              replication_factor,
              std::move(existing_rc)});
      });

    // no assignments, error
    if (!units) {
        co_return make_error_result(p_cfg.tp_ns, units.error());
    }

    auto tp_ns = p_cfg.tp_ns;
    create_partitions_configuration_assignment payload(
      std::move(p_cfg), units.value()->copy_assignments());
    create_partition_cmd cmd = create_partition_cmd(tp_ns, std::move(payload));

    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), timeout);
        co_return topic_result(tp_ns, map_errc(ec));
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Unable to create topic {} partitions - {}",
          tp_ns,
          std::current_exception());
        co_return topic_result(std::move(tp_ns), errc::replication_error);
    }
}

ss::future<std::error_code> topics_frontend::finish_moving_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout,
  dispatch_to_leader dispatch) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }

    // current node is a leader, just replicate
    if (leader == _self) {
        // optimization: if update is not in progress return early
        if (!_topics.local().is_update_in_progress(ntp)) {
            return ss::make_ready_future<std::error_code>(
              errc::no_update_in_progress);
        }

        finish_moving_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        return replicate_and_wait(_stm, _as, std::move(cmd), tout);
    }

    if (!dispatch) {
        return ss::make_ready_future<std::error_code>(
          errc::not_leader_controller);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp), replicas = std::move(new_replica_set), tout](
          controller_client_protocol client) mutable {
            return client
              .finish_partition_update(
                finish_partition_update_request{
                  .ntp = std::move(ntp),
                  .new_replica_set = std::move(replicas)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<finish_partition_update_reply>);
        })
      .then([](result<finish_partition_update_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}
ss::future<std::error_code> topics_frontend::revert_cancel_partition_move(
  model::ntp ntp, model::timeout_clock::time_point tout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }
    // optimization: if update is not in progress return early
    if (!_topics.local().is_update_in_progress(ntp)) {
        return ss::make_ready_future<std::error_code>(
          errc::no_update_in_progress);
    }
    // current node is a leader, just replicate
    if (leader == _self) {
        revert_cancel_partition_move_cmd cmd(
          0, revert_cancel_partition_move_cmd_data{.ntp = std::move(ntp)});

        return replicate_and_wait(_stm, _as, std::move(cmd), tout);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp),
         tout](controller_client_protocol client) mutable {
            return client
              .revert_cancel_partition_move(
                revert_cancel_partition_move_request{.ntp = std::move(ntp)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<revert_cancel_partition_move_reply>);
        })
      .then([](result<revert_cancel_partition_move_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_partition_replicas_node(
  model::node_id node_id,
  partition_move_direction dir,
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        switch (dir) {
        case partition_move_direction::from_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_from_node(node_id), timeout);
        case partition_move_direction::to_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_to_node(node_id), timeout);
        case partition_move_direction::all:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().all_ntps_moving_per_node(node_id), timeout);
        }
        __builtin_unreachable();
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout, node_id, dir](controller_client_protocol client) mutable {
            return client
              .cancel_node_partition_movements(
                cancel_node_partition_movements_request{
                  .node_id = node_id, .direction = dir},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_all_partition_replicas(
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_cancel_moving_partition_replicas(
          _topics.local().all_updates_in_progress(), timeout);
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout](controller_client_protocol client) mutable {
            return client
              .cancel_all_partition_movements(
                cancel_all_partition_movements_request{},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<std::vector<move_cancellation_result>>
topics_frontend::do_cancel_moving_partition_replicas(
  chunked_vector<model::ntp> ntps, model::timeout_clock::time_point timeout) {
    std::vector<move_cancellation_result> results;
    results.reserve(ntps.size());
    co_await ss::max_concurrent_for_each(
      ntps, 32, [this, &results, timeout](model::ntp& ntp) {
          auto f = cancel_moving_partition_replicas(ntp, timeout);
          return f.then(
            [ntp = std::move(ntp), &results](std::error_code ec) mutable {
                results.emplace_back(
                  std::move(ntp), map_update_interruption_error_code(ec));
            });
      });

    co_return results;
}

ss::future<std::error_code> topics_frontend::change_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    auto current_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    if (current_replication_factor == new_replication_factor) {
        co_return errc::success;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    if (new_replication_factor < current_replication_factor) {
        co_return co_await decrease_replication_factor(
          topic, new_replication_factor, timeout);
    }
    co_return co_await increase_replication_factor(
      topic, new_replication_factor, timeout);
}

ss::future<topics_frontend::capacity_info> topics_frontend::get_health_info(
  model::topic_namespace topic, int32_t partition_count) const {
    capacity_info info;

    partitions_filter::partitions_set_t parititon_set;
    for (auto i = 0; i < partition_count; ++i) {
        parititon_set.emplace(i);
    }

    partitions_filter::topic_map_t topic_map;
    topic_map.emplace(topic.tp, std::move(parititon_set));

    partitions_filter partitions_for_report;
    partitions_for_report.namespaces.emplace(topic.ns, std::move(topic_map));

    node_report_filter filter;
    filter.ntp_filters = std::move(partitions_for_report);

    auto health_report = co_await _hm_frontend.local().get_cluster_health(
      cluster_report_filter{.node_report_filter = std::move(filter)},
      force_refresh::no,
      model::timeout_clock::now() + _get_health_report_timeout);

    if (!health_report) {
        vlog(
          clusterlog.info,
          "unable to get health report - {}",
          health_report.error().message());
        co_return info;
    }

    for (const auto& node_report : health_report.value().node_reports) {
        uint64_t total = 0;
        uint64_t free = 0;

        // This health report is just on the data disk.  If the cache has
        // a separate disk, it is not reflected in the node health.
        total += node_report->local_state.data_disk.total;
        free += node_report->local_state.data_disk.free;

        info.node_disk_reports.emplace(
          node_report->id,
          node_disk_space(node_report->id, total, total - free));
    }

    for (auto& node_report : health_report.value().node_reports) {
        co_await ss::max_concurrent_for_each(
          node_report->topics,
          32,
          [&info](const node_health_report::topics_t::value_type& status) {
              for (const auto& partition : status.second) {
                  info.ntp_sizes[partition.first] = partition.second.size_bytes;
              }
              return ss::now();
          });
    }

    co_return info;
}
namespace {

// Executed on the partition_allocator shard
ss::future<result<allocation_units::pointer>> do_increase_replication_factor(
  const model::topic_namespace& ns_tp,
  const assignments_set& assignments,
  partition_allocator& al,
  replication_factor new_rf,
  double max_disk_usage_ratio,
  const topics_frontend::capacity_info& capacity_info,
  std::optional<node2count_t> existing_replica_counts) {
    allocation_request req(ns_tp);
    req.partitions.reserve(assignments.size());
    co_await ssx::async_for_each(
      assignments.begin(),
      assignments.end(),
      [&](const assignments_set::value_type& assignment) {
          allocation_constraints allocation_constraints;

          auto partition_size_it = capacity_info.ntp_sizes.find(
            assignment.second.id);
          if (partition_size_it != capacity_info.ntp_sizes.end()) {
              // Add constraint on partition max_disk_usage_ratio overfill
              allocation_constraints.add(disk_not_overflowed_by_partition(
                max_disk_usage_ratio,
                partition_size_it->second,
                capacity_info.node_disk_reports));

              // Add constraint on least disk usage
              allocation_constraints.add(least_disk_filled(
                max_disk_usage_ratio,
                partition_size_it->second,
                capacity_info.node_disk_reports));
          }

          req.partitions.emplace_back(
            assignment.second, new_rf, std::move(allocation_constraints));
      });
    req.existing_replica_counts = existing_replica_counts;

    auto units = co_await al.allocate(std::move(req));
    if (units.has_error()) {
        vlog(
          clusterlog.warn,
          "attempt to replication factor for topic {} to {} failed, error: {}",
          ns_tp,
          new_rf,
          units.error().message());
    }
    co_return units;
}

} // namespace

ss::future<std::error_code> topics_frontend::increase_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    if (
      static_cast<size_t>(new_replication_factor)
      > _members_table.local().node_count()) {
        vlog(
          clusterlog.warn,
          "New replication factor({}) is greater than number of brokers({})",
          new_replication_factor,
          _members_table.local().node_count());
        co_return errc::topic_invalid_replication_factor;
    }

    auto tp_metadata = _topics.local().get_topic_metadata(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    auto partition_count = tp_metadata->get_configuration().partition_count;

    auto health_report = co_await get_health_info(topic, partition_count);

    auto hard_max_disk_usage_ratio = (100 - _hard_max_disk_usage_ratio())
                                     / 100.0;

    std::optional<node2count_t> existing_replica_counts;
    if (_partition_autobalancing_topic_aware()) {
        node2count_t node2count;
        for (const auto& [_, p_as] : tp_metadata->get_assignments()) {
            for (const auto& bs : p_as.replicas) {
                node2count[bs.node_id] += 1;
            }
        }
        existing_replica_counts = std::move(node2count);
    }

    // units shold exist during replicate_and_wait call
    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [&topic,
       &tp_metadata,
       new_replication_factor,
       hard_max_disk_usage_ratio,
       &health_report,
       existing_replica_counts = std::move(existing_replica_counts)](
        partition_allocator& al) mutable {
          return do_increase_replication_factor(
            topic,
            tp_metadata->get_assignments(),
            al,
            new_replication_factor,
            hard_max_disk_usage_ratio,
            health_report,
            std::move(existing_replica_counts));
      });
    if (units.has_error()) {
        co_return units.error();
    }

    std::vector<move_topic_replicas_data> new_assignments;
    new_assignments.reserve(units.value()->get_assignments().size());
    for (const auto& assignment : units.value()->get_assignments()) {
        new_assignments.emplace_back(assignment.id, assignment.replicas);
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

ss::future<std::error_code> topics_frontend::decrease_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    std::vector<move_topic_replicas_data> new_assignments;

    auto tp_metadata = _topics.local().get_topic_metadata(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    std::optional<std::error_code> error;

    co_await ss::max_concurrent_for_each(
      tp_metadata->get_assignments(),
      32,
      [&new_assignments, &error, topic, new_replication_factor](
        assignments_set::value_type& assignment) {
          if (error) {
              return ss::now();
          }
          if (assignment.second.replicas.size() < new_replication_factor()) {
              error = errc::topic_invalid_replication_factor;
              return ss::now();
          }

          new_assignments.emplace_back(move_topic_replicas_data());
          new_assignments.back().partition = assignment.second.id;
          new_assignments.back().replicas.resize(new_replication_factor);
          std::copy_n(
            assignment.second.replicas.begin(),
            new_replication_factor,
            new_assignments.back().replicas.begin());
          return ss::now();
      });

    if (error) {
        co_return error.value();
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));

    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

ss::future<result<ss::chunked_fifo<partition_assignment>>>
topics_frontend::generate_reassignments(
  model::ntp ntp, std::vector<model::node_id> new_replicas) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(
      model::topic_namespace{ntp.ns, ntp.tp.topic});
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }

    auto tp_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [ntp, tp_replication_factor, new_replicas{std::move(new_replicas)}](
        partition_allocator& al) {
          return al.allocate(
            make_allocation_request(ntp, tp_replication_factor, new_replicas));
      });

    if (!units) {
        co_return units.error();
    }

    auto assignments = units.value()->copy_assignments();
    if (assignments.empty()) {
        co_return errc::no_partition_assignments;
    }

    co_return assignments;
}

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::node_id> new_replica_set,
  reconfiguration_policy policy,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto assignments = co_await generate_reassignments(
      ntp, std::move(new_replica_set));

    // The return type from generate_reassignments is a
    // result<vector<partition_assignment>>. So we need to make sure
    // the result: 1) has a value and 2) the vector should only have one replica
    // set, so check that the front partition id matches the request.

    if (!assignments) {
        co_return assignments.error();
    }

    if (assignments.value().front().id != ntp.tp.partition) {
        co_return errc::allocation_error;
    }

    co_return co_await move_partition_replicas(
      ntp, std::move(assignments.value().front().replicas), policy, tout, term);
}

ss::future<result<partition_state_reply>>
topics_frontend::do_get_partition_state(model::node_id node, model::ntp ntp) {
    if (node == _self) {
        partition_state_reply reply{};
        auto shard = _shard_table.local().shard_for(ntp);
        if (!shard) {
            reply.error_code = errc::partition_not_exists;
            return ss::make_ready_future<result<partition_state_reply>>(reply);
        }
        return _pm.invoke_on(
          *shard,
          [ntp = std::move(ntp),
           reply = std::move(reply)](partition_manager& pm) mutable {
              auto partition = pm.get(ntp);
              if (!partition) {
                  reply.error_code = errc::partition_not_exists;
                  return ss::make_ready_future<result<partition_state_reply>>(
                    reply);
              }
              reply.state = ::cluster::get_partition_state(partition);
              reply.error_code = errc::success;
              return ss::make_ready_future<result<partition_state_reply>>(
                reply);
          });
    }
    auto timeout = model::timeout_clock::now() + 5s;
    return _connections.local().with_node_client<controller_client_protocol>(
      _self,
      ss::this_shard_id(),
      node,
      timeout,
      [ntp = std::move(ntp),
       timeout](controller_client_protocol client) mutable {
          return client
            .get_partition_state(
              partition_state_request{.ntp = ntp}, rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<partition_state_reply>);
      });
}

ss::future<result<std::vector<partition_state>>>
topics_frontend::get_partition_state(model::ntp ntp) {
    const auto& topics = _topics.local();
    if (!topics.contains(model::topic_namespace_view(ntp))) {
        co_return errc::partition_not_exists;
    }
    std::set<model::node_id> nodes_to_query;
    const auto& current = topics.get_partition_assignment(ntp);
    if (current) {
        const auto& bss = current->replicas;
        std::for_each(
          bss.begin(),
          bss.end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }
    const auto& prev = topics.get_previous_replica_set(ntp);
    if (prev) {
        std::for_each(
          prev.value().begin(),
          prev.value().end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }

    if (nodes_to_query.empty()) {
        co_return errc::no_partition_assignments;
    }

    std::vector<ss::future<result<partition_state_reply>>> futures;
    futures.reserve(nodes_to_query.size());
    for (const auto& node : nodes_to_query) {
        futures.push_back(do_get_partition_state(node, ntp));
    }

    auto finished_futures = co_await ss::when_all(
      futures.begin(), futures.end());
    std::vector<partition_state> results;
    results.reserve(finished_futures.size());
    for (auto& fut : finished_futures) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, failure: {}",
              ntp,
              ex);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, result: {}",
              ntp,
              result.error());
            continue;
        }
        auto res = std::move(result.value());
        if (res.error_code != errc::success || !res.state) {
            vlog(
              clusterlog.debug,
              "Error during partition state fetch for ntp: {}, error: "
              "{}",
              ntp,
              res.error_code);
            continue;
        }
        results.push_back(std::move(*res.state));
    }
    co_return results;
}

void topics_frontend::print_rf_warning_message() {
    const auto min_rf = _minimum_topic_replication();
    const auto& topics = _topics.local().topics_map();
    for (const auto& t : topics) {
        if (!model::is_user_topic(t.first)) {
            continue;
        }
        auto rf = t.second.get_replication_factor();
        if (rf() >= min_rf) {
            continue;
        }
        vlog(
          clusterlog.warn,
          "Topic {} has a replication factor less than specified "
          "minimum: {} < {}",
          t.first,
          rf,
          min_rf);
    }
}

bool topics_frontend::node_local_core_assignment_enabled() const {
    return _features.local().is_active(
      features::feature::node_local_core_assignment);
}

ss::future<std::error_code> topics_frontend::set_partition_replica_shard(
  model::ntp ntp,
  model::node_id replica,
  ss::shard_id shard,
  model::timeout_clock::time_point deadline) {
    if (!node_local_core_assignment_enabled()) {
        co_return errc::feature_disabled;
    }

    if (replica == _self) {
        co_return co_await set_local_partition_shard(ntp, shard);
    }

    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (!replicas_view) {
        co_return errc::partition_not_exists;
    }
    if (!log_revision_on_node(replicas_view.value(), replica)) {
        co_return errc::replica_does_not_exist;
    }

    auto reply = co_await _connections.local()
                   .with_node_client<cluster::controller_client_protocol>(
                     _self,
                     ss::this_shard_id(),
                     replica,
                     deadline,
                     [ntp, shard, deadline](
                       controller_client_protocol cp) mutable {
                         return cp
                           .set_partition_shard(
                             set_partition_shard_request{
                               .ntp = std::move(ntp), .shard = shard},
                             rpc::client_opts(deadline))
                           .then(&rpc::get_ctx_data<set_partition_shard_reply>);
                     });

    if (reply.has_error()) {
        co_return reply.error();
    }
    co_return reply.value().ec;
}

ss::future<errc>
topics_frontend::set_local_partition_shard(model::ntp ntp, ss::shard_id shard) {
    return _shard_balancer.invoke_on(
      shard_balancer::shard_id,
      [ntp = std::move(ntp), shard](shard_balancer& sb) {
          return sb.reassign_shard(ntp, shard);
      });
}

ss::future<errc> topics_frontend::trigger_local_partition_shard_rebalance() {
    return _shard_balancer.invoke_on(
      shard_balancer::shard_id,
      [](shard_balancer& sb) { return sb.trigger_rebalance(); });
}

} // namespace cluster
