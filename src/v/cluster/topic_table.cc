// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_table.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "storage/ntp_config.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <optional>
#include <span>
#include <utility>

namespace cluster {

template<typename Func>
std::vector<std::invoke_result_t<Func, const topic_table::topic_metadata_item&>>
topic_table::transform_topics(Func&& f) const {
    std::vector<std::invoke_result_t<Func, const topic_metadata_item&>> ret;
    ret.reserve(_topics.size());
    std::transform(
      std::cbegin(_topics),
      std::cend(_topics),
      std::back_inserter(ret),
      [f = std::forward<Func>(f)](
        const std::pair<model::topic_namespace, topic_metadata_item>& p) {
          return f(p.second);
      });
    return ret;
}

ss::future<std::error_code>
topic_table::apply(create_topic_cmd cmd, model::offset offset) {
    _last_applied_revision_id = model::revision_id(offset);
    if (_topics.contains(cmd.key)) {
        // topic already exists
        return ss::make_ready_future<std::error_code>(
          errc::topic_already_exists);
    }

    std::optional<model::initial_revision_id> remote_revision
      = cmd.value.cfg.properties.remote_topic_properties ? std::make_optional(
          cmd.value.cfg.properties.remote_topic_properties->remote_revision)
                                                         : std::nullopt;
    auto md = topic_metadata_item{
      .metadata = topic_metadata(
        std::move(cmd.value), model::revision_id(offset()), remote_revision)};
    // calculate delta
    md.partitions.reserve(cmd.value.assignments.size());
    auto rev_id = model::revision_id{offset};
    for (auto& pas : md.get_assignments()) {
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, pas.id);
        replicas_revision_map replica_revisions;
        _partition_count++;
        for (auto& r : pas.replicas) {
            replica_revisions[r.node_id] = rev_id;
        }
        md.partitions.emplace(
          pas.id,
          partition_meta{
            .replicas_revisions = replica_revisions,
            .last_update_finished_revision = rev_id});
        _pending_deltas.emplace_back(
          std::move(ntp),
          pas,
          offset,
          delta::op_type::add,
          std::nullopt,
          std::move(replica_revisions));
    }

    _topics.insert({
      cmd.key,
      std::move(md),
    });
    notify_waiters();

    _probe.handle_topic_creation(std::move(cmd.key));
    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<> topic_table::stop() {
    for (auto& w : _waiters) {
        w->promise.set_exception(ss::abort_requested_exception());
    }
    return ss::now();
}

ss::future<std::error_code>
topic_table::apply(delete_topic_cmd cmd, model::offset offset) {
    _last_applied_revision_id = model::revision_id(offset);
    auto delete_type = delta::op_type::del;
    if (auto tp = _topics.find(cmd.value); tp != _topics.end()) {
        if (!tp->second.is_topic_replicable()) {
            delete_type = delta::op_type::del_non_replicable;
            model::topic_namespace_view tp_nsv{
              cmd.key.ns, tp->second.get_source_topic()};
            auto found = _topics_hierarchy.find(tp_nsv);
            vassert(
              found != _topics_hierarchy.end(),
              "Missing source for non_replicable topic: {}",
              tp_nsv);
            vassert(
              found->second.erase(cmd.value) > 0,
              "non_replicable_topic should exist in hierarchy: {}",
              tp_nsv);
        } else {
            /// Prevent deletion of source topics that have non_replicable
            /// topics. To delete this topic all of its non_replicable descedent
            /// topics must first be deleted
            auto found = _topics_hierarchy.find(cmd.value);
            if (found != _topics_hierarchy.end() && !found->second.empty()) {
                return ss::make_ready_future<std::error_code>(
                  errc::source_topic_still_in_use);
            }
        }

        for (auto& p_as : tp->second.get_assignments()) {
            _partition_count--;
            auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p_as.id);
            _updates_in_progress.erase(ntp);
            _pending_deltas.emplace_back(
              std::move(ntp), p_as, offset, delete_type);
        }

        _topics.erase(tp);
        notify_waiters();
        _probe.handle_topic_deletion(cmd.key);

        return ss::make_ready_future<std::error_code>(errc::success);
    }

    return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
}
ss::future<std::error_code>
topic_table::apply(create_partition_cmd cmd, model::offset offset) {
    _last_applied_revision_id = model::revision_id(offset);
    auto tp = _topics.find(cmd.key);
    if (tp == _topics.end() || !tp->second.is_topic_replicable()) {
        co_return errc::topic_not_exists;
    }

    // add partitions
    auto prev_partition_count = tp->second.get_configuration().partition_count;
    // update partitions count
    tp->second.get_configuration().partition_count
      = cmd.value.cfg.new_total_partition_count;
    // add assignments of newly created partitions
    auto rev_id = model::revision_id{offset};
    for (auto& p_as : cmd.value.assignments) {
        _partition_count++;
        p_as.id += model::partition_id(prev_partition_count);
        tp->second.get_assignments().emplace(p_as);
        // propagate deltas
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p_as.id);
        replicas_revision_map replicas_revisions;
        for (auto& bs : p_as.replicas) {
            replicas_revisions[bs.node_id] = rev_id;
        }
        tp->second.partitions[p_as.id] = partition_meta{
          .replicas_revisions = replicas_revisions,
          .last_update_finished_revision = rev_id};
        _pending_deltas.emplace_back(
          std::move(ntp),
          std::move(p_as),
          offset,
          delta::op_type::add,
          std::nullopt,
          std::move(replicas_revisions));
    }
    notify_waiters();
    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(move_partition_replicas_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }
    if (!tp->second.is_topic_replicable()) {
        return ss::make_ready_future<std::error_code>(
          errc::topic_operation_error);
    }

    auto current_assignment_it = tp->second.get_assignments().find(
      cmd.key.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (_updates_in_progress.contains(cmd.key)) {
        return ss::make_ready_future<std::error_code>(errc::update_in_progress);
    }

    change_partition_replicas(
      cmd.key, cmd.value, tp->second, *current_assignment_it, o);
    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

static replicas_revision_map update_replicas_revisions(
  replicas_revision_map revisions,
  const std::vector<model::broker_shard>& new_assignment,
  model::revision_id update_revision) {
    // remove replicas not present in the new assignment
    for (auto it = revisions.begin(); it != revisions.end();) {
        bool is_removed = std::none_of(
          new_assignment.begin(),
          new_assignment.end(),
          [node = it->first](const auto& bs) { return bs.node_id == node; });
        auto it_copy = it++;
        if (is_removed) {
            revisions.erase(it_copy);
        }
    }

    // insert newly appearing replicas with update_revision
    for (const auto& bs : new_assignment) {
        revisions.emplace(bs.node_id, update_revision);
    }

    return revisions;
}

ss::future<std::error_code>
topic_table::apply(finish_moving_partition_replicas_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }
    if (!tp->second.is_topic_replicable()) {
        return ss::make_ready_future<std::error_code>(
          errc::topic_operation_error);
    }

    // calculate deleta for backend
    auto current_assignment_it = tp->second.get_assignments().find(
      cmd.key.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (current_assignment_it->replicas != cmd.value) {
        return ss::make_ready_future<std::error_code>(
          errc::invalid_node_operation);
    }
    auto it = _updates_in_progress.find(cmd.key);
    if (it == _updates_in_progress.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::no_update_in_progress);
    }

    auto p_meta_it = tp->second.partitions.find(cmd.key.tp.partition);
    if (p_meta_it == tp->second.partitions.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }
    if (it->second.get_state() == reconfiguration_state::in_progress) {
        // update went through and the cancellation didn't happen, we must
        // update replicas_revisions.
        p_meta_it->second.replicas_revisions = update_replicas_revisions(
          std::move(p_meta_it->second.replicas_revisions),
          cmd.value,
          it->second.get_update_revision());
    }
    p_meta_it->second.last_update_finished_revision = model::revision_id{o};

    _updates_in_progress.erase(it);

    partition_assignment delta_assignment{
      current_assignment_it->group,
      current_assignment_it->id,
      std::move(cmd.value),
    };

    /// Remove child non_replicable topics out of the 'updates_in_progress' set
    auto found = _topics_hierarchy.find(model::topic_namespace_view(cmd.key));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            bool erased = _updates_in_progress.erase(
                            model::ntp(cs.ns, cs.tp, cmd.key.tp.partition))
                          > 0;
            if (!erased) {
                vlog(
                  clusterlog.error,
                  "non_replicable_topic expected to exist in "
                  "updates_in_progress set");
            }
        }
    }

    // notify backend about finished update
    _pending_deltas.emplace_back(
      std::move(cmd.key),
      std::move(delta_assignment),
      o,
      delta::op_type::update_finished);

    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<std::error_code>
topic_table::apply(cancel_moving_partition_replicas_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    vlog(
      clusterlog.trace,
      "applying cancel moving partition replicas command ntp: {}, "
      "force_cancel: {}",
      cmd.key,
      cmd.value.force);

    /**
     * Validate partition exists
     */
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        co_return errc::topic_not_exists;
    }
    if (!tp->second.is_topic_replicable()) {
        co_return errc::topic_operation_error;
    }

    auto current_assignment_it = tp->second.get_assignments().find(
      cmd.key.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        co_return errc::partition_not_exists;
    }

    // update must be in progress to be able to cancel it
    auto in_progress_it = _updates_in_progress.find(cmd.key);
    if (in_progress_it == _updates_in_progress.end()) {
        co_return errc::no_update_in_progress;
    }
    switch (in_progress_it->second.get_state()) {
    case reconfiguration_state::in_progress:
        break;
    case reconfiguration_state::cancelled:
        // partition reconfiguration already cancelled, only allow force
        // cancelling it
        if (cmd.value.force == force_abort_update::no) {
            co_return errc::no_update_in_progress;
        }
        break;
    case reconfiguration_state::force_cancelled:
        // partition reconfiguration already cancelled forcibly
        co_return errc::no_update_in_progress;
    }

    in_progress_it->second.set_state(
      cmd.value.force ? reconfiguration_state::force_cancelled
                      : reconfiguration_state::cancelled,
      model::revision_id{o});

    auto replicas = current_assignment_it->replicas;
    // replace replica set with set from in progress operation
    current_assignment_it->replicas
      = in_progress_it->second.get_previous_replicas();

    /// Update all non_replicable topics to have the same 'in-progress' state
    auto found = _topics_hierarchy.find(model::topic_namespace_view(cmd.key));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            auto child_it = _updates_in_progress.find(
              model::ntp(cs.ns, cs.tp, current_assignment_it->id));

            vassert(
              child_it != _updates_in_progress.end(),
              "non_replicable topic {} in progress state inconsistent with its "
              "parent {}",
              cs,
              cmd.key);

            child_it->second.set_state(
              cmd.value.force ? reconfiguration_state::force_cancelled
                              : reconfiguration_state::cancelled,
              model::revision_id{o});

            auto sfound = _topics.find(cs);
            vassert(
              sfound != _topics.end(),
              "Non replicable topic must exist: {} as it was found in "
              "hierarchy",
              cs);
            auto assignment_it = sfound->second.get_assignments().find(
              current_assignment_it->id);
            vassert(
              assignment_it != sfound->second.get_assignments().end(),
              "Non replicable partition {}/{} assignment must exists",
              cs,
              current_assignment_it->id);
            /// The new assignments of the non_replicable topic/partition must
            /// match the source topic
            assignment_it->replicas
              = in_progress_it->second.get_previous_replicas();
        }
    }
    /**
     * Cancel/force abort delta contains two assignments new_assignment is set
     * to the one the partition is currently being moved from. Previous
     * assignment is set to an assignment which is target assignment from
     * current move.
     */

    auto partition_it = tp->second.partitions.find(cmd.key.tp.partition);
    vassert(
      partition_it != tp->second.partitions.end(),
      "partition {} must exist in the partitions map",
      cmd.key);

    _pending_deltas.emplace_back(
      std::move(cmd.key),
      partition_assignment{
        current_assignment_it->group,
        current_assignment_it->id,
        in_progress_it->second.get_previous_replicas()},
      o,
      cmd.value.force ? delta::op_type::force_abort_update
                      : delta::op_type::cancel_update,
      std::move(replicas),
      // this replica revisions map reflects the state right before the update
      // (i.e. the state we are trying to return to by cancelling)
      partition_it->second.replicas_revisions);

    notify_waiters();

    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(revert_cancel_partition_move_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);

    const auto ntp = std::move(cmd.value.ntp);
    vlog(
      clusterlog.trace,
      "applying revert cancel move partition replicas command ntp: {}",
      ntp);
    /**
     * Validate partition exists
     */
    auto tp = _topics.find(model::topic_namespace_view(ntp));
    if (tp == _topics.end()) {
        co_return errc::topic_not_exists;
    }
    if (!tp->second.is_topic_replicable()) {
        co_return errc::topic_operation_error;
    }

    auto current_assignment_it = tp->second.get_assignments().find(
      ntp.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        co_return errc::partition_not_exists;
    }

    // update must be cancelled to be able to revert cancellation
    auto in_progress_it = _updates_in_progress.find(ntp);
    if (in_progress_it == _updates_in_progress.end()) {
        co_return errc::no_update_in_progress;
    }
    if (
      in_progress_it->second.get_state()
      == reconfiguration_state::in_progress) {
        co_return errc::no_update_in_progress;
    }

    // revert replica set update
    current_assignment_it->replicas
      = in_progress_it->second.get_target_replicas();

    partition_assignment delta_assignment{
      current_assignment_it->group,
      current_assignment_it->id,
      current_assignment_it->replicas,
    };

    // update partition_meta object
    auto p_meta_it = tp->second.partitions.find(ntp.tp.partition);
    if (p_meta_it == tp->second.partitions.end()) {
        co_return errc::partition_not_exists;
    }
    // the cancellation was reverted and update went through, we must
    // update replicas_revisions.
    p_meta_it->second.replicas_revisions = update_replicas_revisions(
      std::move(p_meta_it->second.replicas_revisions),
      current_assignment_it->replicas,
      in_progress_it->second.get_update_revision());
    p_meta_it->second.last_update_finished_revision = model::revision_id{o};

    /// Since the update is already finished we drop in_progress state
    _updates_in_progress.erase(in_progress_it);

    /// Remove child non_replicable topics out of the 'updates_in_progress' set
    auto found = _topics_hierarchy.find(model::topic_namespace_view(ntp));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            bool erased = _updates_in_progress.erase(
                            model::ntp(cs.ns, cs.tp, ntp.tp.partition))
                          > 0;
            if (!erased) {
                vlog(
                  clusterlog.error,
                  "non_replicable_topic expected to exist in "
                  "updates_in_progress set");
            }
        }
    }

    // notify backend about finished update
    _pending_deltas.emplace_back(
      ntp, std::move(delta_assignment), o, delta::op_type::update_finished);

    notify_waiters();

    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(move_topic_replicas_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        co_return errc::topic_not_exists;
    }
    if (!tp->second.is_topic_replicable()) {
        co_return errc::topic_operation_error;
    }

    // We should check partition before create updates

    if (std::any_of(
          cmd.value.begin(),
          cmd.value.end(),
          [&tp](const auto& partition_and_replicas) {
              return !tp->second.get_assignments().contains(
                partition_and_replicas.partition);
          })) {
        vlog(
          clusterlog.warn,
          "topic: {}: Can not move replicas, becasue can not find "
          "partitions",
          cmd.key);
        co_return errc::partition_not_exists;
    }

    if (std::any_of(
          cmd.value.begin(),
          cmd.value.end(),
          [this, key = cmd.key](const auto& partition_and_replicas) {
              return _updates_in_progress.contains(
                model::ntp(key.ns, key.tp, partition_and_replicas.partition));
          })) {
        vlog(
          clusterlog.warn,
          "topic: {}: Can not move replicas for topic, some updates in "
          "progress",
          cmd.key);
        co_return errc::update_in_progress;
    }

    for (const auto& [partition_id, new_replicas] : cmd.value) {
        auto assignment = tp->second.get_assignments().find(partition_id);
        change_partition_replicas(
          model::ntp(cmd.key.ns, cmd.key.tp, partition_id),
          new_replicas,
          tp->second,
          *assignment,
          o);
    }

    notify_waiters();

    co_return errc::success;
}

template<typename T>
void incremental_update(
  std::optional<T>& property, property_update<std::optional<T>> override) {
    switch (override.op) {
    case incremental_update_operation::remove:
        // remove override, fallback to default
        property = std::nullopt;
        return;
    case incremental_update_operation::set:
        // set new value
        property = override.value;
        return;
    case incremental_update_operation::none:
        // do nothing
        return;
    }
}

template<>
void incremental_update(
  std::optional<model::shadow_indexing_mode>& property,
  property_update<std::optional<model::shadow_indexing_mode>> override) {
    switch (override.op) {
    case incremental_update_operation::remove:
        if (!override.value || !property) {
            break;
        }
        // It's guaranteed that the remove operation will only be
        // used with one of the 'drop_' flags.
        property = model::add_shadow_indexing_flag(*property, *override.value);
        if (*property == model::shadow_indexing_mode::disabled) {
            property = std::nullopt;
        }
        return;
    case incremental_update_operation::set:
        // set new value
        if (!override.value) {
            break;
        }
        property = model::add_shadow_indexing_flag(
          property ? *property : model::shadow_indexing_mode::disabled,
          *override.value);
        return;
    case incremental_update_operation::none:
        // do nothing
        return;
    }
}

template<typename T>
void incremental_update(
  tristate<T>& property, property_update<tristate<T>> override) {
    switch (override.op) {
    case incremental_update_operation::remove:
        // remove override, fallback to default
        property = tristate<T>(std::nullopt);
        return;
    case incremental_update_operation::set:
        // set new value
        property = override.value;
        return;
    case incremental_update_operation::none:
        // do nothing
        return;
    }
}

template<typename T>
void incremental_update(
  T& property, property_update<T> override, const T& default_value) {
    switch (override.op) {
    case incremental_update_operation::remove:
        // remove override, fallback to default
        property = default_value;
        return;
    case incremental_update_operation::set:
        // set new value
        property = override.value;
        return;
    case incremental_update_operation::none:
        // do nothing
        return;
    }
}

ss::future<std::error_code>
topic_table::apply(update_topic_properties_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    auto tp = _topics.find(cmd.key);
    if (tp == _topics.end() || !tp->second.is_topic_replicable()) {
        co_return make_error_code(errc::topic_not_exists);
    }
    auto& properties = tp->second.get_configuration().properties;
    auto properties_snapshot = properties;
    auto& overrides = cmd.value;
    /**
     * Update topic properties
     */
    incremental_update(
      properties.cleanup_policy_bitflags, overrides.cleanup_policy_bitflags);
    incremental_update(
      properties.compaction_strategy, overrides.compaction_strategy);
    incremental_update(properties.compression, overrides.compression);
    incremental_update(properties.retention_bytes, overrides.retention_bytes);
    incremental_update(
      properties.retention_duration, overrides.retention_duration);
    incremental_update(properties.segment_size, overrides.segment_size);
    incremental_update(properties.timestamp_type, overrides.timestamp_type);

    incremental_update(properties.shadow_indexing, overrides.shadow_indexing);
    incremental_update(properties.batch_max_bytes, overrides.batch_max_bytes);

    incremental_update(
      properties.retention_local_target_bytes,
      overrides.retention_local_target_bytes);
    incremental_update(
      properties.retention_local_target_ms,
      overrides.retention_local_target_ms);
    incremental_update(
      properties.remote_delete,
      overrides.remote_delete,
      storage::ntp_config::default_remote_delete);
    incremental_update(properties.segment_ms, overrides.segment_ms);
    // no configuration change, no need to generate delta
    if (properties == properties_snapshot) {
        co_return errc::success;
    }
    // generate deltas for controller backend
    const auto& assignments = tp->second.get_assignments();
    for (auto& p_as : assignments) {
        _pending_deltas.emplace_back(
          model::ntp(cmd.key.ns, cmd.key.tp, p_as.id),
          p_as,
          o,
          delta::op_type::update_properties);
    }

    notify_waiters();

    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
topic_table::apply(create_non_replicable_topic_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    const model::topic_namespace& source = cmd.key.source;
    const model::topic_namespace& new_non_rep_topic = cmd.key.name;
    if (_topics.contains(new_non_rep_topic)) {
        co_return make_error_code(errc::topic_already_exists);
    }
    auto tp = _topics.find(source);
    if (tp == _topics.end()) {
        co_return make_error_code(errc::source_topic_not_exists);
    }
    vassert(
      tp->second.is_topic_replicable(), "Source topic must be replicable");

    for (const auto& pas : tp->second.get_assignments()) {
        _pending_deltas.emplace_back(
          model::ntp(new_non_rep_topic.ns, new_non_rep_topic.tp, pas.id),
          pas,
          o,
          delta::op_type::add_non_replicable);
    }

    auto cfg = tp->second.get_configuration();
    auto p_as = tp->second.get_assignments();
    cfg.tp_ns = new_non_rep_topic;

    auto [itr, success] = _topics_hierarchy.try_emplace(
      source,
      (std::initializer_list<model::topic_namespace>){new_non_rep_topic});
    if (!success) {
        auto [_, success] = itr->second.emplace(new_non_rep_topic);
        /// Assert because if item already exists, the contains check at the
        /// topic of the method should have passed
        vassert(
          success,
          "Duplicate non_replicable_topic detected when it shouldn't exist");
    }
    auto md = topic_metadata(
      std::move(cfg), std::move(p_as), model::revision_id(o()), source.tp);

    _topics.insert(
      {new_non_rep_topic,
       topic_metadata_item{
         .metadata = std::move(md),
       }});
    notify_waiters();
    co_return make_error_code(errc::success);
}

ss::future<>
topic_table::fill_snapshot(controller_snapshot& controller_snap) const {
    auto& snap = controller_snap.topics;
    for (const auto& [ns_tp, md_item] : _topics) {
        absl::node_hash_map<
          model::partition_id,
          controller_snapshot_parts::topics_t::partition_t>
          partitions;
        absl::node_hash_map<
          model::partition_id,
          controller_snapshot_parts::topics_t::update_t>
          updates;

        for (const auto& p_as : md_item.get_assignments()) {
            std::vector<model::broker_shard> replicas;
            model::ntp ntp(ns_tp.ns, ns_tp.tp, p_as.id);
            if (auto upd_it = _updates_in_progress.find(ntp);
                upd_it != _updates_in_progress.end()) {
                const auto& upd = upd_it->second;
                updates.emplace(
                  p_as.id,
                  controller_snapshot_parts::topics_t::update_t{
                    .target_assignment = upd.get_target_replicas(),
                    .state = upd.get_state(),
                    .revision = upd.get_update_revision(),
                    .last_cmd_revision = upd.get_last_cmd_revision()});

                replicas = upd.get_previous_replicas();
            } else {
                replicas = p_as.replicas;
            }

            auto p_it = md_item.partitions.find(p_as.id);
            vassert(
              p_it != md_item.partitions.end(),
              "ntp {} must be present in the partition map",
              ntp);

            partitions.emplace(
              p_as.id,
              controller_snapshot_parts::topics_t::partition_t{
                .group = p_as.group,
                .replicas = std::move(replicas),
                .replicas_revisions = p_it->second.replicas_revisions,
                .last_update_finished_revision
                = p_it->second.last_update_finished_revision});

            co_await ss::coroutine::maybe_yield();
        }

        snap.topics.emplace(
          ns_tp,
          controller_snapshot_parts::topics_t::topic_t{
            .metadata = md_item.metadata.get_fields(),
            .partitions = std::move(partitions),
            .updates = std::move(updates),
          });
    }
}

// helper class to hold context needed for adding/deleting ntps when applying a
// controller snapshot
class topic_table::snapshot_applier {
    updates_t& _updates_in_progress;
    fragmented_vector<delta>& _pending_deltas;
    topic_table_probe& _probe;

public:
    explicit snapshot_applier(topic_table& parent)
      : _updates_in_progress(parent._updates_in_progress)
      , _pending_deltas(parent._pending_deltas)
      , _probe(parent._probe) {}

    void delete_ntp(
      const model::topic_namespace& ns_tp,
      bool is_replicable,
      const partition_assignment& p_as,
      model::revision_id cmd_rev) {
        auto ntp = model::ntp(ns_tp.ns, ns_tp.tp, p_as.id);
        vlog(
          clusterlog.trace, "deleting ntp {} not in controller snapshot", ntp);
        _updates_in_progress.erase(ntp);
        auto op = is_replicable ? delta::op_type::del
                                : delta::op_type::del_non_replicable;
        _pending_deltas.emplace_back(
          std::move(ntp), std::move(p_as), model::offset{cmd_rev}, op);
        // partition_assignment object is supposed to be removed from the
        // assignments set by the caller
    }

    ss::future<> delete_topic(
      const model::topic_namespace& ns_tp,
      const topic_metadata_item& old_md_item,
      model::revision_id cmd_rev) {
        vlog(
          clusterlog.trace,
          "deleting topic {} not in controller snapshot",
          ns_tp);
        for (const auto& p_as : old_md_item.get_assignments()) {
            delete_ntp(ns_tp, old_md_item.is_topic_replicable(), p_as, cmd_rev);
            co_await ss::coroutine::maybe_yield();
        }
        _probe.handle_topic_deletion(ns_tp);
        // topic_metadata_item object is supposed to be removed from _topics by
        // the caller
    }

    void add_ntp(
      const model::ntp& ntp,
      const controller_snapshot_parts::topics_t::topic_t& topic,
      const controller_snapshot_parts::topics_t::partition_t& partition,
      topic_metadata_item& md_item,
      bool must_update_properties) {
        vlog(clusterlog.trace, "adding ntp {} from controller snapshot", ntp);
        size_t pending_deltas_start_idx = _pending_deltas.size();

        const model::partition_id p_id = ntp.tp.partition;

        // For a non-replicable topic we (in accordance to the apply() logic):
        // * don't generate deltas except add_non_replicable
        // * don't add the corresponding partition_meta map entries
        // * do add corresponding in_progress_update map entries
        const bool prev_is_replicable = md_item.is_topic_replicable();
        const bool is_replicable = !topic.metadata.source_topic.has_value();

        // 1. reconcile the _topics state (the md_item object) and generate
        // related deltas

        std::optional<partition_assignment> prev_assignment;
        model::revision_id prev_update_finished_revision;
        if (auto as_it = md_item.get_assignments().find(p_id);
            as_it != md_item.get_assignments().end()) {
            prev_assignment = std::move(*as_it);
            md_item.get_assignments().erase(as_it);

            if (prev_is_replicable) {
                auto p_it = md_item.partitions.find(p_id);
                vassert(
                  p_it != md_item.partitions.end(),
                  "ntp {} must be present in the partition map",
                  ntp);
                prev_update_finished_revision
                  = p_it->second.last_update_finished_revision;
            }
        }

        vlog(
          clusterlog.trace,
          "{}: prev_update_finished_revision: {}, new: "
          "last_update_finished_revision: {}",
          ntp,
          prev_update_finished_revision,
          partition.last_update_finished_revision);

        // TODO: assert that group and possibly replicas match with
        // prev_assignment

        // NOTE: Replicas in the snapshot don't take effects of in-progress
        // update into account, but replicas in _topics do. So if there is
        // info in the snapshot about an in-progress update, we'll have to
        // update replicas later in this function.
        partition_assignment& cur_assignment
          = *md_item.get_assignments()
               .emplace(partition.group, p_id, partition.replicas)
               .first;
        if (is_replicable) {
            md_item.partitions[p_id] = partition_meta{
              .replicas_revisions = partition.replicas_revisions,
              .last_update_finished_revision
              = partition.last_update_finished_revision,
            };
        } else {
            md_item.partitions.clear();
        }

        // Determine if the previous state was in the same "update epoch" as
        // the state in the snapshot. If not, we have to generate a "reset
        // delta" that will cause controller_backend to perform needed
        // partition add/delete operations to bring the replica set in sync
        // with the snapshot.
        if (!prev_assignment) {
            // new partition
            auto op = is_replicable ? delta::op_type::add
                                    : delta::op_type::add_non_replicable;
            _pending_deltas.emplace_back(
              ntp,
              cur_assignment,
              model::offset{partition.last_update_finished_revision},
              op,
              std::nullopt,
              partition.replicas_revisions);
        } else if (is_replicable) {
            if (
              prev_update_finished_revision
              != partition.last_update_finished_revision) {
                // the partition in the snapshot is the same as we have, but
                // after some updates were initiated and finished.
                _pending_deltas.emplace_back(
                  ntp,
                  cur_assignment,
                  model::offset{partition.last_update_finished_revision},
                  delta::op_type::reset,
                  prev_assignment->replicas,
                  partition.replicas_revisions);
            }

            if (must_update_properties) {
                _pending_deltas.emplace_back(
                  ntp,
                  cur_assignment,
                  model::offset{partition.last_update_finished_revision},
                  delta::op_type::update_properties);
            }
        }

        // 2. reconcile the _updates_in_progress state and generate
        // deltas related to the update

        // If we are in the same "update epoch", use
        // prev_update_last_cmd_revision to determine if we need to add
        // additional controller deltas to finish the previous update. If we
        // are not in the same epoch, don't bother finishing the previous
        // update, it will be interrupted by the reset delta.
        model::revision_id prev_update_last_cmd_revision;
        if (auto prev_update_it = _updates_in_progress.find(ntp);
            prev_update_it != _updates_in_progress.end()) {
            prev_update_last_cmd_revision
              = prev_update_it->second.get_last_cmd_revision();
            _updates_in_progress.erase(prev_update_it);
        }

        if (auto update_it = topic.updates.find(p_id);
            update_it != topic.updates.end()) {
            const auto& update = update_it->second;

            if (update.state == reconfiguration_state::in_progress) {
                cur_assignment.replicas = update_it->second.target_assignment;
            }

            in_progress_update inp_update{
              partition.replicas,
              update.target_assignment,
              update.state,
              update.revision,
              _probe,
            };
            inp_update.set_state(update.state, update.last_cmd_revision);
            vlog(
              clusterlog.trace,
              "{}: prev_update_last_cmd_revision: {}, new: state: {} "
              "update_rev: {} last_cmd_rev: {}",
              ntp,
              prev_update_last_cmd_revision,
              inp_update.get_state(),
              inp_update.get_update_revision(),
              inp_update.get_last_cmd_revision());
            _updates_in_progress.emplace(ntp, std::move(inp_update));

            if (
              is_replicable
              && prev_update_last_cmd_revision != update.last_cmd_revision) {
                if (
                  prev_update_finished_revision
                    != partition.last_update_finished_revision
                  || prev_update_last_cmd_revision == model::revision_id{}) {
                    // This is the new update for us, add the initial update
                    // delta. Do it even if the last op in the snapshot is
                    // cancellation because if later the "cancel revert" event
                    // happens, controller_backend has to execute the update
                    // delta to make progress.
                    _pending_deltas.emplace_back(
                      ntp,
                      partition_assignment(
                        partition.group,
                        p_id,
                        update_it->second.target_assignment),
                      model::offset{update.revision},
                      delta::op_type::update,
                      partition.replicas,
                      update_replicas_revisions(
                        partition.replicas_revisions,
                        update.target_assignment,
                        update.revision));
                }

                auto add_cancel_delta = [&](topic_table_delta::op_type op) {
                    _pending_deltas.emplace_back(
                      ntp,
                      cur_assignment,
                      model::offset{update.last_cmd_revision},
                      op,
                      update.target_assignment,
                      partition.replicas_revisions);
                };

                switch (update.state) {
                case reconfiguration_state::in_progress:
                    break;
                case reconfiguration_state::cancelled:
                    add_cancel_delta(topic_table_delta::op_type::cancel_update);
                    break;
                case reconfiguration_state::force_cancelled:
                    add_cancel_delta(
                      topic_table_delta::op_type::force_abort_update);
                    break;
                }
            }
        }

        for (size_t i = pending_deltas_start_idx; i < _pending_deltas.size();
             ++i) {
            vlog(
              clusterlog.trace,
              "applying controller snapshot: produced delta {}",
              _pending_deltas[i]);
        }
    }

    ss::future<topic_metadata_item> create_topic(
      const model::topic_namespace& ns_tp,
      const controller_snapshot_parts::topics_t::topic_t& topic) {
        topic_metadata_item ret{topic_metadata{topic.metadata, {}}};
        for (const auto& [p_id, partition] : topic.partitions) {
            auto ntp = model::ntp(ns_tp.ns, ns_tp.tp, p_id);
            add_ntp(ntp, topic, partition, ret, false);
            co_await ss::coroutine::maybe_yield();
        }
        _probe.handle_topic_creation(ns_tp);
        co_return ret;
    };
};

ss::future<> topic_table::apply_snapshot(
  model::offset snap_offset, const controller_snapshot& controller_snap) {
    model::revision_id snap_revision{snap_offset};
    if (snap_revision <= _last_applied_revision_id) {
        co_return;
    }

    // 1. reconcile the _topics and _updates_in_progress state and generate
    // corresponding deltas.

    const auto& snap = controller_snap.topics;
    snapshot_applier applier(*this);

    // Go over the old topics set, delete those that are not present in
    // the snapshot and reconcile those that are.
    for (auto old_it = _topics.begin(); old_it != _topics.end();) {
        const auto& ns_tp = old_it->first;
        auto& md_item = old_it->second;

        if (auto new_it = snap.topics.find(ns_tp);
            new_it != snap.topics.end()) {
            const auto& topic_snapshot = new_it->second;
            if (
              topic_snapshot.metadata.revision
              != md_item.metadata.get_revision()) {
                // The topic was re-created, delete and add it anew.
                co_await applier.delete_topic(
                  ns_tp, md_item, topic_snapshot.metadata.revision);
                md_item = co_await applier.create_topic(ns_tp, topic_snapshot);
            } else {
                // The topic was present in the previous set, now we need to
                // reconcile individual partitions.

                // 1. update configuration in the topic table

                const bool must_update_properties
                  = md_item.get_configuration().properties
                    != topic_snapshot.metadata.configuration.properties;

                md_item.metadata.get_fields() = topic_snapshot.metadata;

                // 2. For each partition in the new set, reconcile assignments
                // and add corresponding deltas
                for (const auto& [p_id, partition] :
                     topic_snapshot.partitions) {
                    model::ntp ntp(ns_tp.ns, ns_tp.tp, p_id);
                    applier.add_ntp(
                      ntp,
                      topic_snapshot,
                      partition,
                      md_item,
                      must_update_properties);
                    co_await ss::coroutine::maybe_yield();
                }

                // 3. For each partition not present in the new set (not
                // possible with current controller commands, but do it anyway
                // for completeness), generate del delta.
                for (auto as_it = md_item.get_assignments().begin();
                     as_it != md_item.get_assignments().end();) {
                    auto as_it_copy = as_it++;
                    if (!topic_snapshot.partitions.contains(as_it_copy->id)) {
                        applier.delete_ntp(
                          ns_tp,
                          md_item.is_topic_replicable(),
                          *as_it_copy,
                          snap_revision);
                        md_item.get_assignments().erase(as_it_copy);
                    }
                    co_await ss::coroutine::maybe_yield();
                }
            }

            ++old_it;
        } else {
            // For topics that are not present in the new set, simply
            // remove them and generate del deltas.
            co_await applier.delete_topic(ns_tp, md_item, snap_revision);
            auto to_delete = old_it++;
            _topics.erase(to_delete);
        }
    }

    // Next, go over the new topics set and add state for new topics.
    for (const auto& [ns_tp, topic] : snap.topics) {
        if (!_topics.contains(ns_tp)) {
            _topics.emplace(ns_tp, co_await applier.create_topic(ns_tp, topic));
        }
    }

    // 2. re-calculate derived state

    _topics_hierarchy.clear();
    for (const auto& [ns_tp, md_item] : _topics) {
        if (!md_item.metadata.is_topic_replicable()) {
            // NOTE: here we assume that source and derivative topics are in the
            // same namespace.
            model::topic_namespace_view src_ns_tp(
              ns_tp.ns, md_item.metadata.get_source_topic());
            _topics_hierarchy[src_ns_tp].insert(ns_tp);
        }
        co_await ss::coroutine::maybe_yield();
    }

    _partition_count = 0;
    for (const auto& [ns_tp, md_item] : _topics) {
        if (md_item.metadata.is_topic_replicable()) {
            _partition_count += md_item.metadata.get_assignments().size();
        }
        co_await ss::coroutine::maybe_yield();
    }

    // 3. notify delta waiters
    notify_waiters();

    _last_applied_revision_id = snap_revision;
}

void topic_table::notify_waiters() {
    /// If by invocation of this method there are no waiters, notify
    /// function_ptrs stored in \ref notifications, without consuming all
    /// pending_deltas for when a subsequent waiter does arrive

    // \ref notify_waiters is called after every apply. Hence for the most
    // part there should only be a few items towards the end of \ref
    // _pending_deltas that need to be sent to callbacks in \ref
    // notifications. \ref _last_consumed_by_notifier_offset allows us to
    // skip ahead to the items added since the last \ref notify_waiters
    // call.
    auto starting_iter = _pending_deltas.cbegin()
                         + _last_consumed_by_notifier_offset;

    delta_range_t changes{starting_iter, _pending_deltas.cend()};

    if (!changes.empty()) {
        for (auto& cb : _notifications) {
            cb.second(changes);
        }
    }

    _last_consumed_by_notifier_offset = _pending_deltas.end()
                                        - _pending_deltas.begin();

    /// Consume all pending deltas
    if (!_waiters.empty()) {
        std::vector<std::unique_ptr<waiter>> active_waiters;
        active_waiters.swap(_waiters);
        for (auto& w :
             std::span{active_waiters.begin(), active_waiters.size() - 1}) {
            w->promise.set_value(_pending_deltas.copy());
        }

        active_waiters[active_waiters.size() - 1]->promise.set_value(
          std::move(_pending_deltas));
        std::exchange(_pending_deltas, {});
        _last_consumed_by_notifier_offset = 0;
    }
}

ss::future<fragmented_vector<topic_table::delta>>
topic_table::wait_for_changes(ss::abort_source& as) {
    using ret_t = fragmented_vector<topic_table::delta>;
    if (!_pending_deltas.empty()) {
        ret_t ret;
        ret.swap(_pending_deltas);
        // \ref notify_waiters uses this member to skip ahead in \ref
        // _pending_deltas to items not yet sent to callbacks in \ref
        // _notifications. Since we are clearing \ref _pending_deltas here we
        // need to clear this member too.
        _last_consumed_by_notifier_offset = 0;
        return ss::make_ready_future<ret_t>(std::move(ret));
    }
    auto w = std::make_unique<waiter>(_waiter_id++);
    auto opt_sub = as.subscribe(
      [this, &pr = w->promise, id = w->id]() noexcept {
          pr.set_exception(ss::abort_requested_exception{});
          auto it = std::find_if(
            _waiters.begin(),
            _waiters.end(),
            [id](std::unique_ptr<waiter>& ptr) { return ptr->id == id; });
          if (it != _waiters.end()) {
              _waiters.erase(it);
          }
      });

    if (unlikely(!opt_sub)) {
        return ss::make_exception_future<ret_t>(
          ss::abort_requested_exception{});
    } else {
        w->sub = std::move(*opt_sub);
    }

    auto f = w->promise.get_future();
    _waiters.push_back(std::move(w));
    return f;
}

std::vector<model::topic_namespace> topic_table::all_topics() const {
    return transform_topics([](const topic_metadata_item& tp) {
        return tp.get_configuration().tp_ns;
    });
}

size_t topic_table::all_topics_count() const { return _topics.size(); }

std::optional<topic_metadata>
topic_table::get_topic_metadata(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.metadata;
    }
    return {};
}
std::optional<std::reference_wrapper<const topic_metadata>>
topic_table::get_topic_metadata_ref(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.metadata;
    }
    return {};
}

std::optional<topic_configuration>
topic_table::get_topic_cfg(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.get_configuration();
    }
    return {};
}

std::optional<replication_factor> topic_table::get_topic_replication_factor(
  model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.get_replication_factor();
    }
    return {};
}

std::optional<assignments_set>
topic_table::get_topic_assignments(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.get_assignments();
    }
    return std::nullopt;
}

std::optional<model::timestamp_type>
topic_table::get_topic_timestamp_type(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.get_configuration().properties.timestamp_type;
    }
    return {};
}

const topic_table::underlying_t& topic_table::all_topics_metadata() const {
    return _topics;
}

bool topic_table::contains(
  model::topic_namespace_view topic, model::partition_id pid) const {
    if (auto it = _topics.find(topic); it != _topics.end()) {
        return it->second.get_assignments().contains(pid);
    }
    return false;
}

topic_table::topic_state topic_table::get_topic_state(
  model::topic_namespace_view tp, model::revision_id id) const {
    if (id > _last_applied_revision_id) {
        // Cache is not sufficiently up to date to give a
        // reliable result.
        return topic_state::indeterminate;
    }
    const auto& topic_md = get_topic_metadata_ref(tp);
    auto exists = topic_md && topic_md->get().get_revision() == id;
    return exists ? topic_state::exists : topic_state::not_exists;
}

std::optional<cluster::partition_assignment>
topic_table::get_partition_assignment(const model::ntp& ntp) const {
    auto it = _topics.find(model::topic_namespace_view(ntp));
    if (it == _topics.end()) {
        return {};
    }

    auto p_it = it->second.get_assignments().find(ntp.tp.partition);

    if (p_it == it->second.get_assignments().cend()) {
        return {};
    }

    return *p_it;
}

bool topic_table::is_update_in_progress(const model::ntp& ntp) const {
    return _updates_in_progress.contains(ntp);
}

std::optional<model::initial_revision_id>
topic_table::get_initial_revision(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.get_remote_revision().value_or(
          model::initial_revision_id(it->second.get_revision()()));
    }
    return std::nullopt;
}

std::optional<model::initial_revision_id>
topic_table::get_initial_revision(const model::ntp& ntp) const {
    return get_initial_revision(model::topic_namespace_view(ntp));
}

std::optional<std::vector<model::broker_shard>>
topic_table::get_previous_replica_set(const model::ntp& ntp) const {
    if (auto it = _updates_in_progress.find(ntp);
        it != _updates_in_progress.end()) {
        return it->second.get_previous_replicas();
    }
    return std::nullopt;
}
std::optional<std::vector<model::broker_shard>>
topic_table::get_target_replica_set(const model::ntp& ntp) const {
    if (auto it = _updates_in_progress.find(ntp);
        it != _updates_in_progress.end()) {
        return it->second.get_target_replicas();
    }
    return std::nullopt;
}

std::vector<model::ntp>
topic_table::all_ntps_moving_per_node(model::node_id node) const {
    std::vector<model::ntp> ret;

    for (const auto& [ntp, state] : _updates_in_progress) {
        auto current_assignment = get_partition_assignment(ntp);
        if (unlikely(!current_assignment)) {
            continue;
        }
        const auto in_previous = contains_node(
          state.get_previous_replicas(), node);
        const auto in_current = contains_node(
          current_assignment->replicas, node);

        if ((in_previous && in_current) || (!in_previous && !in_current)) {
            continue;
        }

        ret.push_back(ntp);
    }
    return ret;
}

std::vector<model::ntp>
topic_table::ntps_moving_to_node(model::node_id node) const {
    std::vector<model::ntp> ret;

    for (const auto& [ntp, state] : _updates_in_progress) {
        auto current_assignment = get_partition_assignment(ntp);
        if (unlikely(!current_assignment)) {
            continue;
        }
        if (moving_to_node(
              node,
              state.get_previous_replicas(),
              current_assignment->replicas)) {
            ret.push_back(ntp);
        }
    }
    return ret;
}

std::vector<model::ntp>
topic_table::ntps_moving_from_node(model::node_id node) const {
    std::vector<model::ntp> ret;

    for (const auto& [ntp, state] : _updates_in_progress) {
        auto current_assignment = get_partition_assignment(ntp);
        if (unlikely(!current_assignment)) {
            continue;
        }
        if (moving_from_node(
              node,
              state.get_previous_replicas(),
              current_assignment->replicas)) {
            ret.push_back(ntp);
        }
    }
    return ret;
}

std::vector<model::ntp> topic_table::all_updates_in_progress() const {
    std::vector<model::ntp> ret;
    ret.reserve(_updates_in_progress.size());
    for (const auto& [ntp, _] : _updates_in_progress) {
        ret.push_back(ntp);
    }

    return ret;
}

void topic_table::change_partition_replicas(
  model::ntp ntp,
  const std::vector<model::broker_shard>& new_assignment,
  topic_metadata_item& metadata,
  partition_assignment& current_assignment,
  model::offset o) {
    if (are_replica_sets_equal(current_assignment.replicas, new_assignment)) {
        return;
    }

    auto update_revision = model::revision_id{o};

    _updates_in_progress.emplace(
      ntp,
      in_progress_update(
        current_assignment.replicas,
        new_assignment,
        reconfiguration_state::in_progress,
        update_revision,
        _probe));
    auto previous_assignment = current_assignment.replicas;
    // replace partition replica set
    current_assignment.replicas = new_assignment;

    /// Update all non_replicable topics to have the same 'in-progress' state
    auto found = _topics_hierarchy.find(model::topic_namespace_view(ntp));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            /// Insert non-replicable topic into the 'updates_in_progress' set
            auto [_, success] = _updates_in_progress.emplace(
              model::ntp(cs.ns, cs.tp, ntp.tp.partition),
              in_progress_update(
                current_assignment.replicas,
                new_assignment,
                reconfiguration_state::in_progress,
                update_revision,
                _probe));
            vassert(
              success,
              "non_replicable topic {}-{} already in _updates_in_progress set",
              cs.tp,
              ntp.tp.partition);
            /// For each child topic of the to-be-moved source, update its new
            /// replica assignment to reflect the change
            auto sfound = _topics.find(cs);
            vassert(
              sfound != _topics.end(),
              "Non replicable topic must exist: {}",
              cs);
            auto assignment_it = sfound->second.get_assignments().find(
              ntp.tp.partition);
            vassert(
              assignment_it != sfound->second.get_assignments().end(),
              "Non replicable partition doesn't exist: {}-{}",
              cs,
              ntp.tp.partition);
            /// The new assignments of the non_replicable topic/partition must
            /// match the source topic
            assignment_it->replicas = new_assignment;
        }
    }

    // calculate deleta for backend

    auto partition_it = metadata.partitions.find(ntp.tp.partition);
    vassert(
      partition_it != metadata.partitions.end(),
      "partition {} must exist in the partition map",
      ntp);

    _pending_deltas.emplace_back(
      std::move(ntp),
      current_assignment,
      o,
      delta::op_type::update,
      std::move(previous_assignment),
      update_replicas_revisions(
        partition_it->second.replicas_revisions,
        new_assignment,
        update_revision));
}

size_t topic_table::get_node_partition_count(model::node_id id) const {
    size_t cnt = 0;
    // NOTE: if this loop will cause reactor stalls with large partition counts
    // we may consider making this method asynchronous
    for (const auto& [_, tp_md] : _topics) {
        cnt += std::count_if(
          tp_md.get_assignments().begin(),
          tp_md.get_assignments().end(),
          [id](const partition_assignment& p_as) {
              return contains_node(p_as.replicas, id);
          });
    }
    return cnt;
}

} // namespace cluster
