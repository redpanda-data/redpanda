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
#include "cluster/topic_validators.h"
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

    if (!schema_id_validation_validator::is_valid(cmd.value.cfg.properties)) {
        return ss::make_ready_future<std::error_code>(
          schema_id_validation_validator::ec);
    }

    std::optional<model::initial_revision_id> remote_revision
      = cmd.value.cfg.properties.remote_topic_properties ? std::make_optional(
          cmd.value.cfg.properties.remote_topic_properties->remote_revision)
                                                         : std::nullopt;
    auto assignments_size = cmd.value.assignments.size();
    auto md = topic_metadata_item{
      .metadata = topic_metadata(
        std::move(cmd.value), model::revision_id(offset()), remote_revision)};
    // calculate delta
    md.partitions.reserve(assignments_size);
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
          model::revision_id(offset),
          topic_table_delta_type::added);
    }

    _topics.insert({
      cmd.key,
      std::move(md),
    });
    _topics_map_revision++;
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

    co_return do_local_delete(cmd.key, offset);
}

std::error_code
topic_table::do_local_delete(model::topic_namespace nt, model::offset offset) {
    if (auto tp = _topics.find(nt); tp != _topics.end()) {
        for (auto& p_as : tp->second.get_assignments()) {
            _partition_count--;
            auto ntp = model::ntp(nt.ns, nt.tp, p_as.id);
            _updates_in_progress.erase(ntp);
            on_partition_deletion(ntp);
            _pending_deltas.emplace_back(
              std::move(ntp),
              model::revision_id(offset),
              topic_table_delta_type::removed);
        }

        _topics.erase(tp);
        _disabled_partitions.erase(nt);
        _topics_map_revision++;
        notify_waiters();
        _probe.handle_topic_deletion(nt);

        return errc::success;
    }

    return errc::topic_not_exists;
}

ss::future<std::error_code>
topic_table::apply(topic_lifecycle_transition soft_del, model::offset offset) {
    _last_applied_revision_id = model::revision_id(offset);

    if (soft_del.mode == topic_lifecycle_transition_mode::pending_gc) {
        // Create a lifecycle marker
        auto tp = _topics.find(soft_del.topic.nt);
        if (tp == _topics.end()) {
            return ss::make_ready_future<std::error_code>(
              errc::topic_not_exists);
        }

        auto tombstone = nt_lifecycle_marker{
          .config = tp->second.get_configuration(),
          .initial_revision_id = tp->second.get_remote_revision().value_or(
            model::initial_revision_id(tp->second.get_revision())),
          .timestamp = ss::lowres_system_clock::now()};

        _lifecycle_markers.emplace(soft_del.topic, tombstone);
        vlog(
          clusterlog.debug,
          "Created lifecycle marker for topic {} {}",
          soft_del.topic.nt,
          soft_del.topic.initial_revision_id);
    } else if (soft_del.mode == topic_lifecycle_transition_mode::drop) {
        if (_lifecycle_markers.contains(soft_del.topic)) {
            vlog(
              clusterlog.debug,
              "Purged lifecycle marker for {} {}",
              soft_del.topic.nt,
              soft_del.topic.initial_revision_id);
            _lifecycle_markers.erase(soft_del.topic);
            return ss::make_ready_future<std::error_code>(errc::success);
        } else {
            vlog(
              clusterlog.info,
              "Unexpected record at offset {} to drop non-existent lifecycle "
              "marker {} {}",
              offset,
              soft_del.topic.nt,
              soft_del.topic.initial_revision_id);
            return ss::make_ready_future<std::error_code>(
              errc::topic_not_exists);
        }
    }

    if (
      soft_del.mode == topic_lifecycle_transition_mode::pending_gc
      || soft_del.mode == topic_lifecycle_transition_mode::oneshot_delete) {
        return ss::make_ready_future<std::error_code>(
          do_local_delete(soft_del.topic.nt, offset));
    } else {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
}

ss::future<std::error_code>
topic_table::apply(create_partition_cmd cmd, model::offset offset) {
    _last_applied_revision_id = model::revision_id(offset);
    auto tp = _topics.find(cmd.key);
    if (tp == _topics.end()) {
        co_return errc::topic_not_exists;
    }

    if (is_fully_disabled(cmd.key)) {
        co_return errc::topic_disabled;
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
        _topics_map_revision++;
        _pending_deltas.emplace_back(
          std::move(ntp),
          model::revision_id(offset),
          topic_table_delta_type::added);
    }
    notify_waiters();
    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(move_partition_replicas_cmd cmd, model::offset o) {
    return do_apply(
      update_partition_replicas_cmd_data{
        .ntp = std::move(cmd.key),
        .replicas = std::move(cmd.value),
        // default policy for legacy partition move
        .policy = reconfiguration_policy::full_local_retention},
      o);
}

ss::future<std::error_code>
topic_table::apply(update_partition_replicas_cmd cmd, model::offset o) {
    return do_apply(std::move(cmd.value), o);
}
ss::future<std::error_code> topic_table::do_apply(
  update_partition_replicas_cmd_data cmd_data, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);

    auto tp = _topics.find(model::topic_namespace_view(cmd_data.ntp));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }

    auto current_assignment_it = tp->second.get_assignments().find(
      cmd_data.ntp.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (is_disabled(cmd_data.ntp)) {
        return ss::make_ready_future<std::error_code>(errc::partition_disabled);
    }

    if (_updates_in_progress.contains(cmd_data.ntp)) {
        return ss::make_ready_future<std::error_code>(errc::update_in_progress);
    }

    change_partition_replicas(
      std::move(cmd_data.ntp),
      cmd_data.replicas,
      *current_assignment_it,
      o,
      false,
      cmd_data.policy);
    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

static replicas_revision_map update_replicas_revisions(
  replicas_revision_map revisions,
  const replicas_t& new_assignment,
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
    if (!is_cancelled_state(it->second.get_state())) {
        // update went through and the cancellation didn't happen, we must
        // update replicas_revisions.
        p_meta_it->second.replicas_revisions = update_replicas_revisions(
          std::move(p_meta_it->second.replicas_revisions),
          cmd.value,
          it->second.get_update_revision());
    }
    p_meta_it->second.last_update_finished_revision = model::revision_id{o};

    _updates_in_progress.erase(it);

    on_partition_move_finish(cmd.key, cmd.value);

    // notify backend about finished update
    _pending_deltas.emplace_back(
      std::move(cmd.key),
      model::revision_id(o),
      topic_table_delta_type::replicas_updated);

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

    auto current_assignment_it = tp->second.get_assignments().find(
      cmd.key.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        co_return errc::partition_not_exists;
    }

    if (is_disabled(cmd.key)) {
        co_return errc::partition_disabled;
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
    case reconfiguration_state::force_update:
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

    _pending_deltas.emplace_back(
      std::move(cmd.key),
      model::revision_id(o),
      topic_table_delta_type::replicas_updated);
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
    if (!is_cancelled_state(in_progress_it->second.get_state())) {
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

    // notify backend about finished update
    _pending_deltas.emplace_back(
      ntp, model::revision_id(o), topic_table_delta_type::replicas_updated);
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

    // We should check partition before create updates

    const auto* disabled_set = get_topic_disabled_set(cmd.key);
    if (disabled_set && disabled_set->is_fully_disabled()) {
        vlog(
          clusterlog.warn,
          "topic {}: Can not move replicas, topic disabled",
          cmd.key);
        co_return errc::topic_disabled;
    }

    for (const auto& partition_and_replicas : cmd.value) {
        auto partition = partition_and_replicas.partition;
        if (!tp->second.get_assignments().contains(partition)) {
            vlog(
              clusterlog.warn,
              "topic {}: Can not move replicas, partition {} not found",
              cmd.key,
              partition);
            co_return errc::partition_not_exists;
        }

        if (disabled_set && disabled_set->is_disabled(partition)) {
            vlog(
              clusterlog.warn,
              "topic {}: Can not move replicas, partition {} disabled",
              cmd.key,
              partition);
            co_return errc::partition_disabled;
        }

        if (_updates_in_progress.contains(
              model::ntp{cmd.key.ns, cmd.key.tp, partition})) {
            vlog(
              clusterlog.warn,
              "topic {}: Can not move replicas, update for partition {} is in "
              "progress",
              cmd.key,
              partition);
            co_return errc::update_in_progress;
        }
    }

    for (const auto& [partition_id, new_replicas] : cmd.value) {
        auto assignment = tp->second.get_assignments().find(partition_id);
        change_partition_replicas(
          model::ntp(cmd.key.ns, cmd.key.tp, partition_id),
          new_replicas,
          *assignment,
          o,
          false,
          // for up replication we use a default reconfiguration policy
          reconfiguration_policy::full_local_retention);
    }

    notify_waiters();

    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(force_partition_reconfiguration_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    // Check the topic exists.
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }

    auto current_assignment_it = tp->second.get_assignments().find(
      cmd.key.tp.partition);

    if (current_assignment_it == tp->second.get_assignments().end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (is_disabled(cmd.key)) {
        return ss::make_ready_future<std::error_code>(errc::partition_disabled);
    }

    if (auto it = _updates_in_progress.find(cmd.key);
        it != _updates_in_progress.end()) {
        return ss::make_ready_future<std::error_code>(errc::update_in_progress);
    }

    change_partition_replicas(
      cmd.key,
      cmd.value.replicas,
      *current_assignment_it,
      o,
      true,
      /**
       * For now use default full local retention policy when force
       * reconfiguring partition.
       */
      reconfiguration_policy::full_local_retention);
    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<std::error_code>
topic_table::apply(set_topic_partitions_disabled_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);

    auto topic_it = _topics.find(cmd.value.ns_tp);
    if (topic_it == _topics.end()) {
        co_return errc::topic_not_exists;
    }
    const auto& assignments = topic_it->second.get_assignments();

    if (cmd.value.partition_id) {
        if (!assignments.contains(*cmd.value.partition_id)) {
            co_return errc::partition_not_exists;
        }

        auto [disabled_it, inserted] = _disabled_partitions.try_emplace(
          cmd.value.ns_tp);
        auto& disabled_set = disabled_it->second;

        if (cmd.value.disabled) {
            disabled_set.add(*cmd.value.partition_id);
        } else {
            disabled_set.remove(*cmd.value.partition_id, assignments);
        }

        if (disabled_set.is_fully_enabled()) {
            _disabled_partitions.erase(disabled_it);
        }

        _pending_deltas.emplace_back(
          model::ntp{
            cmd.value.ns_tp.ns, cmd.value.ns_tp.tp, *cmd.value.partition_id},
          model::revision_id{o},
          topic_table_delta_type::disabled_flag_updated);
    } else {
        topic_disabled_partitions_set old_disabled_set;
        if (cmd.value.disabled) {
            auto& disabled_set = _disabled_partitions[cmd.value.ns_tp];
            old_disabled_set = std::exchange(
              disabled_set, topic_disabled_partitions_set{});
            disabled_set.set_fully_disabled();
        } else {
            auto it = _disabled_partitions.find(cmd.value.ns_tp);
            if (it != _disabled_partitions.end()) {
                old_disabled_set = std::move(it->second);
                _disabled_partitions.erase(it);
            }
        }

        for (const auto& p_as : assignments) {
            if (old_disabled_set.is_disabled(p_as.id) == cmd.value.disabled) {
                continue;
            }

            _pending_deltas.emplace_back(
              model::ntp{cmd.value.ns_tp.ns, cmd.value.ns_tp.tp, p_as.id},
              model::revision_id{o},
              topic_table_delta_type::disabled_flag_updated);
        }
    }

    notify_waiters();

    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(bulk_force_reconfiguration_cmd cmd, model::offset o) {
    _last_applied_revision_id = model::revision_id(o);
    auto validation_ec = validate_force_reconfigurable_partitions(
      cmd.value.user_approved_force_recovery_partitions);
    if (validation_ec) {
        co_return validation_ec;
    }
    for (auto& entry : cmd.value.user_approved_force_recovery_partitions) {
        add_partition_to_force_reconfigure(std::move(entry));
    }
    co_return errc::success;
}

std::error_code topic_table::validate_force_reconfigurable_partition(
  const ntp_with_majority_loss& entry) const {
    if (entry.dead_nodes.size() <= 0) {
        return errc::invalid_request;
    }
    const auto& ntp = entry.ntp;
    const auto& topic_md = get_topic_metadata_ref({ntp.ns, ntp.tp.topic});
    if (!topic_md || topic_md->get().get_revision() != entry.topic_revision) {
        return errc::topic_not_exists;
    }
    if (is_update_in_progress(ntp)) {
        return errc::update_in_progress;
    }
    const auto& current_assignment = get_partition_assignment(ntp);
    if (!current_assignment) {
        return errc::no_partition_assignments;
    }
    if (!are_replica_sets_equal(
          current_assignment->replicas, entry.assignment)) {
        return errc::partition_configuration_differs;
    }
    // check if the entry is already in progress
    auto it = _partitions_to_force_reconfigure.find(ntp);
    if (it != _partitions_to_force_reconfigure.end()) {
        const auto& entries = it->second;
        auto match_it = std::find(entries.begin(), entries.end(), entry);
        if (match_it != entries.end()) {
            return errc::partition_already_exists;
        }
    }
    return errc::success;
}

std::error_code topic_table::validate_force_reconfigurable_partitions(
  const fragmented_vector<ntp_with_majority_loss>& partitions) const {
    std::error_code result = errc::success;
    for (const auto& entry : partitions) {
        auto error = validate_force_reconfigurable_partition(entry);
        if (error) {
            result = error;
        }
        vlog(
          clusterlog.debug,
          "Processed force reconfiguration entry {}, result: {}",
          entry,
          error);
    }
    return result;
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
    if (tp == _topics.end()) {
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
    incremental_update(
      properties.record_key_schema_id_validation,
      overrides.record_key_schema_id_validation);
    incremental_update(
      properties.record_key_schema_id_validation_compat,
      overrides.record_key_schema_id_validation_compat);
    incremental_update(
      properties.record_key_subject_name_strategy,
      overrides.record_key_subject_name_strategy);
    incremental_update(
      properties.record_key_subject_name_strategy_compat,
      overrides.record_key_subject_name_strategy_compat);
    incremental_update(
      properties.record_value_schema_id_validation,
      overrides.record_value_schema_id_validation);
    incremental_update(
      properties.record_value_schema_id_validation_compat,
      overrides.record_value_schema_id_validation_compat);
    incremental_update(
      properties.record_value_subject_name_strategy,
      overrides.record_value_subject_name_strategy);
    incremental_update(
      properties.record_value_subject_name_strategy_compat,
      overrides.record_value_subject_name_strategy_compat);
    incremental_update(
      properties.initial_retention_local_target_bytes,
      overrides.initial_retention_local_target_bytes);
    incremental_update(
      properties.initial_retention_local_target_ms,
      overrides.initial_retention_local_target_ms);
    // no configuration change, no need to generate delta
    if (properties == properties_snapshot) {
        co_return errc::success;
    }

    if (!schema_id_validation_validator::is_valid(properties)) {
        co_return schema_id_validation_validator::ec;
    }

    // generate deltas for controller backend
    const auto& assignments = tp->second.get_assignments();
    for (auto& p_as : assignments) {
        _pending_deltas.emplace_back(
          model::ntp(cmd.key.ns, cmd.key.tp, p_as.id),
          model::revision_id(o),
          topic_table_delta_type::properties_updated);
    }
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
            replicas_t replicas;
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

        std::optional<topic_disabled_partitions_set> disabled_set;
        if (auto it = _disabled_partitions.find(ns_tp);
            it != _disabled_partitions.end()) {
            disabled_set = it->second;
        }

        snap.topics.emplace(
          ns_tp,
          controller_snapshot_parts::topics_t::topic_t{
            .metadata = md_item.metadata.get_fields(),
            .partitions = std::move(partitions),
            .updates = std::move(updates),
            .disabled_set = std::move(disabled_set),
          });
    }

    for (const auto& [ntr, lm] : _lifecycle_markers) {
        snap.lifecycle_markers.emplace(ntr, lm);
    }

    snap.partitions_to_force_recover = _partitions_to_force_reconfigure;
}

// helper class to hold context needed for adding/deleting ntps when applying a
// controller snapshot
class topic_table::snapshot_applier {
    updates_t& _updates_in_progress;
    disabled_partitions_t& _disabled_partitions;
    fragmented_vector<delta>& _pending_deltas;
    topic_table_probe& _probe;
    model::revision_id _snap_revision;

public:
    snapshot_applier(topic_table& parent, model::revision_id snap_revision)
      : _updates_in_progress(parent._updates_in_progress)
      , _disabled_partitions(parent._disabled_partitions)
      , _pending_deltas(parent._pending_deltas)
      , _probe(parent._probe)
      , _snap_revision(snap_revision) {}

    void delete_ntp(
      const model::topic_namespace& ns_tp, const partition_assignment& p_as) {
        auto ntp = model::ntp(ns_tp.ns, ns_tp.tp, p_as.id);
        vlog(
          clusterlog.trace, "deleting ntp {} not in controller snapshot", ntp);
        _updates_in_progress.erase(ntp);

        _pending_deltas.emplace_back(
          std::move(ntp), _snap_revision, topic_table_delta_type::removed);
        // partition_assignment object is supposed to be removed from the
        // assignments set by the caller
    }

    ss::future<> delete_topic(
      const model::topic_namespace& ns_tp,
      const topic_metadata_item& old_md_item) {
        vlog(
          clusterlog.trace,
          "deleting topic {} not in controller snapshot",
          ns_tp);
        for (const auto& p_as : old_md_item.get_assignments()) {
            delete_ntp(ns_tp, p_as);
            co_await ss::coroutine::maybe_yield();
        }
        _disabled_partitions.erase(ns_tp);
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

        // 1. reconcile the _topics state (the md_item object) and generate
        // related deltas

        std::optional<partition_assignment> prev_assignment;
        model::revision_id prev_update_finished_revision;
        if (auto as_it = md_item.get_assignments().find(p_id);
            as_it != md_item.get_assignments().end()) {
            prev_assignment = std::move(*as_it);
            md_item.get_assignments().erase(as_it);

            auto p_it = md_item.partitions.find(p_id);
            vassert(
              p_it != md_item.partitions.end(),
              "ntp {} must be present in the partition map",
              ntp);
            prev_update_finished_revision
              = p_it->second.last_update_finished_revision;
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

        md_item.partitions[p_id] = partition_meta{
          .replicas_revisions = partition.replicas_revisions,
          .last_update_finished_revision
          = partition.last_update_finished_revision,
        };

        if (!prev_assignment) {
            // new partition
            _pending_deltas.emplace_back(
              ntp, _snap_revision, topic_table_delta_type::added);
        }

        if (must_update_properties) {
            _pending_deltas.emplace_back(
              ntp, _snap_revision, topic_table_delta_type::properties_updated);
        }

        // 2. reconcile the _updates_in_progress state and possibly generate
        // delta related to the replicas update

        // Determine if the snapshot contains the result of any new replica
        // update commands by comparing last command revision.

        model::revision_id prev_last_replica_update_revision
          = prev_update_finished_revision;
        if (auto prev_update_it = _updates_in_progress.find(ntp);
            prev_update_it != _updates_in_progress.end()) {
            prev_last_replica_update_revision
              = prev_update_it->second.get_last_cmd_revision();
            _updates_in_progress.erase(prev_update_it);
        }

        model::revision_id last_replica_update_revision
          = partition.last_update_finished_revision;
        if (auto update_it = topic.updates.find(p_id);
            update_it != topic.updates.end()) {
            const auto& update = update_it->second;
            last_replica_update_revision = update.last_cmd_revision;

            if (!is_cancelled_state(update.state)) {
                cur_assignment.replicas = update_it->second.target_assignment;
            }

            auto initial_state = update.state
                                     == reconfiguration_state::force_update
                                   ? update.state
                                   : reconfiguration_state::in_progress;
            in_progress_update inp_update{
              partition.replicas,
              update.target_assignment,
              initial_state,
              update.revision,
              update.policy,
              _probe,
            };
            inp_update.set_state(update.state, update.last_cmd_revision);
            vlog(
              clusterlog.trace,
              "{}: prev_last_replica_update_revision: {}, new: state: {} "
              "update_rev: {} last_cmd_rev: {}",
              ntp,
              prev_last_replica_update_revision,
              inp_update.get_state(),
              inp_update.get_update_revision(),
              inp_update.get_last_cmd_revision());
            _updates_in_progress.emplace(ntp, std::move(inp_update));
        }

        if (
          prev_assignment
          && last_replica_update_revision
               != prev_last_replica_update_revision) {
            _pending_deltas.emplace_back(
              ntp, _snap_revision, topic_table_delta_type::replicas_updated);
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
        if (topic.disabled_set) {
            _disabled_partitions[ns_tp] = *topic.disabled_set;
        }
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
    snapshot_applier applier(*this, snap_revision);

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
                co_await applier.delete_topic(ns_tp, md_item);
                md_item = co_await applier.create_topic(ns_tp, topic_snapshot);
            } else {
                // The topic was present in the previous set, now we need to
                // reconcile individual partitions.

                // 1. update configuration in the topic table

                const bool must_update_properties
                  = md_item.get_configuration().properties
                    != topic_snapshot.metadata.configuration.properties;

                md_item.metadata.get_fields() = topic_snapshot.metadata;

                topic_disabled_partitions_set old_disabled_set;
                if (topic_snapshot.disabled_set) {
                    old_disabled_set = std::exchange(
                      _disabled_partitions[ns_tp],
                      *topic_snapshot.disabled_set);
                } else if (auto it = _disabled_partitions.find(ns_tp);
                           it != _disabled_partitions.end()) {
                    old_disabled_set = std::move(it->second);
                    _disabled_partitions.erase(it);
                }

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

                    const bool new_is_disabled
                      = topic_snapshot.disabled_set
                        && topic_snapshot.disabled_set->is_disabled(p_id);
                    if (old_disabled_set.is_disabled(p_id) != new_is_disabled) {
                        _pending_deltas.emplace_back(
                          ntp,
                          snap_revision,
                          topic_table_delta_type::disabled_flag_updated);
                    }

                    co_await ss::coroutine::maybe_yield();
                }

                // 3. For each partition not present in the new set (not
                // possible with current controller commands, but do it anyway
                // for completeness), generate del delta.
                for (auto as_it = md_item.get_assignments().begin();
                     as_it != md_item.get_assignments().end();) {
                    auto as_it_copy = as_it++;
                    if (!topic_snapshot.partitions.contains(as_it_copy->id)) {
                        applier.delete_ntp(ns_tp, *as_it_copy);
                        md_item.get_assignments().erase(as_it_copy);
                    }
                    co_await ss::coroutine::maybe_yield();
                }
            }

            ++old_it;
        } else {
            // For topics that are not present in the new set, simply
            // remove them and generate del deltas.
            co_await applier.delete_topic(ns_tp, md_item);
            auto to_delete = old_it++;
            _topics.erase(to_delete);
            _topics_map_revision++;
        }
    }

    // Next, go over the new topics set and add state for new topics.
    for (const auto& [ns_tp, topic] : snap.topics) {
        if (!_topics.contains(ns_tp)) {
            _topics.emplace(ns_tp, co_await applier.create_topic(ns_tp, topic));
            _topics_map_revision++;
        }
    }

    // Lifecycle markers is a simple static collection without notifications
    // etc, so we can just copy directly into place.
    _lifecycle_markers = controller_snap.topics.lifecycle_markers;

    reset_partitions_to_force_reconfigure(
      controller_snap.topics.partitions_to_force_recover);

    // 2. re-calculate derived state

    _partition_count = 0;
    for (const auto& [ns_tp, md_item] : _topics) {
        _partition_count += md_item.metadata.get_assignments().size();
        co_await ss::coroutine::maybe_yield();
    }

    // 3. notify delta waiters
    notify_waiters();

    _last_applied_revision_id = snap_revision;
}

void topic_table::add_partition_to_force_reconfigure(
  ntp_with_majority_loss entry) {
    const auto& [it, _] = _partitions_to_force_reconfigure.try_emplace(
      entry.ntp);
    it->second.push_back(std::move(entry));
    _partitions_to_force_reconfigure_revision++;
}

void topic_table::reset_partitions_to_force_reconfigure(
  const force_recoverable_partitions_t& other) {
    _partitions_to_force_reconfigure = other;
    _partitions_to_force_reconfigure_revision++;
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

    for (auto& cb : _lw_notifications) {
        cb.second();
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

void topic_table::check_topics_map_stable(model::revision_id start_rev) const {
    if (_topics_map_revision != start_rev) {
        throw concurrent_modification_error(start_rev, _topics_map_revision);
    }
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

std::optional<replicas_t>
topic_table::get_previous_replica_set(const model::ntp& ntp) const {
    if (auto it = _updates_in_progress.find(ntp);
        it != _updates_in_progress.end()) {
        return it->second.get_previous_replicas();
    }
    return std::nullopt;
}
std::optional<replicas_t>
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
  const replicas_t& new_assignment,
  partition_assignment& current_assignment,
  model::offset o,
  bool is_forced,
  reconfiguration_policy policy) {
    if (are_replica_sets_equal(current_assignment.replicas, new_assignment)) {
        return;
    }

    auto update_revision = model::revision_id{o};

    _updates_in_progress.emplace(
      ntp,
      in_progress_update(
        current_assignment.replicas,
        new_assignment,
        is_forced ? reconfiguration_state::force_update
                  : reconfiguration_state::in_progress,
        update_revision,
        policy,
        _probe));
    auto previous_assignment = current_assignment.replicas;
    // replace partition replica set
    current_assignment.replicas = new_assignment;

    // calculate delta for backend

    _pending_deltas.emplace_back(
      std::move(ntp),
      model::revision_id(o),
      topic_table_delta_type::replicas_updated);
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

void topic_table::on_partition_move_finish(
  const model::ntp& ntp, const std::vector<model::broker_shard>& replicas) {
    auto it = _partitions_to_force_reconfigure.find(ntp);
    if (it == _partitions_to_force_reconfigure.end()) {
        return;
    }
    auto& entries = it->second;
    auto num_erased = std::erase_if(entries, [&replicas](const auto& entry) {
        // check if the new replica set contains any dead nodes that were
        // intended to be removed.
        bool has_dead_nodes = std::any_of(
          entry.dead_nodes.begin(),
          entry.dead_nodes.end(),
          [&replicas](const model::node_id& id) {
              return contains_node(replicas, id);
          });
        return !has_dead_nodes;
    });
    if (num_erased > 0) {
        _partitions_to_force_reconfigure_revision++;
    }
    if (entries.size() == 0) {
        _partitions_to_force_reconfigure.erase(it);
        _partitions_to_force_reconfigure_revision++;
    }
}

void topic_table::on_partition_deletion(const model::ntp& ntp) {
    auto it = _partitions_to_force_reconfigure.find(ntp);
    if (it == _partitions_to_force_reconfigure.end()) {
        return;
    }
    auto topic_md = get_topic_metadata_ref({ntp.ns, ntp.tp.topic});
    if (!topic_md) {
        // topic no longer exists
        _partitions_to_force_reconfigure.erase(it);
        _partitions_to_force_reconfigure_revision++;
        vlog(
          clusterlog.debug,
          "marking force repair action as finished for partition: {} because "
          "the topic was deleted.",
          it->first);
        return;
    }
    // A newer version of the topic exists.
    // delete all entries for older revision of the topic.
    auto& entries = it->second;
    auto new_topic_revision = topic_md.value().get().get_revision();
    auto num_erased = std::erase_if(entries, [&](const auto& entry) {
        return entry.topic_revision < new_topic_revision;
    });
    if (num_erased > 0) {
        _partitions_to_force_reconfigure_revision++;
    }
    if (entries.size() == 0) {
        _partitions_to_force_reconfigure.erase(it);
        _partitions_to_force_reconfigure_revision++;
        vlog(
          clusterlog.debug,
          "marking force repair action as finished for partition: {} because "
          "the topic was deleted.",
          it->first);
    }
}

std::ostream&
operator<<(std::ostream& o, const topic_table::in_progress_update& u) {
    fmt::print(
      o,
      "{{state: {}, update_rev: {}, last_cmd_rev: {}, previous: {}, target: "
      "{}, policy: {}}}",
      u._state,
      u._update_revision,
      u._last_cmd_revision,
      u._previous_replicas,
      u._target_replicas,
      u._policy);
    return o;
}

} // namespace cluster
