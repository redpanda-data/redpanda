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
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/coroutine.hh>

#include <optional>

namespace cluster {

topic_table::topic_table(ss::sharded<partition_manager>& pm)
  : _probe(*this)
  , _pm(pm) {
    register_delta_notification([this](const std::vector<delta>& deltas) {
        _pm.local().notify_topic_table_deltas(deltas);
    });
};

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
    md.replica_revisions.reserve(cmd.value.assignments.size());
    for (auto& pas : md.get_assignments()) {
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, pas.id);
        for (auto& r : pas.replicas) {
            md.replica_revisions[pas.id][r.node_id] = model::revision_id(
              offset);
        }
        _pending_deltas.emplace_back(
          std::move(ntp),
          pas,
          offset,
          delta::op_type::add,
          std::nullopt,
          md.replica_revisions[pas.id]);
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
    for (auto& p_as : cmd.value.assignments) {
        p_as.id += model::partition_id(prev_partition_count);
        tp->second.get_assignments().emplace(p_as);
        // propagate deltas
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p_as.id);
        for (auto& bs : p_as.replicas) {
            tp->second.replica_revisions[p_as.id][bs.node_id]
              = model::revision_id(offset);
        }
        _pending_deltas.emplace_back(
          std::move(ntp),
          std::move(p_as),
          offset,
          delta::op_type::add,
          std::nullopt,
          tp->second.replica_revisions[p_as.id]);
    }

    notify_waiters();
    co_return errc::success;
}

ss::future<std::error_code>
topic_table::apply(move_partition_replicas_cmd cmd, model::offset o) {
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

    // assignment is already up to date, this operation is NOP do not propagate
    // delta

    if (are_replica_sets_equal(current_assignment_it->replicas, cmd.value)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    auto revisions_it = tp->second.replica_revisions.find(cmd.key.tp.partition);
    vassert(
      revisions_it != tp->second.replica_revisions.end(),
      "partition {}, replica revisions map must exists as partition is present",
      cmd.key);

    _updates_in_progress.emplace(
      cmd.key,
      in_progress_update(
        current_assignment_it->replicas,
        cmd.value,
        in_progress_state::update_requested,
        model::revision_id(o),
        // snapshot replicas revisions
        revisions_it->second,
        _probe));
    auto previous_assignment = *current_assignment_it;
    // replace partition replica set
    current_assignment_it->replicas = cmd.value;
    /**
     * Update partition replica revisions. Assign new revision to added replicas
     * and erase replicas which are removed from replica set
     */
    auto added_replicas = subtract_replica_sets_by_node_id(
      current_assignment_it->replicas, previous_assignment.replicas);

    for (auto& r : added_replicas) {
        revisions_it->second[r.node_id] = model::revision_id(o);
    }

    auto removed_replicas = subtract_replica_sets_by_node_id(
      previous_assignment.replicas, current_assignment_it->replicas);

    for (auto& removed : removed_replicas) {
        revisions_it->second.erase(removed.node_id);
    }

    /// Update all non_replicable topics to have the same 'in-progress' state
    auto found = _topics_hierarchy.find(model::topic_namespace_view(cmd.key));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            /// Insert non-replicable topic into the 'updates_in_progress' set
            auto [_, success] = _updates_in_progress.emplace(
              model::ntp(cs.ns, cs.tp, current_assignment_it->id),
              in_progress_update(
                current_assignment_it->replicas,
                cmd.value,
                in_progress_state::update_requested,
                model::revision_id(o),
                // empty replicas revisions
                {},
                _probe));
            vassert(
              success,
              "non_replicable topic {}-{} already in _updates_in_progress set",
              cs.tp,
              current_assignment_it->id);
            /// For each child topic of the to-be-moved source, update its new
            /// replica assignment to reflect the change
            auto sfound = _topics.find(cs);
            vassert(
              sfound != _topics.end(),
              "Non replicable topic must exist: {}",
              cs);
            auto assignment_it = sfound->second.get_assignments().find(
              current_assignment_it->id);
            vassert(
              assignment_it != sfound->second.get_assignments().end(),
              "Non replicable partition doesn't exist: {}-{}",
              cs,
              current_assignment_it->id);
            /// The new assignments of the non_replicable topic/partition must
            /// match the source topic
            assignment_it->replicas = cmd.value;
        }
    }

    // calculate deleta for backend
    model::ntp ntp(tp->first.ns, tp->first.tp, current_assignment_it->id);
    _pending_deltas.emplace_back(
      std::move(ntp),
      *current_assignment_it,
      o,
      delta::op_type::update,
      std::move(previous_assignment.replicas),
      revisions_it->second);

    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<std::error_code>
topic_table::apply(finish_moving_partition_replicas_cmd cmd, model::offset o) {
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
    case in_progress_state::update_requested:
        break;
    case in_progress_state::cancel_requested:
        // partition reconfiguration already cancelled, only allow force
        // cancelling it
        if (cmd.value.force == force_abort_update::no) {
            co_return errc::no_update_in_progress;
        }
        break;
    case in_progress_state::force_cancel_requested:
        // partition reconfiguration already cancelled forcibly
        co_return errc::no_update_in_progress;
    }

    in_progress_it->second.set_state(
      cmd.value.force ? in_progress_state::force_cancel_requested
                      : in_progress_state::cancel_requested);

    auto replicas = current_assignment_it->replicas;
    // replace replica set with set from in progress operation
    current_assignment_it->replicas
      = in_progress_it->second.get_previous_replicas();
    auto revisions_it = tp->second.replica_revisions.find(cmd.key.tp.partition);
    vassert(
      revisions_it != tp->second.replica_revisions.end(),
      "partition {} replica revisions map must exists",
      cmd.key);

    revisions_it->second = in_progress_it->second.get_replicas_revisions();

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
      revisions_it->second);

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

ss::future<std::error_code>
topic_table::apply(update_topic_properties_cmd cmd, model::offset o) {
    auto tp = _topics.find(cmd.key);
    if (tp == _topics.end() || !tp->second.is_topic_replicable()) {
        co_return make_error_code(errc::topic_not_exists);
    }
    auto& properties = tp->second.get_configuration().properties;
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

    // generate deltas for controller backend
    std::vector<topic_table_delta> deltas;
    deltas.reserve(tp->second.get_assignments().size());
    for (const auto& p_as : tp->second.get_assignments()) {
        deltas.emplace_back(
          model::ntp(cmd.key.ns, cmd.key.tp, p_as.id),
          p_as,
          o,
          delta::op_type::update_properties);
    }

    std::move(
      deltas.begin(), deltas.end(), std::back_inserter(_pending_deltas));

    notify_waiters();

    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
topic_table::apply(create_non_replicable_topic_cmd cmd, model::offset o) {
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

void topic_table::notify_waiters() {
    /// If by invocation of this method there are no waiters, notify
    /// function_ptrs stored in \ref notifications, without consuming all
    /// pending_deltas for when a subsequent waiter does arrive
    std::vector<delta> changes;
    std::copy_if(
      _pending_deltas.begin(),
      _pending_deltas.end(),
      std::back_inserter(changes),
      [this](const delta& d) { return d.offset > _last_consumed_by_notifier; });
    for (auto& cb : _notifications) {
        cb.second(changes);
    }
    if (!changes.empty()) {
        _last_consumed_by_notifier = changes.back().offset;
    }

    /// Consume all pending deltas
    if (!_waiters.empty()) {
        changes.clear();
        changes.swap(_pending_deltas);
        std::vector<std::unique_ptr<waiter>> active_waiters;
        active_waiters.swap(_waiters);
        for (auto& w : active_waiters) {
            w->promise.set_value(changes);
        }
    }
}

ss::future<std::vector<topic_table::delta>>
topic_table::wait_for_changes(ss::abort_source& as) {
    using ret_t = std::vector<topic_table::delta>;
    if (!_pending_deltas.empty()) {
        ret_t ret;
        ret.swap(_pending_deltas);
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

std::ostream&
operator<<(std::ostream& o, topic_table::in_progress_state update) {
    switch (update) {
    case topic_table::in_progress_state::update_requested:
        return o << "update_requested";
    case topic_table::in_progress_state::cancel_requested:
        return o << "cancel_requested";
    case topic_table::in_progress_state::force_cancel_requested:
        return o << "force_cancel_requested";
    }
    __builtin_unreachable();
}

} // namespace cluster
