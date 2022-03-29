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
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

template<typename Func>
std::vector<std::invoke_result_t<Func, topic_configuration_assignment>>
topic_table::transform_topics(Func&& f) const {
    std::vector<std::invoke_result_t<Func, topic_configuration_assignment>> ret;
    ret.reserve(_topics.size());
    std::transform(
      std::cbegin(_topics),
      std::cend(_topics),
      std::back_inserter(ret),
      [f = std::forward<Func>(f)](
        const std::pair<model::topic_namespace, topic_metadata>& p) {
          return f(p.second.configuration);
      });
    return ret;
}

topic_table::topic_metadata::topic_metadata(
  topic_configuration_assignment c, model::revision_id rid) noexcept
  : configuration(std::move(c))
  , _source_topic(std::nullopt)
  , _revision(rid) {}

topic_table::topic_metadata::topic_metadata(
  topic_configuration_assignment c,
  model::revision_id rid,
  model::topic st) noexcept
  : configuration(std::move(c))
  , _source_topic(st)
  , _revision(rid) {}

bool topic_table::topic_metadata::is_topic_replicable() const {
    return _source_topic.has_value() == false;
}

model::revision_id topic_table::topic_metadata::get_revision() const {
    vassert(
      is_topic_replicable(), "Query for revision_id on a non-replicable topic");
    return _revision;
}

const model::topic& topic_table::topic_metadata::get_source_topic() const {
    vassert(
      !is_topic_replicable(), "Query for source_topic on a replicable topic");
    return _source_topic.value();
}

const topic_configuration_assignment&
topic_table::topic_metadata::get_configuration() const {
    return configuration;
}

ss::future<std::error_code>
topic_table::apply(create_topic_cmd cmd, model::offset offset) {
    if (_topics.contains(cmd.key)) {
        // topic already exists
        return ss::make_ready_future<std::error_code>(
          errc::topic_already_exists);
    }
    // calculate delta
    for (auto& pas : cmd.value.assignments) {
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, pas.id);
        _pending_deltas.emplace_back(
          std::move(ntp), pas, offset, delta::op_type::add);
    }

    _topics.insert(
      {cmd.key,
       topic_metadata(std::move(cmd.value), model::revision_id(offset()))});
    notify_waiters();
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

        for (auto& p : tp->second.configuration.assignments) {
            auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p.id);
            _update_in_progress.erase(ntp);
            _pending_deltas.emplace_back(
              std::move(ntp), std::move(p), offset, delete_type);
        }
        _topics.erase(tp);
        notify_waiters();
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
    auto prev_partition_count = tp->second.configuration.cfg.partition_count;
    // update partitions count
    tp->second.configuration.cfg.partition_count
      = cmd.value.cfg.new_total_partition_count;
    // add assignments of newly created partitions
    for (auto& p_as : cmd.value.assignments) {
        p_as.id += model::partition_id(prev_partition_count);
        tp->second.configuration.assignments.push_back(p_as);
        // propagate deltas
        auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p_as.id);
        _pending_deltas.emplace_back(
          std::move(ntp), std::move(p_as), offset, delta::op_type::add);
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
    auto find_partition = [](
                            std::vector<partition_assignment>& assignments,
                            model::partition_id p_id) {
        return std::find_if(
          assignments.begin(),
          assignments.end(),
          [p_id](const partition_assignment& p_as) { return p_id == p_as.id; });
    };

    auto current_assignment_it = find_partition(
      tp->second.configuration.assignments, cmd.key.tp.partition);

    if (current_assignment_it == tp->second.configuration.assignments.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (_update_in_progress.contains(cmd.key)) {
        return ss::make_ready_future<std::error_code>(errc::update_in_progress);
    }

    // assignment is already up to date, this operation is NOP do not propagate
    // delta

    if (are_replica_sets_equal(current_assignment_it->replicas, cmd.value)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    _update_in_progress.insert(cmd.key);
    auto previous_assignment = *current_assignment_it;
    // replace partition replica set
    current_assignment_it->replicas = cmd.value;

    /// Update all non_replicable topics to have the same 'in-progress' state
    auto found = _topics_hierarchy.find(model::topic_namespace_view(cmd.key));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            /// Insert non-replicable topic into the 'update_in_progress' set
            auto [_, success] = _update_in_progress.insert(
              model::ntp(cs.ns, cs.tp, current_assignment_it->id));
            vassert(
              success,
              "non_replicable topic {}-{} already in _update_in_progress set",
              cs.tp,
              current_assignment_it->id);
            /// For each child topic of the to-be-moved source, update its new
            /// replica assignment to reflect the change
            auto sfound = _topics.find(cs);
            vassert(
              sfound != _topics.end(),
              "Non replicable topic must exist: {}",
              cs);
            auto assignment_it = find_partition(
              sfound->second.configuration.assignments,
              current_assignment_it->id);
            vassert(
              assignment_it != sfound->second.configuration.assignments.end(),
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
      previous_assignment);

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
    auto current_assignment_it = std::find_if(
      tp->second.configuration.assignments.begin(),
      tp->second.configuration.assignments.end(),
      [p_id = cmd.key.tp.partition](partition_assignment& p_as) {
          return p_id == p_as.id;
      });

    if (current_assignment_it == tp->second.configuration.assignments.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (current_assignment_it->replicas != cmd.value) {
        return ss::make_ready_future<std::error_code>(
          errc::invalid_node_operation);
    }

    _update_in_progress.erase(cmd.key);

    partition_assignment delta_assignment{
      .group = current_assignment_it->group,
      .id = current_assignment_it->id,
      .replicas = std::move(cmd.value),
    };

    /// Remove child non_replicable topics out of the 'update_in_progress' set
    auto found = _topics_hierarchy.find(model::topic_namespace_view(cmd.key));
    if (found != _topics_hierarchy.end()) {
        for (const auto& cs : found->second) {
            bool erased = _update_in_progress.erase(
                            model::ntp(cs.ns, cs.tp, cmd.key.tp.partition))
                          > 0;
            if (!erased) {
                vlog(
                  clusterlog.error,
                  "non_replicable_topic expected to exist in "
                  "update_in_progress set");
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
    auto& properties = tp->second.configuration.cfg.properties;
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
    deltas.reserve(tp->second.configuration.assignments.size());
    for (const auto& p_as : tp->second.configuration.assignments) {
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

    for (const auto& pas : tp->second.configuration.assignments) {
        _pending_deltas.emplace_back(
          model::ntp(new_non_rep_topic.ns, new_non_rep_topic.tp, pas.id),
          pas,
          o,
          delta::op_type::add_non_replicable);
    }

    auto ca = tp->second.configuration;
    ca.cfg.tp_ns = new_non_rep_topic;

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
    _topics.insert(
      {new_non_rep_topic,
       topic_metadata(std::move(ca), model::revision_id(o()), source.tp)});
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
    return transform_topics(
      [](const topic_configuration_assignment& td) { return td.cfg.tp_ns; });
}

std::optional<model::topic_metadata>
topic_table::get_topic_metadata(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.configuration.get_metadata();
    }
    return {};
}

std::optional<topic_configuration>
topic_table::get_topic_cfg(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.configuration.cfg;
    }
    return {};
}

std::optional<std::vector<partition_assignment>>
topic_table::get_topic_assignments(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.configuration.assignments;
    }
    return {};
}

std::optional<model::timestamp_type>
topic_table::get_topic_timestamp_type(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.configuration.cfg.properties.timestamp_type;
    }
    return {};
}

std::vector<model::topic_metadata> topic_table::all_topics_metadata() const {
    return transform_topics([](const topic_configuration_assignment& td) {
        return td.get_metadata();
    });
}

bool topic_table::contains(
  model::topic_namespace_view topic, model::partition_id pid) const {
    if (auto it = _topics.find(topic); it != _topics.end()) {
        const auto& partitions = it->second.configuration.assignments;
        return std::any_of(
          partitions.cbegin(),
          partitions.cend(),
          [&pid](const partition_assignment& pas) { return pas.id == pid; });
    }
    return false;
}

std::optional<cluster::partition_assignment>
topic_table::get_partition_assignment(const model::ntp& ntp) const {
    auto it = _topics.find(model::topic_namespace_view(ntp));
    if (it == _topics.end()) {
        return {};
    }

    auto p_it = std::find_if(
      it->second.configuration.assignments.cbegin(),
      it->second.configuration.assignments.cend(),
      [&ntp](const partition_assignment& pas) {
          return pas.id == ntp.tp.partition;
      });

    if (p_it == it->second.configuration.assignments.cend()) {
        return {};
    }

    return *p_it;
}

bool topic_table::is_update_in_progress(const model::ntp& ntp) const {
    return _update_in_progress.contains(ntp);
}

std::optional<model::initial_revision_id>
topic_table::get_initial_revision(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return model::initial_revision_id(it->second.get_revision()());
    }
    return std::nullopt;
}

std::optional<model::initial_revision_id>
topic_table::get_initial_revision(const model::ntp& ntp) const {
    return get_initial_revision(model::topic_namespace_view(ntp));
}

} // namespace cluster
