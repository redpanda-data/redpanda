// Copyright 2020 Vectorized, Inc.
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
  , _id_or_topic(topic_metadata::normal_topic_meta{.rev = rid}) {}

topic_table::topic_metadata::topic_metadata(
  topic_configuration_assignment c, model::topic st) noexcept
  : configuration(std::move(c))
  , _id_or_topic(std::move(st)) {}

bool topic_table::topic_metadata::is_topic_replicable() const {
    return std::holds_alternative<model::topic>(_id_or_topic);
}
model::revision_id topic_table::topic_metadata::get_revision() const {
    vassert(
      !is_topic_replicable(), "Query for revision_id on a replicable topic");
    return std::get<normal_topic_meta>(_id_or_topic).rev;
}
const absl::flat_hash_set<model::topic_namespace>&
topic_table::topic_metadata::get_children() const {
    vassert(!is_topic_replicable(), "Query for children on a replicable topic");
    return std::get<normal_topic_meta>(_id_or_topic).children;
}
absl::flat_hash_set<model::topic_namespace>&
topic_table::topic_metadata::get_children() {
    vassert(!is_topic_replicable(), "Query for children on a replicable topic");
    return std::get<normal_topic_meta>(_id_or_topic).children;
}
const model::topic& topic_table::topic_metadata::get_source_topic() const {
    vassert(is_topic_replicable(), "Query for source_topic on a normal topic");
    return std::get<model::topic>(_id_or_topic);
}
const topic_configuration_assignment&
topic_table::topic_metadata::get_configuration() const {
    vassert(
      !is_topic_replicable(), "Query for configuration on a replicable topic");
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
    if (auto tp = _topics.find(cmd.value); tp != _topics.end()) {
        if (tp->second.is_topic_replicable()) {
            return ss::make_ready_future<std::error_code>(
              errc::invalid_delete_topic_request);
        }
        for (auto& p : tp->second.configuration.assignments) {
            auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, p.id);
            _pending_deltas.emplace_back(
              std::move(ntp), std::move(p), offset, delta::op_type::del);
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
    if (tp == _topics.end() || tp->second.is_topic_replicable()) {
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
    if (tp == _topics.end() || tp->second.is_topic_replicable()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }

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
    if (tp == _topics.end() || tp->second.is_topic_replicable()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
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
    if (tp == _topics.end() || tp->second.is_topic_replicable()) {
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

    for (const auto& pas : tp->second.configuration.assignments) {
        _pending_deltas.emplace_back(
          model::ntp(new_non_rep_topic.ns, new_non_rep_topic.tp, pas.id),
          pas,
          o,
          delta::op_type::add_non_replicable);
    }

    auto ca = tp->second.configuration;
    ca.cfg.tp_ns = new_non_rep_topic;
    for (auto& assignment : ca.assignments) {
        assignment.group = raft::group_id(-1);
    }
    _topics.insert(
      {new_non_rep_topic, topic_metadata(std::move(ca), source.tp)});
    auto& children = tp->second.get_children();
    auto h = children.find(source);
    if (h == children.end()) {
        children.insert(source, {new_non_rep_topic});
    } else {
        children.insert(new_non_rep_topic);
    }
    notify_waiters();
    co_return make_error_code(errc::success);
}

void topic_table::notify_waiters() {
    if (_waiters.empty()) {
        return;
    }
    std::vector<delta> changes;
    changes.swap(_pending_deltas);
    for (auto& cb : _notifications) {
        cb.second(changes);
    }
    std::vector<std::unique_ptr<waiter>> active_waiters;
    active_waiters.swap(_waiters);
    for (auto& w : active_waiters) {
        w->promise.set_value(changes);
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
    if (it == _topics.end() || it->second.is_topic_replicable()) {
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

} // namespace cluster
