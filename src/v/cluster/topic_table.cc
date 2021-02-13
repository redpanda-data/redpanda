// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_table.h"

#include "cluster/commands.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

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
        const std::pair<model::topic_namespace, topic_configuration_assignment>&
          p) { return f(p.second); });
    return ret;
}

topic_table_delta::topic_table_delta(
  model::ntp ntp,
  cluster::partition_assignment p_as,
  model::offset o,
  op_type tp)
  : ntp(std::move(ntp))
  , p_as(std::move(p_as))
  , offset(o)
  , type(tp) {}

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

    _topics.insert({cmd.key, std::move(cmd.value)});
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
        for (auto& p : tp->second.assignments) {
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
topic_table::apply(move_partition_replicas_cmd cmd, model::offset o) {
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }

    auto current_assignment_it = std::find_if(
      tp->second.assignments.begin(),
      tp->second.assignments.end(),
      [p_id = cmd.key.tp.partition](partition_assignment& p_as) {
          return p_id == p_as.id;
      });

    if (current_assignment_it == tp->second.assignments.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }

    if (_update_in_progress.contains(cmd.key)) {
        return ss::make_ready_future<std::error_code>(errc::update_in_progress);
    }

    _update_in_progress.insert(cmd.key);
    // replace partition replica set
    current_assignment_it->replicas = cmd.value;

    // calculate deleta for backend
    model::ntp ntp(tp->first.ns, tp->first.tp, current_assignment_it->id);
    _pending_deltas.emplace_back(
      std::move(ntp), *current_assignment_it, o, delta::op_type::update);

    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
}

ss::future<std::error_code>
topic_table::apply(finish_moving_partition_replicas_cmd cmd, model::offset o) {
    auto tp = _topics.find(model::topic_namespace_view(cmd.key));
    if (tp == _topics.end()) {
        return ss::make_ready_future<std::error_code>(errc::topic_not_exists);
    }
    _update_in_progress.erase(cmd.key);
    // calculate deleta for backend
    auto current_assignment_it = std::find_if(
      tp->second.assignments.begin(),
      tp->second.assignments.end(),
      [p_id = cmd.key.tp.partition](partition_assignment& p_as) {
          return p_id == p_as.id;
      });

    if (current_assignment_it == tp->second.assignments.end()) {
        return ss::make_ready_future<std::error_code>(
          errc::partition_not_exists);
    }
    // notify backend about finished update
    _pending_deltas.emplace_back(
      std::move(cmd.key),
      *current_assignment_it,
      o,
      delta::op_type::update_finished);

    notify_waiters();

    return ss::make_ready_future<std::error_code>(errc::success);
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
        return it->second.get_metadata();
    }
    return {};
}

std::optional<topic_configuration>
topic_table::get_topic_cfg(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.cfg;
    }
    return {};
}

std::optional<model::timestamp_type>
topic_table::get_topic_timestamp_type(model::topic_namespace_view tp) const {
    if (auto it = _topics.find(tp); it != _topics.end()) {
        return it->second.cfg.timestamp_type;
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
        const auto& partitions = it->second.assignments;
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
      it->second.assignments.cbegin(),
      it->second.assignments.cend(),
      [&ntp](const partition_assignment& pas) {
          return pas.id == ntp.tp.partition;
      });

    if (p_it == it->second.assignments.cend()) {
        return {};
    }

    return *p_it;
}

std::ostream&
operator<<(std::ostream& o, const topic_table::delta::op_type& tp) {
    switch (tp) {
    case topic_table::delta::op_type::add:
        return o << "addition";
    case topic_table::delta::op_type::del:
        return o << "deletion";
    case topic_table::delta::op_type::update:
        return o << "update";
    case topic_table::delta::op_type::update_finished:
        return o << "update_finished";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const topic_table::delta& d) {
    fmt::print(
      o,
      "{{type: {}, ntp: {}, offset: {}, assignment: {}}}",
      d.type,
      d.ntp,
      d.offset,
      d.p_as);

    return o;
}

} // namespace cluster
