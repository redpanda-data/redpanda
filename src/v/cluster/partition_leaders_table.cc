// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_leaders_table.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future-util.hh>

#include <optional>

namespace cluster {

partition_leaders_table::partition_leaders_table(
  ss::sharded<topic_table>& topic_table)
  : _topic_table(topic_table) {}

ss::future<> partition_leaders_table::stop() {
    while (!_leader_promises.empty()) {
        auto it = _leader_promises.begin();
        for (auto& promise : it->second) {
            promise.second.set_exception(
              std::make_exception_ptr(ss::timed_out_error()));
        }
        _leader_promises.erase(it);
    }
    return ss::now();
}

std::optional<partition_leaders_table::leader_meta>
partition_leaders_table::find_leader_meta(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto& topics_map = _topic_table.local().topics_map();
    if (auto it = _leaders.find(leader_key_view{tp_ns, pid});
        it != _leaders.end()) {
        return it->second;
    } else if (auto it = topics_map.find(tp_ns); it != topics_map.end()) {
        // Possible leadership query for materialized topic, search for it
        // in the topics table.
        if (!it->second.is_topic_replicable()) {
            // Leadership properties of non replicated topic are that of its
            // parent
            return find_leader_meta(
              model::topic_namespace_view{
                tp_ns.ns, it->second.get_source_topic()},
              pid);
        }
    }
    return std::nullopt;
}

std::optional<model::node_id> partition_leaders_table::get_previous_leader(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto meta = find_leader_meta(tp_ns, pid);
    return meta ? meta->previous_leader : std::nullopt;
}

std::optional<model::node_id> partition_leaders_table::get_leader(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto meta = find_leader_meta(tp_ns, pid);
    return meta ? meta->current_leader : std::nullopt;
}

std::optional<model::node_id>
partition_leaders_table::get_leader(const model::ntp& ntp) const {
    return get_leader(model::topic_namespace_view(ntp), ntp.tp.partition);
}

void partition_leaders_table::update_partition_leader(
  const model::ntp& ntp,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    auto key = leader_key_view{
      model::topic_namespace_view(ntp), ntp.tp.partition};
    auto it = _leaders.find(key);
    if (it == _leaders.end()) {
        auto [new_it, _] = _leaders.emplace(
          leader_key{
            model::topic_namespace(ntp.ns, ntp.tp.topic), ntp.tp.partition},
          leader_meta{.current_leader = leader_id, .update_term = term});
        it = new_it;
    } else {
        // existing partition
        if (it->second.update_term > term) {
            // Do nothing if update term is older
            return;
        }
        // if current leader has value, store it as a previous leader
        if (it->second.current_leader) {
            it->second.previous_leader = it->second.current_leader;
        }
        it->second.current_leader = leader_id;
        it->second.update_term = term;
    }
    vlog(
      clusterlog.trace,
      "updated partition: {} leader: {{term: {}, current leader: {}, previous "
      "leader: {}}}",
      ntp,
      it->second.update_term,
      it->second.current_leader,
      it->second.previous_leader);
    // notify waiters if update is setting the leader
    if (!leader_id) {
        return;
    }

    if (auto it = _leader_promises.find(ntp); it != _leader_promises.end()) {
        for (auto& promise : it->second) {
            promise.second.set_value(*leader_id);
        }
    }
}

ss::future<model::node_id> partition_leaders_table::wait_for_leader(
  const model::ntp& ntp,
  ss::lowres_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (auto leader = get_leader(ntp); leader.has_value()) {
        return ss::make_ready_future<model::node_id>(*leader);
    }
    auto id = _promise_id++;
    _leader_promises[ntp].emplace(id, expiring_promise<model::node_id>{});
    auto& promise = _leader_promises[ntp][id];

    return promise
      .get_future_with_timeout(
        timeout,
        [] { return std::make_exception_ptr(ss::timed_out_error()); },
        as)
      .then_wrapped([id, ntp, this](ss::future<model::node_id> leader) {
          auto& ntp_promises = _leader_promises[ntp];
          ntp_promises.erase(id);
          if (ntp_promises.empty()) {
              _leader_promises.erase(ntp);
          }

          return leader;
      });
}

} // namespace cluster
