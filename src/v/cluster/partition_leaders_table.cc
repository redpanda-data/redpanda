// Copyright 2020 Redpanda Data, Inc.
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
#include "model/namespace.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future-util.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <optional>

namespace cluster {

partition_leaders_table::partition_leaders_table(
  ss::sharded<topic_table>& topic_table)
  : _topic_table(topic_table) {}

ss::future<> partition_leaders_table::stop() {
    vlog(clusterlog.info, "Stopping Partition Leaders Table...");

    while (!_leader_promises.empty()) {
        auto it = _leader_promises.begin();
        for (auto& promise : it->second) {
            promise.second->set_exception(
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

std::optional<leader_term>
partition_leaders_table::get_leader_term(const model::ntp& ntp) const {
    return get_leader_term(model::topic_namespace_view(ntp), ntp.tp.partition);
}

std::optional<leader_term> partition_leaders_table::get_leader_term(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto meta = find_leader_meta(tp_ns, pid);
    return meta ? std::make_optional<leader_term>(
             meta->current_leader, meta->last_stable_leader_term)
                : std::nullopt;
}

void partition_leaders_table::update_partition_leader(
  const model::ntp& ntp,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    // we set revision_id to invalid, this way we will skip revision check
    update_partition_leader(ntp, model::revision_id{}, term, leader_id);
}

void partition_leaders_table::update_partition_leader(
  const model::ntp& ntp,
  model::revision_id revision_id,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    auto key = leader_key_view{
      model::topic_namespace_view(ntp), ntp.tp.partition};

    const auto is_controller = ntp == model::controller_ntp;
    /**
     * Use revision to differentiate updates for the topic that was
     * deleted from topics table from the one which are processed before the
     * topic is known to node local topic table.
     * Controller is a special case as it is not present in topic table.
     *
     */
    const auto topic_removed
      = revision_id <= _topic_table.local().last_applied_revision()
        && !_topic_table.local().contains(model::topic_namespace_view(ntp));

    if (topic_removed && !is_controller) {
        vlog(
          clusterlog.trace,
          "can't update leadership of the removed topic {}",
          ntp);
        return;
    }

    auto it = _leaders.find(key);
    if (it == _leaders.end()) {
        auto [new_it, _] = _leaders.emplace(
          leader_key{
            model::topic_namespace(ntp.ns, ntp.tp.topic), ntp.tp.partition},
          leader_meta{
            .current_leader = leader_id,
            .update_term = term,
            .partition_revision = revision_id});
        it = new_it;
    } else {
        /**
         * Controller is a special case as it revision never but it
         * configuration revision does. We always update controller
         * leadership with revision 0 not to lose any of the updates.
         *
         * TODO: introduce a feature that will change the behavior for all
         * the partitions and it will use ntp_config revision instead of a
         * revision coming from raft.
         */

        // Currently we have to check if revision id is valid since not all
        // the code paths devlivers revision information
        //
        // TODO: always check revision after we will add revision to
        // metadata dissemination requests
        const bool revision_id_valid = revision_id >= model::revision_id{0};
        if (!is_controller) {
            // skip update for partition with previous revision
            if (
              revision_id_valid
              && revision_id < it->second.partition_revision) {
                vlog(
                  clusterlog.trace,
                  "skip update for partition {} with previous revision {} "
                  "current "
                  "revision {}",
                  ntp,
                  revision_id,
                  it->second.partition_revision);
                return;
            }
            // reset the term for new ntp revision
            if (
              revision_id_valid
              && revision_id > it->second.partition_revision) {
                it->second.update_term = model::term_id{};
            }
        }

        // existing partition
        if (it->second.update_term > term) {
            vlog(
              clusterlog.trace,
              "skip update for partition {} with previous term {} current term "
              "{}",
              ntp,
              term,
              it->second.update_term);
            // Do nothing if update term is older
            return;
        }

        // if current leader has value, store it as a previous leader
        if (it->second.current_leader) {
            it->second.previous_leader = it->second.current_leader;
        }
        it->second.current_leader = leader_id;
        it->second.update_term = term;
        if (revision_id_valid) {
            it->second.partition_revision = revision_id;
        }
    }
    vlog(
      clusterlog.trace,
      "updated partition: {} leader: {{term: {}, current leader: {}, previous "
      "leader: {}, revision: {}}}",
      ntp,
      it->second.update_term,
      it->second.current_leader,
      it->second.previous_leader,
      it->second.partition_revision);
    // notify waiters if update is setting the leader
    if (!leader_id) {
        return;
    }
    // update stable leader term
    it->second.last_stable_leader_term = term;

    if (auto it = _leader_promises.find(ntp); it != _leader_promises.end()) {
        for (auto& promise : it->second) {
            promise.second->set_value(*leader_id);
        }
    }

    // Ensure leadership has changed before notifying watchers
    if (
      !it->second.previous_leader
      || leader_id.value() != it->second.previous_leader.value()) {
        _watchers.notify(ntp, ntp, term, leader_id);
    }
}

ss::future<> partition_leaders_table::update_with_estimates() {
    for (const auto& [ns_tp, topic] :
         _topic_table.local().all_topics_metadata()) {
        if (!topic.is_topic_replicable()) {
            // skip non-replicable topics
            continue;
        }

        for (const auto& part : topic.metadata.get_assignments()) {
            if (!_leaders.contains(leader_key_view{ns_tp, part.id})) {
                model::ntp ntp{ns_tp.ns, ns_tp.tp, part.id};
                vassert(
                  !part.replicas.empty(),
                  "set of replicas for ntp {} can't be empty",
                  ntp);
                update_partition_leader(
                  ntp, model::term_id{1}, part.replicas.begin()->node_id);
            }

            co_await ss::coroutine::maybe_yield();
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
    _leader_promises[ntp].emplace(
      id, std::make_unique<expiring_promise<model::node_id>>());
    auto& promise = _leader_promises[ntp][id];

    return promise
      ->get_future_with_timeout(
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

void partition_leaders_table::remove_leader(
  const model::ntp& ntp, model::revision_id revision) {
    auto it = _leaders.find(
      leader_key_view{model::topic_namespace_view(ntp), ntp.tp.partition});
    // ignore updates with old revision
    if (it != _leaders.end() && it->second.partition_revision <= revision) {
        vlog(
          clusterlog.trace,
          "removing {} with revision {} matched by revision {}",
          ntp,
          it->second.partition_revision,
          revision);
        _leaders.erase(it);
    }
}

void partition_leaders_table::reset() {
    vlog(clusterlog.trace, "resetting leaders");
    _leaders.clear();
}

partition_leaders_table::leaders_info_t
partition_leaders_table::get_leaders() const {
    leaders_info_t ans;
    ans.reserve(_leaders.size());
    for (const auto& leader_info : _leaders) {
        leader_info_t info{
          .tp_ns = leader_info.first.tp_ns,
          .pid = leader_info.first.pid,
          .current_leader = leader_info.second.current_leader,
          .previous_leader = leader_info.second.previous_leader,
          .last_stable_leader_term = leader_info.second.last_stable_leader_term,
          .update_term = leader_info.second.update_term,
          .partition_revision = leader_info.second.partition_revision,
        };
        ans.push_back(std::move(info));
    }
    return ans;
}

notification_id_type
partition_leaders_table::register_leadership_change_notification(
  leader_change_cb_t cb) {
    return _watchers.register_notify(std::move(cb));
}

notification_id_type
partition_leaders_table::register_leadership_change_notification(
  const model::ntp& ntp, leader_change_cb_t cb) {
    return _watchers.register_notify(ntp, std::move(cb));
}

void partition_leaders_table::unregister_leadership_change_notification(
  notification_id_type id) {
    return _watchers.unregister_notify(id);
}

void partition_leaders_table::unregister_leadership_change_notification(
  const model::ntp& ntp, notification_id_type id) {
    return _watchers.unregister_notify(ntp, id);
}

} // namespace cluster
