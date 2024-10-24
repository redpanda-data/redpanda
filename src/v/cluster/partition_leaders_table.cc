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
#include "ssx/async_algorithm.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <absl/container/btree_map.h>

#include <optional>

namespace cluster {

partition_leaders_table::partition_leaders_table(
  ss::sharded<topic_table>& topic_table)
  : _topic_table(topic_table) {}

ss::future<> partition_leaders_table::stop() {
    _as.request_abort();
    return _gate.close();
}

std::optional<
  std::reference_wrapper<const partition_leaders_table::leader_meta>>
partition_leaders_table::find_leader_meta(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    if (auto t_it = _topic_leaders.find(tp_ns); t_it != _topic_leaders.end()) {
        if (auto p_it = t_it->second.find(pid); p_it != t_it->second.end()) {
            return std::cref(p_it->second);
        }
    }
    return std::nullopt;
}

std::optional<model::node_id> partition_leaders_table::get_previous_leader(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto meta = find_leader_meta(tp_ns, pid);
    return meta ? meta->get().previous_leader : std::nullopt;
}

std::optional<model::node_id> partition_leaders_table::get_leader(
  model::topic_namespace_view tp_ns, model::partition_id pid) const {
    const auto meta = find_leader_meta(tp_ns, pid);
    return meta ? meta->get().current_leader : std::nullopt;
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
                    meta->get().current_leader,
                    meta->get().last_stable_leader_term)
                : std::nullopt;
}

void partition_leaders_table::update_partition_leader(
  const model::ntp& ntp,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    // we set revision_id to invalid, this way we will skip revision check
    update_partition_leader(ntp, model::revision_id{}, term, leader_id);
}

ss::future<> partition_leaders_table::update_with_node_report(
  const node_health_report_ptr& node_report) {
    ssx::async_counter counter;
    for (const auto& [tp_ns, partitions] : node_report->topics) {
        /**
         * Here we minimize the number of topic table and topic map lookups by
         * doing it only once for each topic.
         **/
        version topic_map_version_snapshot = _topic_map_version;
        auto [t_it, _] = _topic_leaders.try_emplace(tp_ns);

        const bool is_controller = tp_ns == model::controller_nt;
        bool present_in_table = _topic_table.local().contains(tp_ns);
        auto last_applied_revision
          = _topic_table.local().last_applied_revision();
        co_await ssx::async_for_each_counter(
          counter,
          partitions.begin(),
          partitions.end(),
          [&](const partition_status& p) {
              if (!p.leader_id.has_value()) {
                  return;
              }

              const auto topic_removed = p.revision_id <= last_applied_revision
                                         && !present_in_table;

              if (topic_removed && !is_controller) {
                  vlog(
                    clusterlog.trace,
                    "can't update leadership of the removed topic {}",
                    tp_ns);
                  return;
              }
              /**
               * Check before accessing topic iterator as we might yield during
               * previous iteration.
               */
              if (topic_map_version_snapshot != _topic_map_version) {
                  auto result = _topic_leaders.try_emplace(tp_ns);
                  t_it = result.first;
                  present_in_table = _topic_table.local().contains(tp_ns);
                  last_applied_revision
                    = _topic_table.local().last_applied_revision();
                  topic_map_version_snapshot = _topic_map_version;
              }

              do_update_partition_leader(
                is_controller, t_it, p.id, p.revision_id, p.term, p.leader_id);
          });
    }
}

void partition_leaders_table::do_update_partition_leader(
  bool is_controller,
  topics_t::iterator t_it,
  model::partition_id p_id,
  model::revision_id revision_id,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    auto [p_it, new_entry] = t_it->second.emplace(
      p_id,
      leader_meta{
        .current_leader = leader_id,
        .update_term = term,
        .partition_revision = revision_id});

    if (!new_entry) [[likely]] {
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
              && revision_id < p_it->second.partition_revision) {
                vlog(
                  clusterlog.trace,
                  "skip update for partition {}/{} with previous revision {} "
                  "current revision {}",
                  t_it->first.ns,
                  t_it->first.tp,
                  p_id,
                  revision_id,
                  p_it->second.partition_revision);
                return;
            }
            // reset the term for new ntp revision
            if (
              revision_id_valid
              && revision_id > p_it->second.partition_revision) {
                p_it->second.update_term = model::term_id{};
                p_it->second.last_stable_leader_term = model::term_id{};
            }
        }

        if (p_it->second.update_term > term) {
            vlog(
              clusterlog.trace,
              "skip update for partition {}/{}/{} with previous term {} "
              "current term {}",
              t_it->first.ns,
              t_it->first.tp,
              p_id,
              term,
              p_it->second.update_term);
            // Do nothing if update term is older
            return;
        }
        /**
         * Update leader less partition counter when leadership changed
         *
         * no leader -> leader - decrement leaderless counter
         * leader -> no leader - increment leaderless counter
         */
        if (p_it->second.current_leader && !leader_id) {
            ++_leaderless_partition_count;
        } else if (!p_it->second.current_leader && leader_id) {
            --_leaderless_partition_count;
        }

        // if current leader has value, store it as a previous leader
        if (p_it->second.current_leader) {
            p_it->second.previous_leader = p_it->second.current_leader;
        }
        p_it->second.current_leader = leader_id;
        p_it->second.update_term = term;
        if (revision_id_valid) {
            p_it->second.partition_revision = revision_id;
        }
    } else {
        if (!leader_id) {
            ++_leaderless_partition_count;
        }
        /**
         * We only increment version if any of the maps content was modified
         */
        ++_version;
    }

    vlog(
      clusterlog.trace,
      "updated partition: {}/{}/{} leader: {{term: {}, current leader: {}, "
      "previous leader: {}, revision: {}}}",
      t_it->first.ns,
      t_it->first.tp,
      p_id,
      p_it->second.update_term,
      p_it->second.current_leader,
      p_it->second.previous_leader,
      p_it->second.partition_revision);
    // notify waiters if update is setting the leader
    if (!leader_id) {
        return;
    }
    // update stable leader term
    const auto needs_notification = term > p_it->second.last_stable_leader_term;
    p_it->second.last_stable_leader_term = term;

    if (needs_notification) {
        _watchers.notify(
          t_it->first.ns,
          t_it->first.tp,
          p_id,
          model::ntp(t_it->first.ns, t_it->first.tp, p_id),
          term,
          *leader_id);
    }
}

void partition_leaders_table::update_partition_leader(
  const model::ntp& ntp,
  model::revision_id revision_id,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
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

    auto [t_it, new_topic_entry] = _topic_leaders.try_emplace(
      model::topic_namespace_view(ntp));
    if (new_topic_entry) {
        ++_topic_map_version;
    }

    do_update_partition_leader(
      is_controller, t_it, ntp.tp.partition, revision_id, term, leader_id);
}

ss::future<model::node_id> partition_leaders_table::wait_for_leader(
  const model::ntp& ntp,
  ss::lowres_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (auto leader = get_leader(ntp); leader.has_value()) {
        return ss::make_ready_future<model::node_id>(*leader);
    }
    auto holder = _gate.hold();
    auto promise = ss::make_lw_shared<expiring_promise<model::node_id>>();
    auto n_id = register_leadership_change_notification(
      ntp, [promise](model::ntp, model::term_id, model::node_id leader_id) {
          promise->set_value(leader_id);
      });

    auto f = promise->get_future_with_timeout(
      timeout,
      [] { return std::make_exception_ptr(ss::timed_out_error()); },
      as.value_or(_as));

    return f.finally(
      [this, promise = std::move(promise), n_id, h = std::move(holder)] {
          unregister_leadership_change_notification(n_id);
      });
}

void partition_leaders_table::remove_leader(
  const model::ntp& ntp, model::revision_id revision) {
    auto t_it = _topic_leaders.find(model::topic_namespace_view(ntp));
    if (t_it == _topic_leaders.end()) {
        return;
    }
    auto p_it = t_it->second.find(ntp.tp.partition);
    if (p_it == t_it->second.end()) {
        return;
    }

    // ignore updates with old revision
    if (p_it->second.partition_revision <= revision) {
        vlog(
          clusterlog.trace,
          "removing {} with revision {} matched by revision {}",
          ntp,
          p_it->second.partition_revision,
          revision);

        if (!p_it->second.current_leader.has_value()) {
            _leaderless_partition_count--;
        }

        t_it->second.erase(p_it);
        if (t_it->second.empty()) {
            _topic_leaders.erase(t_it);
            ++_topic_map_version;
        }
        ++_version;
    }
}

void partition_leaders_table::reset() {
    vlog(clusterlog.trace, "resetting leaders");
    _topic_leaders.clear();
    _leaderless_partition_count = 0;
    ++_version;
    ++_topic_map_version;
}

ss::future<partition_leaders_table::leaders_info_t>
partition_leaders_table::get_leaders() const {
    leaders_info_t ans;
    ans.reserve(_topic_leaders.size());
    auto version_snapshot = _version;
    ssx::async_counter counter;
    for (auto& [tp_ns, partition_leaders] : _topic_leaders) {
        co_await ssx::async_for_each_counter(
          counter,
          partition_leaders.begin(),
          partition_leaders.end(),
          [this, &tp_ns, &ans, version_snapshot](
            const partition_leaders::value_type& p) mutable {
              const auto& [p_id, leader_info] = p;
              /**
               * Modification validation must happen before accessing the
               * element as previous iteration might have yield
               */
              throw_if_modified(version_snapshot);
              leader_info_t info{
                .tp_ns = tp_ns,
                .pid = model::partition_id(p_id),
                .current_leader = leader_info.current_leader,
                .previous_leader = leader_info.previous_leader,
                .last_stable_leader_term = leader_info.last_stable_leader_term,
                .update_term = leader_info.update_term,
                .partition_revision = leader_info.partition_revision,
              };
              ans.push_back(std::move(info));
          });
        throw_if_modified(version_snapshot);
    }
    co_return ans;
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
