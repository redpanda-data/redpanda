/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/ntp_callbacks.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "container/contiguous_range_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/async_algorithm.h"
#include "ssx/mutex.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/later.hh>

namespace cluster {

/// Partition leaders contains information about currently elected partition
/// leaders. It allows user to wait for ntp leader to be elected. Partition
/// leaders are instanciated on each core i.e. each core contains copy op
/// partition leaders. Partition leaders are updated through notification
/// received by cluster::metadata_dissemination_service.
class partition_leaders_table {
private:
    using version = named_type<uint64_t, struct plt_version_tag>;

public:
    explicit partition_leaders_table(
      ss::sharded<topic_table>&, ss::sharded<ss::abort_source>&);

    ss::future<> stop();

    std::optional<model::node_id> get_leader(const model::ntp&) const;

    std::optional<model::node_id>
      get_leader(model::topic_namespace_view, model::partition_id) const;

    std::optional<leader_term> get_leader_term(const model::ntp&) const;

    std::optional<leader_term>
      get_leader_term(model::topic_namespace_view, model::partition_id) const;

    /**
     * Returns previous reader of partition if available. This is required by
     * Kafka metadata APIs since it require us to return former leader id even
     * it the leader is not present in a given time point
     */
    std::optional<model::node_id> get_previous_leader(
      model::topic_namespace_view, model::partition_id) const;

    ss::future<model::node_id> wait_for_leader(
      const model::ntp&,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    template<typename Func>
    requires requires(
      Func f,
      model::topic_namespace_view tp_ns,
      model::partition_id pid,
      std::optional<model::node_id> leader,
      model::term_id term) {
        { f(tp_ns, pid, leader, term) } -> std::same_as<void>;
    }
    ss::future<> for_each_leader(const Func& f) {
        auto holder = _gate.hold();
        auto u = co_await _mutex.get_units();
        ssx::async_counter counter;
        for (auto& [tp_ns, partition_leaders] : _topic_leaders) {
            co_await ssx::async_for_each_counter(
              counter,
              partition_leaders.begin(),
              partition_leaders.end(),
              [&tp_ns, &f](const partition_leaders::value_type& p) {
                  f(tp_ns,
                    model::partition_id(p.first),
                    p.second.current_leader,
                    p.second.update_term);
              });
        }
    }

    ss::future<> remove_leader(const model::ntp&, model::revision_id);

    ss::future<> reset();

    ss::future<> update_partition_leader(
      const model::ntp&, model::term_id, std::optional<model::node_id>);

    ss::future<> update_partition_leader(
      const model::ntp&,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);
    /**
     * This method updates leadership metadata in a a way that is optimized to
     * leverage the hierarchical structure of node health report.
     *
     * IMPORTANT: node_report must be kept alive during the execution of this
     * method
     */
    ss::future<>
    update_with_node_report(const node_health_report_ptr& node_report);

    struct leader_info_t {
        model::topic_namespace tp_ns;
        model::partition_id pid;

        std::optional<model::node_id> current_leader;
        std::optional<model::node_id> previous_leader;
        model::term_id last_stable_leader_term;
        model::term_id update_term;
        model::revision_id partition_revision;
    };

    using leaders_info_t = chunked_vector<leader_info_t>;

    /**
     * Get a snapshot of all the current leaders.
     *
     * @throws if the set of leaders changes during iteration.
     */
    ss::future<leaders_info_t> get_leaders();

    uint64_t leaderless_partition_count() const {
        return _leaderless_partition_count;
    }

    using leader_change_cb_t = ss::noncopyable_function<void(
      const model::ntp&, model::term_id, model::node_id)>;

    // Register a callback for all leadership changes
    notification_id_type
      register_leadership_change_notification(leader_change_cb_t);

    // Register a callback for a change in leadership for a specific ntp.
    notification_id_type register_leadership_change_notification(
      const model::ntp&, leader_change_cb_t);

    void unregister_leadership_change_notification(notification_id_type);

    void unregister_leadership_change_notification(
      const model::ntp&, notification_id_type);

private:
    // in order to filter out reordered requests we store last update term
    struct leader_meta {
        // current leader id, this may be empty if a group is in the middle of
        // leader election
        std::optional<model::node_id> current_leader;
        // previous leader id, this is empty if and only if there were no leader
        // elected for the topic before
        std::optional<model::node_id> previous_leader;
        /**
         * We keep a term id of last stable leader, term may be increasing even
         * if leader election was unsuccessful, here we store a term of last
         * successfully elected leader
         */
        model::term_id last_stable_leader_term;
        model::term_id update_term;
        model::revision_id partition_revision;
    };

    using partition_leaders
      = contiguous_range_map<model::partition_id::type, leader_meta>;
    using topics_t = chunked_hash_map<
      model::topic_namespace,
      partition_leaders,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    std::optional<std::reference_wrapper<const leader_meta>>
      find_leader_meta(model::topic_namespace_view, model::partition_id) const;

    void do_update_partition_leader(
      bool is_controller,
      topics_t::iterator,
      model::partition_id,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);

    topics_t _topic_leaders;

    uint64_t _leaderless_partition_count{0};

    ss::sharded<topic_table>& _topic_table;

    ntp_callbacks<leader_change_cb_t> _watchers;
    /**
     * Store version to check for concurrent updates
     */
    version _topic_map_version{0};
    ssx::mutex _mutex{"leaders_table/state"};
    ss::gate _gate;
    ss::abort_source& _as;
};

} // namespace cluster
