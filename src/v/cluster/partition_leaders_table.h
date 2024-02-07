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
#include "cluster/ntp_callbacks.h"
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/expiring_promise.h"

#include <seastar/core/sharded.hh>

#include <absl/container/btree_map.h>
#include <absl/container/node_hash_map.h>

#include <optional>

namespace cluster {

/// Partition leaders contains information about currently elected partition
/// leaders. It allows user to wait for ntp leader to be elected. Partition
/// leaders are instanciated on each core i.e. each core contains copy op
/// partition leaders. Partition leaders are updated through notification
/// received by cluster::metadata_dissemination_service.
class partition_leaders_table {
public:
    explicit partition_leaders_table(ss::sharded<topic_table>&);

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
    void for_each_leader(Func&& f) const {
        for (auto& [tp_ns, partition_leaders] : _topic_leaders) {
            for (auto& [p_id, leader_info] : partition_leaders) {
                f(tp_ns,
                  p_id,
                  leader_info.current_leader,
                  leader_info.update_term);
            }
        }
    }

    void remove_leader(const model::ntp&, model::revision_id);

    void reset();

    void update_partition_leader(
      const model::ntp&, model::term_id, std::optional<model::node_id>);

    void update_partition_leader(
      const model::ntp&,
      model::revision_id,
      model::term_id,
      std::optional<model::node_id>);

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

    leaders_info_t get_leaders() const;

    using leader_change_cb_t = ss::noncopyable_function<void(
      model::ntp, model::term_id, std::optional<model::node_id>)>;

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

    using partition_leaders = absl::btree_map<model::partition_id, leader_meta>;
    using topics_t = absl::node_hash_map<
      model::topic_namespace,
      partition_leaders,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    std::optional<std::reference_wrapper<const leader_meta>>
      find_leader_meta(model::topic_namespace_view, model::partition_id) const;

    topics_t _topic_leaders;

    // per-ntp notifications for leadership election. note that the
    // namespace is currently ignored pending an update to the metadata
    // cache that attaches a namespace to all topics partition references.
    int32_t _promise_id = 0;
    using promises_t = absl::node_hash_map<
      model::ntp,
      absl::node_hash_map<
        int32_t,
        std::unique_ptr<expiring_promise<model::node_id>>>>;

    promises_t _leader_promises;

    ss::sharded<topic_table>& _topic_table;

    ntp_callbacks<leader_change_cb_t> _watchers;
};

} // namespace cluster
