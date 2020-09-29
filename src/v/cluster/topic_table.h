/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "utils/expiring_promise.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/// Topic table represent all topics configuration and partition assignments.
/// The topic table is copied on each core to minimize cross core communication
/// when requesting topic information. The topics table provides an API for
/// Kafka requests and delta API for controller backend. The delta API allows
/// backend to wait for changes in topics table. Topics table is update directly
/// from controller_stm. The table is always updated before any actions related
/// with topic creation or deletion are executed. Topic table is also
/// responsible for commiting or removing pending allocations
///
class topic_table {
public:
    // delta propagated to backend
    struct delta {
        enum class op_type { add, del, update };

        delta(
          model::ntp, cluster::partition_assignment, model::offset, op_type);
        model::ntp ntp;
        cluster::partition_assignment p_as;
        model::offset offset;
        op_type type;

        model::topic_namespace_view tp_ns() const {
            return model::topic_namespace_view(ntp);
        }

        friend std::ostream& operator<<(std::ostream&, const delta&);
        friend std::ostream& operator<<(std::ostream&, const op_type&);
    };

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type == topic_batch_type;
    }

    // list of commands that this table is able to apply, the list is used to
    // automatically deserialize batch into command
    static constexpr auto accepted_commands = make_commands_list<
      create_topic_cmd,
      delete_topic_cmd,
      move_partition_replicas_cmd>{};

    /// State machine applies
    ss::future<std::error_code> apply(create_topic_cmd, model::offset);
    ss::future<std::error_code> apply(delete_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(move_partition_replicas_cmd, model::offset);

    ss::future<> stop();

    /// Delta API

    ss::future<std::vector<delta>> wait_for_changes(ss::abort_source&);

    bool has_pending_changes() const { return !_pending_deltas.empty(); }

    /// Query API

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    /// Returns metadata of all topics.
    std::vector<model::topic_metadata> all_topics_metadata() const;

    /// Checks if it has given partition
    bool contains(model::topic_namespace_view, model::partition_id) const;

    /// Returns partition leader
    std::optional<model::node_id> get_leader(const model::ntp&) const;

    /// Updates partition leader and notify waiters if needed
    void update_partition_leader(
      const model::ntp&, model::term_id, std::optional<model::node_id>);

    std::optional<partition_assignment>
    get_partition_assignment(const model::ntp&) const;

private:
    struct waiter {
        explicit waiter(uint64_t id)
          : id(id) {}
        ss::promise<std::vector<delta>> promise;
        ss::abort_source::subscription sub;
        uint64_t id;
    };
    void deallocate_topic_partitions(const std::vector<partition_assignment>&);

    void notify_waiters();

    template<typename Func>
    std::vector<std::invoke_result_t<Func, topic_configuration_assignment>>
    transform_topics(Func&&) const;

    absl::flat_hash_map<
      model::topic_namespace,
      topic_configuration_assignment,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _topics;

    std::vector<delta> _pending_deltas;
    std::vector<std::unique_ptr<waiter>> _waiters;
    uint64_t _waiter_id{0};
};
} // namespace cluster
