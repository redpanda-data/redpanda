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
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

// The topic updates dispatcher is responsible for receiving update_apply
// upcalls from controller state machine and propagating updates to topic state
// core local copies. The dispatcher also handles partition_allocator and
// partition_balancer_state updates. Those services exist only on core 0 hence
// the updates have to be executed at the same core.
//
//
//                                  +----------------+        +------------+
//                              +-->| Table@core_0   |---+    |            |
//                              |   +----------------+   |    |  Allocator |
//                              |                        |    |            |
//                              |   +----------------+   |    +------------+
//                              +-->| Table@core_0   |---+          ^
//    on core 0                 |   +----------------+   |          |
//   +-----+   +------------+   |                        |    +------------+
//   |     |   |            |   |           .            |    |            |
//   | STM +-->+ Dispatcher +-->+           .            +--->+ Dispatcher +--->
//   |     |   |            |   |           .            |    |            |
//   +-----+   +------------+   |                        |    +------------+
//                              |   +----------------+   |   on core 0
//                              +-->| Table@core n-1 |---+
//                              |   +----------------+   |
//                              |                        |
//                              |   +----------------+   |
//                              +-->| Table@core #n  |---+
//                                  +----------------+
//
class topic_updates_dispatcher {
public:
    topic_updates_dispatcher(
      ss::sharded<partition_allocator>&,
      ss::sharded<topic_table>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<partition_balancer_state>&);

    ss::future<std::error_code> apply_update(model::record_batch);
    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    static constexpr auto commands = make_commands_list<
      create_topic_cmd,
      delete_topic_cmd,
      topic_lifecycle_transition_cmd,
      move_partition_replicas_cmd,
      finish_moving_partition_replicas_cmd,
      update_topic_properties_cmd,
      create_partition_cmd,
      cancel_moving_partition_replicas_cmd,
      move_topic_replicas_cmd,
      revert_cancel_partition_move_cmd,
      force_partition_reconfiguration_cmd,
      update_partition_replicas_cmd,
      set_topic_partitions_disabled_cmd,
      bulk_force_reconfiguration_cmd>();

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type
               == model::record_batch_type::topic_management_cmd;
    }

private:
    using in_progress_map = absl::
      node_hash_map<model::partition_id, std::vector<model::broker_shard>>;
    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(Cmd, model::offset);

    ss::future<std::error_code> apply(create_topic_cmd, model::offset);
    ss::future<std::error_code> apply(delete_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(topic_lifecycle_transition_cmd, model::offset);
    ss::future<std::error_code>
      apply(move_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(finish_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_topic_properties_cmd, model::offset);
    ss::future<std::error_code> apply(create_partition_cmd, model::offset);
    ss::future<std::error_code>
      apply(cancel_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code> apply(move_topic_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(revert_cancel_partition_move_cmd, model::offset);
    ss::future<std::error_code>
      apply(force_partition_reconfiguration_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(set_topic_partitions_disabled_cmd, model::offset);
    ss::future<std::error_code>
      apply(bulk_force_reconfiguration_cmd, model::offset);

    using ntp_leader = std::pair<model::ntp, model::node_id>;

    template<typename T>
    void add_allocations_for_new_partitions(const T&);

    void update_allocations_for_reconfiguration(
      const std::vector<model::broker_shard>& previous,
      const std::vector<model::broker_shard>& target);

    void deallocate_topic(
      const model::topic_namespace&,
      const assignments_set&,
      const in_progress_map&);

    ss::future<std::error_code>
      do_topic_delete(topic_lifecycle_transition, model::offset);

    in_progress_map
    collect_in_progress(const model::topic_namespace&, const assignments_set&);

    ss::sharded<partition_allocator>& _partition_allocator;
    ss::sharded<topic_table>& _topic_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<partition_balancer_state>& _partition_balancer_state;
};

} // namespace cluster
