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

#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/group_configuration.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <ostream>

namespace cluster {

/// on every core, sharded

class controller_backend
  : public ss::peering_sharded_service<controller_backend> {
public:
    using results_t = std::vector<std::error_code>;
    controller_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<shard_table>&,
      ss::sharded<partition_manager>&,
      ss::sharded<non_replicable_partition_manager>&,
      ss::sharded<members_table>&,
      ss::sharded<cluster::partition_leaders_table>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<storage::api>&,
      ss::sharded<seastar::abort_source>&);

    ss::future<> stop();
    ss::future<> start();

    std::vector<topic_table::delta> list_ntp_deltas(const model::ntp&) const;

private:
    struct cross_shard_move_request {
        cross_shard_move_request(model::revision_id, raft::group_configuration);

        model::revision_id revision;
        raft::group_configuration initial_configuration;
        friend std::ostream& operator<<(
          std::ostream& o,
          const controller_backend::cross_shard_move_request& r) {
            fmt::print(
              o,
              "{{revision: {}, configuration: {}}}",
              r.revision,
              r.initial_configuration);
            return o;
        }
    };

    using deltas_t = std::vector<topic_table::delta>;
    using underlying_t = absl::flat_hash_map<model::ntp, deltas_t>;

    // Topics
    ss::future<> bootstrap_controller_backend();
    void start_topics_reconciliation_loop();

    ss::future<> fetch_deltas();

    ss::future<> reconcile_topics();
    ss::future<> reconcile_ntp(deltas_t&);

    ss::future<std::error_code>
    execute_partitition_op(const topic_table::delta&);

    ss::future<std::error_code> process_partition_update(
      model::ntp,
      const partition_assignment&,
      const partition_assignment&,
      model::revision_id);
    ss::future<> finish_partition_update(
      model::ntp, const partition_assignment&, model::revision_id);

    ss::future<>
      process_partition_properties_update(model::ntp, partition_assignment);

    ss::future<std::error_code> create_partition(
      model::ntp,
      raft::group_id,
      model::revision_id,
      std::vector<model::broker>);
    ss::future<std::error_code>
      create_non_replicable_partition(model::ntp, model::revision_id);
    ss::future<>
      add_to_shard_table(model::ntp, ss::shard_id, model::revision_id);
    ss::future<> add_to_shard_table(
      model::ntp, raft::group_id, ss::shard_id, model::revision_id);
    ss::future<>
      remove_from_shard_table(model::ntp, raft::group_id, model::revision_id);
    ss::future<> delete_partition(model::ntp, model::revision_id);
    ss::future<>
      delete_non_replicable_partition(model::ntp, model::revision_id);
    ss::future<std::error_code> update_partition_replica_set(
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      model::revision_id);
    ss::future<std::error_code>
      dispatch_update_finished(model::ntp, partition_assignment);

    ss::future<> do_bootstrap();
    ss::future<> bootstrap_ntp(const model::ntp&, deltas_t&);

    ss::future<std::error_code>
      shutdown_on_current_shard(model::ntp, model::revision_id);

    ss::future<std::optional<cross_shard_move_request>>
      ask_remote_shard_for_initail_rev(model::ntp, ss::shard_id);

    void housekeeping();

    ss::sharded<topic_table>& _topics;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<non_replicable_partition_manager>& _nr_partition_manager;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<storage::api>& _storage;
    model::node_id _self;
    ss::sstring _data_directory;
    std::chrono::milliseconds _housekeeping_timer_interval;
    ss::sharded<ss::abort_source>& _as;
    underlying_t _topic_deltas;
    ss::timer<> _housekeeping_timer;
    ss::semaphore _topics_sem{1};
    ss::gate _gate;
    /**
     * This map is populated by backend instance on shard that given NTP is
     * moved from. Map is then queried by the controller instance on target
     * shard. Partition is created on target shard with the same initial
     * revision and configuration as on originating shard, this way identity of
     * node i.e. raft vnode doesn't change.
     */
    absl::node_hash_map<model::ntp, cross_shard_move_request>
      _cross_shard_requests;
    /**
     * This map is populated when bootstrapping. If partition is moved cross
     * shard on the same node it has to be created with revision that it was
     * first created on current node before cross core move series
     */
    absl::node_hash_map<model::ntp, model::revision_id> _bootstrap_revisions;
};

std::vector<topic_table::delta> calculate_bootstrap_deltas(
  model::node_id self, const std::vector<topic_table::delta>&);
} // namespace cluster
