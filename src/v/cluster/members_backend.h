#pragma once

#include "cluster/fwd.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus.h"

#include <seastar/core/condition-variable.hh>

#include <absl/container/node_hash_set.h>

#include <chrono>
#include <ostream>
namespace cluster {

class members_backend {
public:
    enum class reallocation_state { initial, reassigned, requested, finished };

    struct partition_reallocation {
        explicit partition_reallocation(
          model::ntp ntp, uint16_t replication_factor)
          : ntp(std::move(ntp))
          , constraints(ntp.tp.partition, replication_factor) {}

        model::ntp ntp;
        partition_constraints constraints;
        absl::node_hash_set<model::node_id> replicas_to_remove;
        std::optional<allocation_units> new_assignment;
        std::vector<model::broker_shard> initial_assignment;
        reallocation_state state = reallocation_state::initial;
        friend std::ostream&
        operator<<(std::ostream&, const partition_reallocation&);
    };
    /**
     * struct describing partition reallocation
     */
    struct update_meta {
        explicit update_meta(members_manager::node_update update)
          : update(update) {}

        members_manager::node_update update;
        std::vector<partition_reallocation> partition_reallocations;
        bool finished = false;
    };

    members_backend(
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<members_table>&,
      ss::sharded<controller_api>&,
      ss::sharded<members_manager>&,
      ss::sharded<members_frontend>&,
      consensus_ptr,
      ss::sharded<ss::abort_source>&);

    void start();
    ss::future<> stop();

private:
    void start_reconciliation_loop();
    ss::future<> reconcile();
    ss::future<> reallocate_replica_set(partition_reallocation&);

    ss::future<> try_to_finish_update(update_meta&);
    void calculate_reallocations(update_meta&);

    ss::future<> handle_updates();
    void handle_single_update(members_manager::node_update);
    void handle_recommissioned(const members_manager::node_update&);
    void stop_node_decommissioning(model::node_id);
    void handle_reallocation_finished(model::node_id);
    void reassign_replicas(partition_assignment&, partition_reallocation&);
    void calculate_reallocations_after_node_added(update_meta&) const;
    void setup_metrics();
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<members_table>& _members;
    ss::sharded<controller_api>& _api;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<members_frontend>& _members_frontend;
    consensus_ptr _raft0;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _bg;
    mutex _lock;
    model::term_id _last_term;

    // replicas reallocations in progress
    std::vector<update_meta> _updates;
    std::chrono::milliseconds _retry_timeout;
    ss::timer<> _retry_timer;
    ss::condition_variable _new_updates;
    ss::metrics::metric_groups _metrics;
};
std::ostream&
operator<<(std::ostream&, const members_backend::reallocation_state&);

} // namespace cluster
