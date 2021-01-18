#pragma once

#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "raft/consensus.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
namespace cluster {

class decommissioning_monitor {
public:
    struct reallocation_meta {
        reallocation_meta(model::ntp n, cluster::partition_assignment curr)
          : ntp(std::move(n))
          , current_assignment(std::move(curr)) {}

        model::ntp ntp;
        cluster::partition_assignment current_assignment;
        std::optional<partition_allocator::allocation_units> new_assignment;
        bool is_finished = false;
    };

    decommissioning_monitor(
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<partition_allocator>&,
      consensus_ptr,
      ss::sharded<ss::abort_source>&);

    void start();
    ss::future<> stop();

    void decommission(std::vector<model::node_id>);

private:
    void dispatch_decommissioning();
    ss::future<> try_decommission();
    ss::future<> do_decommission(model::node_id);
    ss::future<> reallocate_replica_set(reallocation_meta&);
    bool is_decommissioned(model::node_id) const;

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    consensus_ptr _raft0;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _bg;
    // vector of nodes which were decommissioned and decommissioning process for
    // those nodes is still in progress.
    std::vector<model::node_id> _decommissioned;
    // replicas reallocations in progress
    std::vector<reallocation_meta> _reallocations;
    std::chrono::milliseconds _retry_timeout;
    ss::timer<> _retry_timer;
};

} // namespace cluster
