#pragma once

#include "cluster/fwd.h"
#include "cluster/members_manager.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/condition-variable.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <chrono>
#include <iosfwd>
namespace cluster {

class members_backend {
public:
    enum class cancellation_state {
        request_cancel,
        cancelled,
        finished,
    };

    struct partition_reallocation {
        partition_reallocation(
          replicas_t current_rs, replicas_t new_rs, cancellation_state state)
          : current_replica_set(std::move(current_rs))
          , new_replica_set(std::move(new_rs))
          , state(state) {}

        replicas_t current_replica_set;
        replicas_t new_replica_set;
        // Currently members_backend only dispatches cancellations after node
        // recommission, other required partition movements are dispatched by
        // partition_balancer.
        cancellation_state state;

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
        // it is ok to use a flat hash map here as it it will be limited in
        // size by the max concurrent reallocations batch size
        absl::flat_hash_map<model::ntp, partition_reallocation>
          partition_reallocations;
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
      ss::sharded<features::feature_table>&,
      consensus_ptr,
      ss::sharded<ss::abort_source>&);

    void start();
    ss::future<> stop();

private:
    static constexpr model::revision_id raft0_revision{0};

    void start_reconciliation_loop();
    ss::future<> reconciliation_loop();
    ss::future<std::error_code> reconcile();
    ss::future<>
    reconcile_reallocation_state(const model::ntp&, partition_reallocation&);

    ss::future<> try_to_finish_update(update_meta&);
    ss::future<> calculate_reallocations(update_meta&);

    ss::future<> handle_updates();
    void handle_single_update(members_manager::node_update);
    void stop_node_decommissioning(model::node_id);
    void stop_node_addition(model::node_id id);
    void handle_reallocation_finished(model::node_id);

    ss::future<> calculate_reallocations_after_recommissioned(update_meta&);
    std::vector<model::ntp> ntps_moving_from_node_older_than(
      model::node_id, model::revision_id) const;
    void setup_metrics();

    bool should_stop_rebalancing_update(const update_meta&) const;

    ss::future<std::error_code>
    update_raft0_configuration(const members_manager::node_update&);

    ss::future<std::error_code>
      add_to_raft0(model::node_id, model::revision_id);
    ss::future<std::error_code>
      remove_from_raft0(model::node_id, model::revision_id);

    ss::future<> reconcile_raft0_updates();
    ss::future<std::error_code> do_remove_node(model::node_id);
    ss::future<> maybe_finish_decommissioning(update_meta&);

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<members_table>& _members;
    ss::sharded<controller_api>& _api;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<members_frontend>& _members_frontend;
    ss::sharded<features::feature_table>& _features;
    consensus_ptr _raft0;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _bg;
    mutex _lock{"members_backend::lock"};
    model::term_id _last_term;

    // replicas reallocations in progress
    std::vector<update_meta> _updates;
    ss::circular_buffer<members_manager::node_update> _raft0_updates;
    std::chrono::milliseconds _retry_timeout;
    ss::condition_variable _new_updates;
    metrics::public_metric_groups _metrics;
    config::binding<size_t> _max_concurrent_reallocations;
};
std::ostream&
operator<<(std::ostream&, const members_backend::cancellation_state&);

} // namespace cluster
