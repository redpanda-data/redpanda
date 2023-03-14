#pragma once

#include "cluster/fwd.h"
#include "cluster/members_manager.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus.h"

#include <seastar/core/condition-variable.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <chrono>
#include <limits>
#include <ostream>
namespace cluster {

class members_backend {
public:
    enum class reallocation_state {
        initial,
        reassigned,
        requested,
        finished,
        request_cancel,
        cancelled
    };

    struct partition_reallocation {
        explicit partition_reallocation(
          model::ntp ntp, uint16_t replication_factor)
          : ntp(std::move(ntp))
          , constraints(
              partition_constraints(ntp.tp.partition, replication_factor)) {}

        explicit partition_reallocation(model::ntp ntp)
          : ntp(std::move(ntp)) {}

        void set_new_replicas(allocation_units units) {
            allocation_units = std::move(units);
            new_replica_set
              = allocation_units->get_assignments().front().replicas;
        }

        void release_assignment_units() { allocation_units.reset(); }

        model::ntp ntp;
        std::optional<partition_constraints> constraints;
        absl::node_hash_set<model::node_id> replicas_to_remove;
        std::optional<allocation_units> allocation_units;
        std::vector<model::broker_shard> new_replica_set;
        std::vector<model::broker_shard> current_replica_set;
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

        // on demand rebalance request
        explicit update_meta() noexcept = default;

        // optional node update, if present the update comes from
        // members_manager, otherwise it is on demand update
        std::optional<members_manager::node_update> update;

        std::vector<partition_reallocation> partition_reallocations;
        bool finished = false;
        // unevenness error is normalized to be at most 1.0, set to max
        absl::flat_hash_map<partition_allocation_domain, double>
          last_unevenness_error;
        absl::flat_hash_map<partition_allocation_domain, size_t> last_ntp_index;
        // revision of a related decommission command, present only in
        // recommission update_meta
        std::optional<model::revision_id> decommission_update_revision;
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

    ss::future<std::error_code> request_rebalance();

private:
    static constexpr model::revision_id raft0_revision{0};
    using update_t = members_manager::node_update_type;
    struct node_replicas {
        size_t allocated_replicas;
        size_t max_capacity;
    };
    struct unevenness_error_info {
        double e;
        double e_step;
    };
    using node_replicas_map_t
      = absl::node_hash_map<model::node_id, members_backend::node_replicas>;
    void start_reconciliation_loop();
    ss::future<> reconciliation_loop();
    ss::future<std::error_code> reconcile();
    ss::future<> reallocate_replica_set(partition_reallocation&);

    ss::future<> try_to_finish_update(update_meta&);
    ss::future<> calculate_reallocations(update_meta&);

    ss::future<> handle_updates();
    void handle_single_update(members_manager::node_update);
    model::revision_id handle_recommissioned_and_get_decomm_revision(
      members_manager::node_update&);
    void stop_node_decommissioning(model::node_id);
    void stop_node_addition_and_ondemand_rebalance(model::node_id id);
    void handle_reallocation_finished(model::node_id);
    void reassign_replicas(partition_assignment&, partition_reallocation&);
    void
    calculate_reallocations_batch(update_meta&, partition_allocation_domain);
    void reallocations_for_even_partition_count(
      update_meta&, partition_allocation_domain);
    ss::future<> calculate_reallocations_after_decommissioned(update_meta&);
    ss::future<> calculate_reallocations_after_recommissioned(update_meta&);
    std::vector<model::ntp> ntps_moving_from_node_older_than(
      model::node_id, model::revision_id) const;
    void setup_metrics();
    absl::node_hash_map<model::node_id, node_replicas>
      calculate_replicas_per_node(partition_allocation_domain) const;

    unevenness_error_info
      calculate_unevenness_error(partition_allocation_domain) const;
    bool should_stop_rebalancing_update(const update_meta&) const;

    static size_t calculate_total_replicas(const node_replicas_map_t&);
    ss::future<std::error_code>
    update_raft0_configuration(const members_manager::node_update&);

    ss::future<std::error_code>
      add_to_raft0(model::node_id, model::revision_id);
    ss::future<std::error_code>
      remove_from_raft0(model::node_id, model::revision_id);

    ss::future<> reconcile_raft0_updates();
    ss::future<std::error_code> do_remove_node(model::node_id);

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
    mutex _lock;
    model::term_id _last_term;

    // replicas reallocations in progress
    std::vector<update_meta> _updates;
    ss::circular_buffer<members_manager::node_update> _raft0_updates;
    std::chrono::milliseconds _retry_timeout;
    ss::condition_variable _new_updates;
    ss::metrics::metric_groups _metrics;
    config::binding<size_t> _max_concurrent_reallocations;
};
std::ostream&
operator<<(std::ostream&, const members_backend::reallocation_state&);

} // namespace cluster
