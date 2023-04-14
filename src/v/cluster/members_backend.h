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
          model::partition_id p_id, uint16_t replication_factor)
          : constraints(partition_constraints(p_id, replication_factor)) {}

        partition_reallocation() = default;
        void set_new_replicas(allocation_units units) {
            allocation_units = std::move(units);
            new_replica_set
              = allocation_units->get_assignments().front().replicas;
        }

        void release_assignment_units() { allocation_units.reset(); }

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
        // it is ok to use a flat hash map here as it it will be limited in
        // size by the max concurrent reallocations batch size
        absl::flat_hash_map<model::ntp, partition_reallocation>
          partition_reallocations;
        bool finished = false;
        // unevenness error is normalized to be at most 1.0, set to max
        absl::flat_hash_map<partition_allocation_domain, double>
          last_unevenness_error;
    };

    struct reallocation_strategy {
        reallocation_strategy() = default;
        reallocation_strategy(const reallocation_strategy&) = default;
        reallocation_strategy(reallocation_strategy&&) = default;
        reallocation_strategy& operator=(const reallocation_strategy&)
          = default;
        reallocation_strategy& operator=(reallocation_strategy&&) = default;
        virtual ~reallocation_strategy() = default;
        virtual void reallocations_for_even_partition_count(
          size_t batch_size,
          partition_allocator&,
          topic_table&,
          update_meta&,
          partition_allocation_domain)
          = 0;
    };

    class default_reallocation_strategy : public reallocation_strategy {
        void reallocations_for_even_partition_count(
          size_t batch_size,
          partition_allocator&,
          topic_table&,
          update_meta&,
          partition_allocation_domain) final;

    private:
        void calculate_reallocations_batch(
          size_t batch_size,
          partition_allocator&,
          topic_table&,
          update_meta&,
          partition_allocation_domain);
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
    void stop_node_addition_and_ondemand_rebalance(model::node_id id);
    void handle_reallocation_finished(model::node_id);
    void reallocations_for_even_partition_count(
      update_meta&, partition_allocation_domain);

    ss::future<> calculate_reallocations_after_decommissioned(update_meta&);
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

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<members_table>& _members;
    ss::sharded<controller_api>& _api;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<members_frontend>& _members_frontend;
    ss::sharded<features::feature_table>& _features;
    std::unique_ptr<reallocation_strategy> _reallocation_strategy;
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
