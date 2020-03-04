#pragma once

#include "cluster/controller_service.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/notification_latch.h"
#include "cluster/partition_allocator.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/log_manager.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>

#include <chrono>

namespace raft {
class connection_cache;
}

namespace cluster {

// all ops must belong to shard0
class controller final : public ss::peering_sharded_service<controller> {
public:
    static constexpr const ss::shard_id shard = 0;
    static constexpr const raft::group_id group{0};
    /// \brief used to distinguished log messages
    static constexpr model::record_batch_type controller_record_batch_type{3};
    static inline const model::ntp ntp{
      model::ns("redpanda"),
      model::topic_partition{model::topic("controller"),
                             model::partition_id(0)}};
    // FIXME: make it configurable
    static constexpr auto join_timeout = std::chrono::seconds(5);

    controller(
      ss::sharded<partition_manager>&,
      ss::sharded<shard_table>&,
      ss::sharded<metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<metadata_dissemination_service>&);

    ss::future<> start();
    ss::future<> stop();

    bool is_leader() const { return _recovered && _raft0->is_leader(); }

    std::optional<model::node_id> get_leader_id() const {
        return _raft0->get_leader_id();
    }

    ss::future<> process_join_request(model::broker broker);

    ss::future<std::vector<topic_result>> create_topics(
      std::vector<topic_configuration> topics,
      model::timeout_clock::time_point timeout);

    ss::future<std::vector<topic_result>> autocreate_topics(
      std::vector<topic_configuration>, model::timeout_clock::time_point);

    raft::group_id get_highest_group_id() const { return _highest_group_id; }

    ss::future<> recover_assignment(partition_assignment);

    /**
     * \brief Wait on controller to become the leader.
     *
     * Returns a future that resolves when this controller becomes the
     * leader of the raft0 group. The future will also resolve when the
     * controller is shutting down. This method serves only as a weak
     * signal. Any caller must make sure to properly check for leadership
     * again when necessary.
     */
    ss::future<> wait_for_leadership();

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    struct batch_consumer {
        explicit batch_consumer(controller* c)
          : ptr(c) {}
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            return ptr->process_raft0_batch(std::move(batch)).then([] {
                return ss::stop_iteration::no;
            });
        }
        void end_of_stream() { ptr->end_of_stream(); }
        controller* ptr;
    };
    friend batch_consumer;

    ss::future<consensus_ptr> start_raft0();
    ss::future<> process_raft0_batch(model::record_batch);
    ss::future<> process_raft0_cfg_update(model::record, model::offset);
    ss::future<> apply_raft0_cfg_update(raft::group_configuration);
    ss::future<> recover_record(model::record);
    ss::future<> recover_replica(
      model::ntp, raft::group_id, uint32_t, std::vector<model::broker_shard>);

    ss::future<> assign_group_to_shard(model::ntp, raft::group_id, uint32_t);
    ss::future<> recover_topic_configuration(topic_configuration);
    ss::future<> dispatch_record_recovery(log_record_key, iobuf&&);
    ss::future<>
    update_cache_with_partitions_assignment(const partition_assignment&);
    std::optional<model::record_batch>
    create_topic_cfg_batch(const topic_configuration&);
    ss::future<std::vector<topic_result>> do_create_topics(
      ss::semaphore_units<> units,
      std::vector<topic_configuration> topics,
      model::timeout_clock::time_point timeout);

    void end_of_stream();
    ss::future<> do_leadership_notification(
      model::ntp, model::term_id, std::optional<model::node_id>);
    void handle_leadership_notification(
      model::ntp, model::term_id, std::optional<model::node_id>);
    ss::future<> update_brokers_cache(std::vector<model::broker>);
    ss::future<>
      update_clients_cache(std::vector<broker_ptr>, std::vector<broker_ptr>);
    void create_partition_allocator();
    void update_partition_allocator(const std::vector<model::broker>&);
    allocation_node local_allocation_node();
    void on_raft0_entries_comitted(model::record_batch_reader&&);

    ss::future<> dispatch_manage_partition(
      model::ntp, raft::group_id, uint32_t, std::vector<model::broker_shard>);

    ss::future<> manage_partition(
      partition_manager&,
      model::ntp,
      raft::group_id,
      std::vector<model::broker_shard>);

    ss::future<> dispatch_join_to_seed_server(seed_iterator it);
    void join_raft0();

    template<typename Func>
    ss::futurize_t<std::result_of_t<Func(controller_client_protocol&)>>
    dispatch_rpc_to_leader(Func&&);

    ss::future<join_reply>
    dispatch_join_to_remote(const config::seed_server&, model::broker);

    model::broker _self;
    std::vector<config::seed_server> _seed_servers;
    ss::sharded<partition_manager>& _pm;
    ss::sharded<shard_table>& _st;
    ss::sharded<metadata_cache>& _md_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<metadata_dissemination_service>& _md_dissemination_service;
    raft::consensus* _raft0;
    raft::group_id _highest_group_id;
    bool _recovered = false;
    bool _leadership_notification_pending = false;
    std::unique_ptr<partition_allocator> _allocator;
    ss::condition_variable _leadership_cond;
    ss::gate _bg;
    // Semaphore used to make sure that the controller state i.e. topics and
    // partition metadata are updated atomically
    ss::semaphore _sem{1};
    model::offset _raft0_cfg_offset;
    partition_manager::notification_id_type _leader_notify_handle;
    notification_latch _notification_latch;
    ss::abort_source _as;
};
} // namespace cluster
