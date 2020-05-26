#pragma once

#include "cluster/controller_service.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_allocator.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/state_machine.h"
#include "seastarx.h"
#include "storage/log_manager.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>

#include <chrono>
#include <optional>

namespace raft {
class connection_cache;
}

namespace cluster {

// all ops must belong to shard0
class controller final
  : public raft::state_machine
  , public ss::peering_sharded_service<controller> {
public:
    static constexpr const ss::shard_id shard = 0;
    static constexpr const raft::group_id group{0};
    // FIXME: make it configurable
    static constexpr auto join_timeout = std::chrono::seconds(5);

    controller(
      ss::sharded<raft::group_manager>&,
      ss::sharded<partition_manager>&,
      ss::sharded<shard_table>&,
      ss::sharded<metadata_cache>&,
      ss::sharded<rpc::connection_cache>&);

    ss::future<> start();
    ss::future<> stop();

    bool is_leader() const { return _raft0->is_leader() && _is_leader; }

    std::optional<model::node_id> get_leader_id() const {
        auto leader_id = _raft0->get_leader_id();
        if (leader_id == _self.id() && !_is_leader) {
            return std::nullopt;
        }
        return leader_id;
    }

    ss::future<result<join_reply>> process_join_request(model::broker);

    ss::future<std::vector<topic_result>> create_topics(
      std::vector<topic_configuration> topics,
      model::timeout_clock::time_point timeout);

    ss::future<std::vector<topic_result>> delete_topics(
      std::vector<model::topic_namespace> topics,
      model::timeout_clock::time_point timeout);

    ss::future<std::vector<topic_result>> autocreate_topics(
      std::vector<topic_configuration>, model::timeout_clock::duration);

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

    virtual ss::future<> apply(model::record_batch) final;

    ss::future<consensus_ptr> start_raft0();
    ss::future<> process_raft0_batch(model::record_batch);
    ss::future<> process_raft0_cfg_update(model::record, model::offset);
    ss::future<> apply_raft0_cfg_update(const raft::group_configuration&);
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

    ss::future<std::vector<topic_result>> do_delete_topics(
      ss::semaphore_units<> units,
      std::vector<model::topic_namespace> topics,
      model::timeout_clock::time_point timeout);

    ss::future<> do_leadership_notification(
      model::ntp, model::term_id, std::optional<model::node_id>);
    ss::future<> handle_controller_leadership_notification(
      ss::semaphore_units<>, model::term_id, std::optional<model::node_id>);
    void handle_leadership_notification(
      model::ntp, model::term_id, std::optional<model::node_id>);
    ss::future<> update_brokers_cache(std::vector<model::broker>);
    ss::future<>
      update_clients_cache(std::vector<broker_ptr>, std::vector<broker_ptr>);
    void create_partition_allocator();
    void update_partition_allocator(const std::vector<broker_ptr>&);
    allocation_node local_allocation_node();

    ss::future<> dispatch_manage_partition(
      model::ntp, raft::group_id, uint32_t, std::vector<model::broker_shard>);

    ss::future<> manage_partition(
      partition_manager&,
      storage::ntp_config,
      raft::group_id,
      std::vector<model::broker_shard>);

    ss::future<result<join_reply>>
    dispatch_join_to_seed_server(seed_iterator it);

    void join_raft0();
    bool is_already_member() const {
        return _raft0->config().contains_broker(_self.id());
    }

    template<typename Func>
    auto dispatch_rpc_to_leader(Func&&);

    ss::future<result<join_reply>>
    dispatch_join_to_remote(const config::seed_server&, model::broker);

    void recover_assignment_in_background(partition_assignment);
    ss::future<std::vector<topic_result>> process_autocreate_response(
      std::vector<topic_configuration>, result<create_topics_reply>);

    storage::ntp_config make_raft0_ntp_config() const;
    ss::future<> process_topic_deletion(model::topic_namespace);
    ss::future<>
      delete_partitition(model::topic_namespace, model::partition_metadata);

    ss::future<std::vector<topic_result>> process_replicate_topic_op_result(
      std::vector<model::topic_namespace>,
      model::timeout_clock::time_point,
      ss::future<result<raft::replicate_result>>);

    ss::future<std::vector<topic_result>> wait_for_all_topic_leaders(
      std::vector<topic_result>, model::timeout_clock::time_point);
    ss::future<topic_result> wait_for_topic_leaders(
      model::topic_namespace, model::timeout_clock::time_point);

    model::broker _self;
    std::vector<config::seed_server> _seed_servers;
    ss::sstring _data_directory;
    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<partition_manager>& _pm;
    ss::sharded<shard_table>& _st;
    ss::sharded<metadata_cache>& _md_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    raft::consensus* _raft0;
    raft::group_id _highest_group_id;
    bool _is_leader = false;
    std::unique_ptr<partition_allocator> _allocator;
    ss::condition_variable _leadership_cond;
    ss::gate _bg;

    /// Controller locking mechanism
    ///
    /// Controller concurrency control is based on two mutexes. The state mutex
    /// guarantee atomic controller state update and access. (f.e. checking if
    /// topic exists, updating metadata cache). Controller state is only updated
    /// when raft0 leadership change or raft0 apply entries to controller via
    /// append entries notification upcall. The state update requires holding
    /// the state mutex. Similar situation takes place when processing requests.
    /// When replicating the state, controller holds a mutex to make sure that
    /// it register promise for an offset returned from replicate before
    /// replicate upcall will be fired by raft. This is guranteed as promise and
    /// upcall use the same 'state mutex'. In order to serialize operations
    /// executed on the controller, as they are not commutative, controller
    /// holds operation mutex while operation is being processed (replicate +
    /// waiting for append_entries upcall) this guarantees that all operations
    /// will be validated against and will change valid up to date state. (If
    /// concurrent operations would be allowed it may lead to creation of the
    /// same topic twice as state would not be updated before validating
    /// request)
    ///
    /// NOTE: This is a temporary solution before we move to after-image based
    ///       state updates in controller.
    ///
    ///
    ///        operation_mutex
    ///        +------------------------------------------------------+
    ///        | state mutex               state mutex                |
    ///        | +------------------+      +------------------------+ |
    /// op     | | validate request |      | process notification   | | result
    /// +----->+ | raft0 replicate  +----->+ update state           | +--->
    ///        | |                  |      |                        | |
    ///        | +------------------+      +------------------------+ |
    ///        +------------------------------------------------------+
    ///
    mutex _state_lock;
    mutex _operation_lock;
    ss::semaphore _recovery_semaphore{0};
    model::offset _raft0_cfg_offset;
    cluster::notification_id_type _leader_notify_handle;
    ss::abort_source _as;
    static constexpr raft::consistency_level default_consistency_level
      = raft::consistency_level::quorum_ack;
};
} // namespace cluster
