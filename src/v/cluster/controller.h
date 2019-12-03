#pragma once

#include "cluster/metadata_cache.h"
#include "cluster/partition_allocator.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"
#include "storage/log_manager.h"

namespace cluster {

// all ops must belong to shard0
class controller final {
public:
    static constexpr const shard_id shard = 0;
    static constexpr const raft::group_id group{0};
    /// \brief used to distinguished log messages
    static constexpr model::record_batch_type controller_record_batch_type{3};
    static inline const model::ntp ntp{
      model::ns("redpanda"),
      model::topic_partition{model::topic("controller"),
                             model::partition_id(0)}};

    controller(
      sharded<partition_manager>&,
      sharded<shard_table>&,
      sharded<metadata_cache>&);

    future<> start();
    future<> stop();

    bool is_leader() const { return _recovered && _raft0->is_leader(); }

    model::node_id get_leader_id() const { return _raft0->config().leader_id; }

    future<std::vector<topic_result>> create_topics(
      std::vector<topic_configuration> topics,
      model::timeout_clock::time_point timeout);

    raft::group_id get_highest_group_id() const { return _highest_group_id; }

    future<> recover_assignment(partition_assignment);

private:
    struct batch_consumer {
        explicit batch_consumer(controller* c)
          : ptr(c) {}
        future<stop_iteration> operator()(model::record_batch batch) {
            return ptr->recover_batch(std::move(batch)).then([] {
                return stop_iteration::no;
            });
        }
        void end_of_stream() { ptr->end_of_stream(); }
        controller* ptr;
    };
    friend batch_consumer;
    
    future<consensus_ptr> start_raft0();
    future<> bootstrap_from_log(storage::log_ptr);
    future<> recover_batch(model::record_batch);
    future<> recover_record(model::record);
    future<> recover_replica(model::ntp, raft::group_id, model::broker_shard);
    future<> assign_group_to_shard(model::ntp, raft::group_id, uint32_t);
    future<> recover_topic_configuration(topic_configuration);
    future<> dispatch_record_recovery(log_record_key, iobuf&&);
    future<>
    update_cache_with_partitions_assignment(const partition_assignment&);
    std::optional<raft::entry>
    create_topic_cfg_entry(const topic_configuration&);
    void end_of_stream();
    void leadership_notification();
    void create_partition_allocator();
    allocation_node local_allocation_node();
    void on_raft0_entries_commited(std::vector<raft::entry>&&);
    future<> dispatch_manage_partition(model::ntp, raft::group_id, uint32_t);
    future<> manage_partition(partition_manager&, model::ntp, raft::group_id);
    future<> join_raft_group(raft::consensus&);

    model::broker _self;
    std::vector<config::seed_server> _seed_servers;
    sharded<partition_manager>& _pm;
    sharded<shard_table>& _st;
    sharded<metadata_cache>& _md_cache;
    raft::consensus* _raft0;
    raft::group_id _highest_group_id;
    bool _recovered = false;
    bool _leadership_notification_pending = false;
    std::unique_ptr<partition_allocator> _allocator;
    seastar::gate _bg;
};

// clang-format off
template<typename T>
CONCEPT(requires requires(const T& req) {
    { req.topic } -> model::topic;
})
// clang-format on
std::vector<topic_result> create_topic_results(
  const std::vector<T>& requests, topic_error_code error_code) {
    std::vector<topic_result> results;
    results.reserve(requests.size());
    std::transform(
      requests.begin(),
      requests.end(),
      std::back_inserter(results),
      [error_code](const T& r) { return topic_result(r.topic, error_code); });
    return results;
}
} // namespace cluster
