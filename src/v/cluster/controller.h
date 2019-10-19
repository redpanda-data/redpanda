#pragma once

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
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
      model::node_id,
      sstring basedir,
      size_t max_segment_size,
      sharded<partition_manager>&,
      sharded<shard_table>&);

    future<> start();
    future<> stop();

    bool is_leader() const {
        return _raft0->raft()->is_leader();
    }

    future<std::vector<topic_result>> create_topics(
      model::ns ns,
      std::vector<topic_configuration> topics,
      model::timeout_clock::time_point timeout);

private:
    // stages the pre_commit hooks from raft
    struct stage_hook final : raft::append_entries_proto_hook {
        explicit stage_hook(controller* self);
        void pre_commit(model::offset begin, const entries&) final;
        void abort(model::offset begin) final;
        void commit(model::offset begin, model::offset committed) final;
        controller* ptr;
    };
    friend stage_hook;

    struct batch_consumer {
        explicit batch_consumer(controller* c)
          : ptr(c) {
        }
        future<stop_iteration> operator()(model::record_batch batch) {
            return ptr->recover_batch(std::move(batch)).then([] {
                return stop_iteration::no;
            });
        }
        void end_of_stream() {
            ptr->end_of_stream();
        }
        controller* ptr;
    };
    friend batch_consumer;

    future<> bootstrap_from_log(storage::log_ptr);
    future<> recover_batch(model::record_batch);
    future<> recover_record(model::record);
    future<> recover_assignment(partition_assignment);
    void end_of_stream();

    model::node_id _self;
    storage::log_manager _mngr;
    std::unique_ptr<partition> _raft0;
    sharded<partition_manager>& _pm;
    sharded<shard_table>& _st;
    stage_hook _stgh;
};

} // namespace cluster
