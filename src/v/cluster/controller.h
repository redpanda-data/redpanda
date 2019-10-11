#pragma once

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log_manager.h"

namespace cluster {

// all ops must belong to shard0
class controller final {
public:
    static constexpr shard_id shard = 0;

    controller(
      model::node_id,
      sstring basedir,
      size_t max_segment_size,
      io_priority_class io_priority,
      sharded<partition_manager>&,
      sharded<shard_table>&);

    future<> start();
    future<> stop();

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

    model::node_id _self;
    io_priority_class _prio;
    storage::log_manager _mngr;
    std::unique_ptr<partition> _raft0;
    sharded<partition_manager>& _pm;
    sharded<shard_table>& _st;
    stage_hook _stgh;
};

} // namespace cluster
