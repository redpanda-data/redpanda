#pragma once
#include "cluster/commands.h"
#include "cluster/partition_allocator.h"
#include "cluster/topic_table.h"
#include "model/record.h"

#include <seastar/core/sharded.hh>

namespace cluster {

// The topic updates dispatcher is resposible for receiving update_apply upcalls
// from controller state machine and propagating updates to topic state core
// local copies. The dispatcher handles partition_allocator updates. The
// partition allocator exists only on core 0 hence the updates have to be
// executed at the same core.
//
//
//                                  +----------------+        +------------+
//                              +-->| Table@core_0   |---+    |            |
//                              |   +----------------+   |    |  Allocator |
//                              |                        |    |            |
//                              |   +----------------+   |    +------------+
//                              +-->| Table@core_0   |---+          ^
//    on core 0                 |   +----------------+   |          |
//   +-----+   +------------+   |                        |    +------------+
//   |     |   |            |   |           .            |    |            |
//   | STM +-->+ Dispatcher +-->+           .            +--->+ Dispatcher +--->
//   |     |   |            |   |           .            |    |            |
//   +-----+   +------------+   |                        |    +------------+
//                              |   +----------------+   |   on core 0
//                              +-->| Table@core n-1 |---+
//                              |   +----------------+   |
//                              |                        |
//                              |   +----------------+   |
//                              +-->| Table@core #n  |---+
//                                  +----------------+
//
class topic_updates_dispatcher {
public:
    topic_updates_dispatcher(
      ss::sharded<partition_allocator>&, ss::sharded<topic_table>&);

    ss::future<std::error_code> apply_update(model::record_batch);

    static constexpr auto commands
      = make_commands_list<create_topic_cmd, delete_topic_cmd>();

    bool is_batch_applicable(const model::record_batch& batch) const {
        return batch.header().type == topic_batch_type;
    }

private:
    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(Cmd);

    void update_allocations(const create_topic_cmd&);
    void deallocate_topic(const model::topic_metadata&);

    ss::sharded<partition_allocator>& _partition_allocator;
    ss::sharded<topic_table>& _topic_table;
};

} // namespace cluster