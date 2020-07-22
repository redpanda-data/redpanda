#pragma once

#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "outcome.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

/// on every core, sharded

class controller_backend {
public:
    controller_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<shard_table>&,
      ss::sharded<partition_manager>&,
      ss::sharded<members_table>&,
      ss::sharded<cluster::partition_leaders_table>&,
      ss::sharded<seastar::abort_source>&);

    ss::future<> stop();
    ss::future<> start();

private:
    template<typename T>
    struct task_meta {
        explicit task_meta(T t)
          : delta(std::move(t)) {}

        bool finished = false;
        T delta;
    };

    // Topics
    void start_topics_reconciliation_loop();
    ss::future<> reconcile_topics();
    ss::future<> do_reconcile_topic(task_meta<topic_table::delta>&);
    ss::future<>
      create_partition(model::ntp, raft::group_id, std::vector<model::broker>);
    ss::future<> add_to_shard_table(model::ntp, raft::group_id, ss::shard_id);
    ss::future<> delete_partition(model::ntp);
    void housekeeping();

    ss::sharded<topic_table>& _topics;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    model::node_id _self;
    ss::sstring _data_directory;
    ss::sharded<ss::abort_source>& _as;
    std::vector<task_meta<topic_table::delta>> _topic_deltas;
    ss::timer<> _housekeeping_timer;
    ss::semaphore _topics_sem{1};
    ss::gate _gate;
};
} // namespace cluster