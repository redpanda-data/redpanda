/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/units.h"
#include "cluster/fwd.h"
#include "datalake/fwd.h"
#include "raft/fundamental.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace datalake {

/*
 * Per shard instance responsible for launching and synchronizing all datalake
 * related tasks like file format translation, frontend etc.
 */
class datalake_manager : public ss::peering_sharded_service<datalake_manager> {
public:
    datalake_manager(
      model::node_id self,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
      ss::sharded<coordinator::frontend>*,
      ss::sharded<ss::abort_source>*,
      ss::scheduling_group sg,
      size_t memory_limit);

    ss::future<> stop();

private:
    ss::future<> stop_translator(raft::group_id);
    model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shards;
    ss::sharded<coordinator::frontend>* _coordinator_frontend;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;
};

} // namespace datalake
