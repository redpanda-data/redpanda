/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/datalake_manager.h"

#include "cluster/partition_manager.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "raft/group_manager.h"

#include <memory>

namespace datalake {

datalake_manager::datalake_manager(
  model::node_id self,
  ss::sharded<raft::group_manager>* group_mgr,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<cluster::topics_frontend>* topics_frontend,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::shard_table>* shards,
  ss::sharded<coordinator::frontend>* frontend,
  ss::sharded<ss::abort_source>* as,
  ss::scheduling_group sg,
  [[maybe_unused]] size_t memory_limit)
  : _self(self)
  , _group_mgr(group_mgr)
  , _partition_mgr(partition_mgr)
  , _topic_table(topic_table)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _shards(shards)
  , _coordinator_frontend(frontend)
  , _as(as)
  , _sg(sg) {}

ss::future<> datalake_manager::stop() { co_await _gate.close(); }

} // namespace datalake
