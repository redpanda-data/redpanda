// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/rm_group_frontend.h"

#include "cluster/id_allocator_frontend.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>

namespace kafka {
using namespace std::chrono_literals;

rm_group_frontend::rm_group_frontend(
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<cluster::partition_leaders_table>& leaders,
  cluster::controller* controller,
  ss::sharded<kafka::coordinator_ntp_mapper>& coordinator_mapper,
  ss::sharded<kafka::group_router>& group_router)
  : _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _controller(controller)
  , _coordinator_mapper(coordinator_mapper)
  , _group_router(group_router)
  , _metadata_dissemination_retries(
      config::shard_local_cfg().metadata_dissemination_retries.value())
  , _metadata_dissemination_retry_delay_ms(
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {
}

} // namespace kafka
