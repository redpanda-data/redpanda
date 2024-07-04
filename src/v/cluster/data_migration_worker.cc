/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/data_migration_worker.h"

#include "cluster/data_migration_types.h"
#include "partition_leaders_table.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"

#include <seastar/core/sleep.hh>

#include <fmt/ostream.h>

#include <optional>
#include <tuple>

namespace cluster::data_migrations {

// TODO: add configuration property
worker::worker(
  model::node_id self, partition_leaders_table& leaders, ss::abort_source& as)
  : _self(self)
  , _leaders_table(leaders)
  , _as(as)
  , _operation_timeout(5s) {}

ss::future<errc> worker::perform_partition_work(
  model::ntp&& ntp, id migration, state sought_state, bool _tmp_wait_forever) {
    // todo: subscribe to group_manager, wait until leader, perform actual work
    std::ignore = std::tuple(std::move(ntp), migration, sought_state);
    auto dur = rand() % 10 * 1s;
    if (_tmp_wait_forever) {
        dur += 100500s;
    }
    return ss::sleep(dur).then([]() { return errc::success; });
}

void worker::abort_partition_work(model::ntp&& ntp) {
    // todo: at least abort it right when we are waiting for leadership
    std::ignore = std::move(ntp);
}

//   if (_leaders_table.get_leader(ntp) == _self) {
//       // todo: local action?
//       // also do it somewhere else
//   }

} // namespace cluster::data_migrations
