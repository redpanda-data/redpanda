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
#pragma once

#include "base/outcome.h"
#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "errc.h"
#include "model/fundamental.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster::data_migrations {
/*
 * This service performs data migration operations on individual partitions
 */
class worker : public ss::peering_sharded_service<worker> {
public:
    worker(
      model::node_id self,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<ss::abort_source>&);

    /*
     * Perform work necessary to transition an ntp to sought_state. Retries on
     * most errors. Waits forever if our shard is not the leader for the ntp.
     */
    ss::future<errc> perform_partition_work(
      model::ntp&& ntp,
      id migration,
      state sought_state,
      bool _tmp_wait_forever);

    /*
     * Aborts requested work where possible (i.e. when we are not the leader or
     * where we retry due to any error)
     */
    void abort_partition_work(model::ntp&& ntp);

private:
    void handle_leadership_update(const model::ntp& ntp, bool is_leader);

    model::node_id _self;
    ss::sharded<partition_leaders_table>& _leaders_table;
    ss::sharded<ss::abort_source>& _as;
    std::chrono::milliseconds _operation_timeout;
};

} // namespace cluster::data_migrations
