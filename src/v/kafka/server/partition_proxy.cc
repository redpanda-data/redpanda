/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "partition_proxy.h"

#include "cluster/partition_manager.h"
#include "kafka/server/materialized_partition.h"
#include "kafka/server/replicated_partition.h"

namespace kafka {

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp&,
  ss::lw_shared_ptr<cluster::partition> partition,
  cluster::partition_manager&) {
    /// Render materialized_partition unused in this commit only. In a
    /// subsequent commit a new method will be used to decide weather a
    /// partition_proxy is materialized or not
    return make_partition_proxy<replicated_partition>(partition);
}

} // namespace kafka
