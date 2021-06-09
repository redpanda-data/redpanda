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
  const model::materialized_ntp& mntp,
  ss::lw_shared_ptr<cluster::partition> partition,
  cluster::partition_manager& pm) {
    if (!mntp.is_materialized()) {
        return make_partition_proxy<replicated_partition>(partition);
    }
    if (auto log = pm.log(mntp.input_ntp()); log) {
        return make_partition_proxy<materialized_partition>(*log);
    }
    return std::nullopt;
}

} // namespace kafka
