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
#include "coproc/partition_manager.h"
#include "kafka/server/materialized_partition.h"
#include "kafka/server/replicated_partition.h"

namespace kafka {

std::optional<partition_proxy> make_partition_proxy(
  const model::ntp& ntp,
  cluster::partition_manager& cluster_pm,
  coproc::partition_manager& coproc_pm) {
    auto partition = cluster_pm.get(ntp);
    if (partition) {
        return make_partition_proxy<replicated_partition>(partition);
    }
    auto cp_partition = coproc_pm.get(ntp);
    if (cp_partition) {
        return make_partition_proxy<materialized_partition>(cp_partition);
    }
    return std::nullopt;
}

} // namespace kafka
