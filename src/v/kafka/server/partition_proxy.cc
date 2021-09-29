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
  const model::ntp& ntp,
  cluster::metadata_cache& md_cache,
  cluster::partition_manager& pm) {
    auto log = pm.get(ntp);
    if (log) {
        return make_partition_proxy<replicated_partition>(log);
    }
    auto mts = md_cache.get_materialized_status(
      model::topic_namespace_view{ntp});
    if (!mts) {
        return std::nullopt;
    }
    auto source_ntp = model::ntp(ntp.ns, *mts, ntp.tp.partition);
    auto src_log = pm.get(source_ntp);
    vassert(src_log, "Materialized topic must have an associated source");
    auto materialized_log = pm.log(ntp);
    vassert(materialized_log, "log should exist for materialized partition");
    return make_partition_proxy<materialized_partition>(
      *materialized_log, src_log);
}

} // namespace kafka
