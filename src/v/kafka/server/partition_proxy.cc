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
    auto entry = md_cache.get_topic_cfg(model::topic_namespace_view(ntp));
    if (!entry) {
        return std::nullopt;
    }
    if (entry->properties.source_topic) {
        auto log = pm.log(ntp);
        auto source_ntp = model::ntp(
          ntp.ns, *entry->properties.source_topic, ntp.tp.partition);
        auto p = pm.get(source_ntp);
        if (log && p) {
            return make_partition_proxy<materialized_partition>(*log, p);
        }
    } else {
        return make_partition_proxy<replicated_partition>(pm.get(ntp));
    }
    return std::nullopt;
}

} // namespace kafka
