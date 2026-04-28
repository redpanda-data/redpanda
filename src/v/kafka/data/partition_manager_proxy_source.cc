/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/data/partition_manager_proxy_source.h"

#include "cluster/partition_manager.h"

namespace kafka {

partition_manager_proxy_source::partition_manager_proxy_source(
  cluster::partition_manager& pm) noexcept
  : _pm(pm) {}

chunked_vector<model::ktp> partition_manager_proxy_source::all_ktps() const {
    chunked_vector<model::ktp> out;
    for (const auto& p : _pm.partitions()) {
        out.emplace_back(p.first.tp.topic, p.first.tp.partition);
    }
    return out;
}

std::optional<partition_proxy>
partition_manager_proxy_source::get(const model::ktp& ktp) {
    return make_partition_proxy(ktp, _pm);
}

} // namespace kafka
