/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/tm_stm_cache.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "model/fundamental.h"

#include <optional>

namespace cluster {

/*
 * Owns all tm_stm_cache.
 */
class tm_stm_cache_manager {
public:
    tm_stm_cache_manager(int32_t tm_stm_partitions) {
        for (int32_t i = 0; i < tm_stm_partitions; ++i) {
            auto tm_stm_cache = ss::make_lw_shared<cluster::tm_stm_cache>();
            tm_stm_caches.push_back(tm_stm_cache);
        }
    }

    ss::lw_shared_ptr<cluster::tm_stm_cache>
    get(model::partition_id partition) {
        vassert(
          partition >= 0 && size_t(partition) < tm_stm_caches.size(),
          "Invalid tm stm cache partition {}. Current partitions amount: {}",
          partition,
          tm_stm_caches.size());
        return tm_stm_caches[partition];
    }

private:
    std::vector<ss::lw_shared_ptr<cluster::tm_stm_cache>> tm_stm_caches;
};

} // namespace cluster
