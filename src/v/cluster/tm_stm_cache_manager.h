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
#include "model/fundamental.h"

#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

/*
 * Owns all tm_stm_cache.
 */
class tm_stm_cache_manager {
public:
    tm_stm_cache_manager() = default;

    ss::lw_shared_ptr<cluster::tm_stm_cache>
    get(model::partition_id partition) {
        auto it = tm_stm_caches.lazy_emplace(
          partition, [partition](const cache_t::constructor& ctor) {
              ctor(partition, ss::make_lw_shared<cluster::tm_stm_cache>());
          });

        return it->second;
    }

private:
    using cache_t = absl::flat_hash_map<
      model::partition_id,
      ss::lw_shared_ptr<cluster::tm_stm_cache>>;
    cache_t tm_stm_caches;
};

} // namespace cluster
