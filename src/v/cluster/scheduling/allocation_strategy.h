/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/scheduling/types.h"
#include "model/metadata.h"
#include "outcome.h"

namespace cluster {
class allocation_state;

class allocation_strategy {
public:
    struct impl {
        /**
         * Allocates single replica according to set of given allocation
         * constraints in the specified domain
         */
        virtual result<model::broker_shard> allocate_replica(
          const replicas_t&,
          const allocation_constraints&,
          allocation_state&,
          partition_allocation_domain)
          = 0;

        virtual ~impl() noexcept = default;
    };

    explicit allocation_strategy(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    result<model::broker_shard> allocate_replica(
      const replicas_t& current_replicas,
      const allocation_constraints& ac,
      allocation_state& state,
      const partition_allocation_domain domain) {
        return _impl->allocate_replica(current_replicas, ac, state, domain);
    }

private:
    std::unique_ptr<impl> _impl;
};

template<typename Impl, typename... Args>
allocation_strategy make_allocation_strategy(Args&&... args) {
    return allocation_strategy(
      std::make_unique<Impl>(std::forward<Args>(args)...));
}

allocation_strategy simple_allocation_strategy();
} // namespace cluster
