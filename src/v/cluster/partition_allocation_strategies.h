/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "model/metadata.h"

namespace cluster {

class round_robin_allocation_strategy final : public allocation_strategy::impl {
public:
    result<allocation_strategy::replicas_t> allocate_partition(
      const allocation_configuration&, allocation_state&) final;
    result<allocation_strategy::replicas_t> allocate_partition(
      const custom_allocation_configuration&, allocation_state&) final;

private:
    uint32_t _next_idx{0};
};

} // namespace cluster
