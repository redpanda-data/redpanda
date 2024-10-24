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

#include "base/outcome.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"

namespace cluster {
class allocation_state;

class allocation_strategy {
public:
    result<model::node_id> choose_node(
      const allocation_state&,
      const allocation_constraints&,
      const allocated_partition&,
      std::optional<model::node_id> prev);
};

} // namespace cluster
