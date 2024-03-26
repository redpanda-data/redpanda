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
      const model::ntp& ntp,
      const replicas_t& current_replicas,
      const allocation_constraints& ac,
      allocation_state& state,
      const partition_allocation_domain domain);
};

} // namespace cluster
