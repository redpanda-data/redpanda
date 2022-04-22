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

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

namespace cluster {

// Calculate vector of nodes that belongs to the cluster but are not partition
// replica set members
std::vector<model::node_id> calculate_non_overlapping_nodes(
  const partition_assignment& partition_members,
  const std::vector<model::node_id>& all_nodes);

} // namespace cluster
