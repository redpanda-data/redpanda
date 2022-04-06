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

namespace cluster {

class allocation_state;

hard_constraint_evaluator not_fully_allocated();
hard_constraint_evaluator is_active();

hard_constraint_evaluator on_node(model::node_id);

hard_constraint_evaluator on_nodes(const std::vector<model::node_id>&);

hard_constraint_evaluator
distinct_from(const std::vector<model::broker_shard>&);

soft_constraint_evaluator least_allocated();

soft_constraint_evaluator
distinct_rack(const std::vector<model::broker_shard>&, const allocation_state&);

} // namespace cluster
