/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"

namespace cluster {

struct node_disk_space {
    model::node_id node_id;
    uint64_t free_space;
    uint64_t total_space;
    double free_space_rate;

    inline node_disk_space(
      model::node_id node_id, uint64_t free_space, uint64_t total_space)
      : node_id(node_id)
      , free_space(free_space)
      , total_space(total_space)
      , free_space_rate(double(free_space) / double(total_space)) {}

    bool operator==(const node_disk_space& other) const {
        return node_id == other.node_id;
    }

    bool operator<(const node_disk_space& other) const {
        return free_space_rate < other.free_space_rate;
    }
};

} // namespace cluster