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
#include "model/timestamp.h"

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

struct partition_balancer_violations {
    struct unavailable_node {
        model::node_id id;
        model::timestamp unavailable_since;

        unavailable_node(model::node_id id, model::timestamp unavailable_since)
          : id(id)
          , unavailable_since(unavailable_since) {}
    };

    struct full_node {
        model::node_id id;
        uint32_t disk_used_percent;

        full_node(model::node_id id, uint32_t disk_used_percent)
          : id(id)
          , disk_used_percent(disk_used_percent) {}
    };

    std::vector<unavailable_node> unavailable_nodes;
    std::vector<full_node> full_nodes;

    bool is_empty() const {
        return unavailable_nodes.empty() && full_nodes.empty();
    }
};

} // namespace cluster
