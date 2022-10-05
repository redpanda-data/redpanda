/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/metadata.h"
#include "rpc/types.h"

namespace cluster {

struct node_status {
    model::node_id node_id;
    rpc::clock_type::time_point last_seen;
};

class node_status_table {
public:
    explicit node_status_table(model::node_id self)
      : _self(self) {}

    std::optional<node_status> get_node_status(model::node_id node) const {
        if (node == _self) {
            return node_status{
              .node_id = node, .last_seen = rpc::clock_type::now()};
        }

        if (auto it = _peers_status.find(node); it != _peers_status.end()) {
            return it->second;
        }

        return std::nullopt;
    }

    void update_peers(std::vector<node_status> updates) {
        for (auto& node_status : updates) {
            _peers_status[node_status.node_id] = std::move(node_status);
        }
    }

private:
    model::node_id _self;
    absl::flat_hash_map<model::node_id, node_status> _peers_status;
};
} // namespace cluster
