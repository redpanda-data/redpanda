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

#include "config/configuration.h"
#include "config/property.h"
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
      : _self(self)
      , _isolation_timeout(
          config::shard_local_cfg().node_isolation_heartbeat_timeout.bind()) {}

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

    bool is_isolated() const {
        if (_peers_status.empty()) {
            return false;
        }

        for (auto& status : _peers_status) {
            if (status.first == _self) {
                continue;
            }

            auto diff = rpc::clock_type::now() - status.second.last_seen;
            if (diff / 1ms < _isolation_timeout()) {
                return false;
            }
        }

        return true;
    }

private:
    model::node_id _self;
    absl::flat_hash_map<model::node_id, node_status> _peers_status;
    config::binding<int64_t> _isolation_timeout;
};
} // namespace cluster
