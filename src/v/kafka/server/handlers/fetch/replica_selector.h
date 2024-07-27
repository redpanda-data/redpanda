/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/fwd.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sharded.hh>

namespace kafka {

struct consumer_info {
    model::offset fetch_offset;
    std::optional<model::rack_id> rack_id;
    friend std::ostream& operator<<(std::ostream&, const consumer_info&);
};

struct replica_selector {
    replica_selector() = default;
    replica_selector(const replica_selector&) = default;
    replica_selector(replica_selector&&) = delete;
    replica_selector& operator=(const replica_selector&) = default;
    replica_selector& operator=(replica_selector&&) = delete;

    using replica_infos_t = std::vector<replica_info>;
    virtual ~replica_selector() = default;

    virtual std::optional<model::node_id>
    select_replica(const consumer_info&, const partition_info&) const = 0;
};

struct select_leader_replica : replica_selector {
    select_leader_replica() = default;
    std::optional<model::node_id> select_replica(
      const consumer_info&, const partition_info& p_info) const final {
        return p_info.leader;
    }
};

class rack_aware_replica_selector : public replica_selector {
public:
    explicit rack_aware_replica_selector(const cluster::metadata_cache&);
    std::optional<model::node_id>
    select_replica(const consumer_info&, const partition_info&) const final;

private:
    const cluster::metadata_cache& _md_cache;
};

} // namespace kafka
