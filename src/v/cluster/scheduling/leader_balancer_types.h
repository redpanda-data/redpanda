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

#include "container/chunked_hash_map.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

#include <roaring/roaring64map.hh>

namespace cluster::leader_balancer_types {

struct reassignment {
    raft::group_id group;
    model::broker_shard from;
    model::broker_shard to;

    reassignment(
      const raft::group_id& g,
      const model::broker_shard& f,
      const model::broker_shard& t)
      : group(g)
      , from(f)
      , to(t) {}

    reassignment() = default;
};

using index_type = chunked_hash_map<
  model::broker_shard,
  chunked_hash_map<raft::group_id, std::vector<model::broker_shard>>>;

using group_id_to_topic_revision_t
  = chunked_hash_map<raft::group_id, model::revision_id>;

using muted_groups_t = roaring::Roaring64Map;
/*
 * Leaders per shard.
 */
struct shard_load {
    model::broker_shard shard;
    size_t leaders{0};
};

class index {
public:
    virtual ~index() = default;
    virtual void update_index(const reassignment&) = 0;
};

class soft_constraint {
    virtual double evaluate_internal(const reassignment&) = 0;

public:
    virtual ~soft_constraint() = default;
    double evaluate(const reassignment& r) {
        auto ret = evaluate_internal(r);
        return ret;
    }

    virtual std::optional<reassignment> recommended_reassignment() = 0;
};

} // namespace cluster::leader_balancer_types
