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

#include "container/fragmented_vector.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/sync_group.h"
#include "model/fundamental.h"

#include <absl/container/flat_hash_map.h>

namespace kafka::client {

using assignment
  = absl::flat_hash_map<model::topic, std::vector<model::partition_id>>;
using assignments = absl::flat_hash_map<member_id, assignment>;

/// \brief Assignment plans are used by the consumer group leader to distribute
/// topic partitions amongst consumers during sync.
struct assignment_plan {
    assignment_plan() = default;
    assignment_plan(const assignment_plan&) = delete;
    assignment_plan(assignment_plan&&) = delete;
    assignment_plan& operator=(const assignment_plan&) = delete;
    assignment_plan& operator=(assignment_plan&&) = delete;
    virtual ~assignment_plan() = default;

    virtual assignments plan(
      const chunked_vector<member_id>& members,
      const chunked_vector<metadata_response::topic>& topics)
      = 0;

    sync_group_request_assignment
    encode(const assignments::value_type& m) const;

    chunked_vector<sync_group_request_assignment>
    encode(const assignments& assignments) const;

    assignment decode(const bytes& b) const;
};

std::unique_ptr<assignment_plan>
make_assignment_plan(const protocol_name& protocol_name);

/// \brief The range assigner is the default assignment plan.
struct assignment_range final : public assignment_plan {
    static inline const protocol_name name{"range"};
    assignments plan(
      const chunked_vector<member_id>& members,
      const chunked_vector<metadata_response::topic>& topics) final;
};

join_group_request_protocol
make_join_group_request_protocol_range(const std::vector<model::topic>& topics);

chunked_vector<join_group_request_protocol>
make_join_group_request_protocols(const chunked_vector<model::topic>& topics);

} // namespace kafka::client
