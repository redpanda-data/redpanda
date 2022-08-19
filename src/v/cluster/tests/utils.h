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
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "test_utils/async.h"

#include <chrono>

static const model::ns test_ns = model::ns("test-namespace");

inline cluster::partition_assignment create_test_assignment(
  const ss::sstring& topic,
  int partition_id,
  std::vector<std::pair<uint32_t, uint32_t>> shards_assignment,
  int group_id) {
    cluster::partition_assignment p_as;
    p_as.group = raft::group_id(group_id);
    p_as.id = model::partition_id(partition_id);

    std::transform(
      shards_assignment.begin(),
      shards_assignment.end(),
      std::back_inserter(p_as.replicas),
      [](const std::pair<uint32_t, uint32_t>& node_shard) {
          return model::broker_shard{
            .node_id = model::node_id(node_shard.first),
            .shard = node_shard.second,
          };
      });
    return p_as;
}

using batches_t = ss::circular_buffer<model::record_batch>;
using batches_ptr_t = ss::lw_shared_ptr<batches_t>;
using foreign_batches_t = ss::foreign_ptr<batches_ptr_t>;

inline void wait_for_metadata(
  cluster::topic_table& topic_table,
  const std::vector<cluster::topic_result>& results,
  std::chrono::milliseconds tout = std::chrono::seconds(5)) {
    tests::cooperative_spin_wait_with_timeout(tout, [&results, &topic_table] {
        return std::all_of(
          results.begin(),
          results.end(),
          [&topic_table](const cluster::topic_result& r) {
              return topic_table.get_topic_metadata(r.tp_ns);
          });
    }).get0();
}

inline bool
are_batches_the_same(const foreign_batches_t& a, const foreign_batches_t& b) {
    if (a->size() != b->size()) {
        return false;
    }

    auto p = std::mismatch(
      a->begin(),
      a->end(),
      b->begin(),
      b->end(),
      [](
        const model::record_batch& a_batch,
        const model::record_batch& b_batch) { return a_batch == b_batch; });

    return p.first == a->end() && p.second == b->end();
}
