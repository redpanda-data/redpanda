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

#include <seastar/core/sharded.hh>

#include <chrono>

static const model::ns test_ns = model::ns("test-namespace");

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
    }).get();
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
