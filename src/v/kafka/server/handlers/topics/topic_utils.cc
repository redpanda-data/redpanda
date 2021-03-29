// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/topic_utils.h"

#include "cluster/errc.h"
#include "cluster/metadata_cache.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"

namespace kafka {

void append_cluster_results(
  const std::vector<cluster::topic_result>& cluster_results,
  std::vector<creatable_topic_result>& kafka_results) {
    std::transform(
      cluster_results.begin(),
      cluster_results.end(),
      std::back_inserter(kafka_results),
      [](const cluster::topic_result& res) {
          return from_cluster_topic_result(res);
      });
}

ss::future<std::vector<model::node_id>> wait_for_leaders(
  cluster::metadata_cache& md_cache,
  std::vector<cluster::topic_result> results,
  model::timeout_clock::time_point timeout) {
    std::vector<ss::future<model::node_id>> futures;

    for (auto& r : results) {
        if (r.ec != cluster::errc::success) {
            continue;
        }
        // collect partitions
        auto md = md_cache.get_topic_metadata(r.tp_ns);
        if (!md) {
            // topic already deleted
            continue;
        }
        // for each partition ask for leader
        for (auto& pmd : md->partitions) {
            futures.push_back(md_cache.get_leader(
              model::ntp(r.tp_ns.ns, r.tp_ns.tp, pmd.id), timeout));
        }
    }

    return seastar::when_all_succeed(futures.begin(), futures.end());
}
} // namespace kafka
