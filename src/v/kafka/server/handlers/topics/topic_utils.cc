// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/topic_utils.h"

#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/metadata_cache.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>

namespace kafka {

void append_cluster_results(
  const std::vector<cluster::topic_result>& cluster_results,
  chunked_vector<creatable_topic_result>& kafka_results) {
    std::transform(
      cluster_results.begin(),
      cluster_results.end(),
      std::back_inserter(kafka_results),
      [](const cluster::topic_result& res) {
          return from_cluster_topic_result(res);
      });
}

ss::future<> wait_for_leaders(
  cluster::metadata_cache& md_cache,
  std::vector<cluster::topic_result> results,
  model::timeout_clock::time_point deadline) {
    for (auto& r : results) {
        if (r.ec != cluster::errc::success) {
            continue;
        }
        // collect partitions
        auto md = md_cache.get_topic_metadata_ref(r.tp_ns);
        if (!md) {
            // topic already deleted
            continue;
        }
        co_await ss::max_concurrent_for_each(
          md->get().get_assignments(),
          64,
          [&r, &md_cache, deadline](const auto& p_as) {
              return md_cache
                .get_leader(
                  model::ntp(r.tp_ns.ns, r.tp_ns.tp, p_as.second.id), deadline)
                .discard_result();
          });
    }
}

ss::future<> wait_for_topics(
  cluster::metadata_cache& md_cache,
  std::vector<cluster::topic_result> results,
  cluster::controller_api& api,
  model::timeout_clock::time_point timeout) {
    return ss::do_with(
      std::move(results),
      [&md_cache, &api, timeout](std::vector<cluster::topic_result>& results) {
          return ss::max_concurrent_for_each(
                   results,
                   5,
                   [&api, timeout](cluster::topic_result& r) {
                       if (r.ec != cluster::errc::success) {
                           return ss::now();
                       }
                       // we discard return here, we do not want to return error
                       // even if waiting for topic wasn't successfull, it was
                       // already created
                       return api.wait_for_topic(r.tp_ns, timeout)
                         .discard_result();
                   })
            .then([&md_cache, &results, timeout]() {
                return wait_for_leaders(md_cache, results, timeout)
                  .discard_result()
                  .handle_exception_type([](const ss::timed_out_error&) {
                      // discard timed out exception, even tho waiting failed
                      // the topic is created
                  });
            });
      });
}
} // namespace kafka
