// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vassert.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/tests/health_monitor_test_utils.h"
#include "model/namespace.h"
#include "random/generators.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>

#include <limits>
#include <optional>

namespace cluster {

struct health_bench : health_report_accessor {
    using health_report_accessor::aggregated_report;

    void bench(auto aggr_fn) {
        using namespace cluster;

        constexpr int topic_count = 10;
        constexpr int parts_per_topic = 10000;
        constexpr int rf = 3;
        constexpr int nodes = 32;

        // genreate a random health report
        absl::node_hash_map<model::node_id, chunked_vector<topic_status>>
          node_topics;

        for (int topic = 0; topic < topic_count; topic++) {
            std::vector<topic_status> statuses;
            for (model::node_id node{0}; node < nodes; node++) {
                model::topic_namespace tns{
                  model::kafka_namespace,
                  model::topic(fmt::format("topic_{}", topic))};

                statuses.emplace_back(topic_status{tns, {}});
            }

            for (int pid = 0; pid < parts_per_topic; pid++) {
                for (int r = 0; r < rf; r++) {
                    auto nid = model::node_id(
                      random_generators::get_int(nodes - 1));
                    partition_status status{
                      .id{pid},
                      .leader_id = std::nullopt,
                      .under_replicated_replicas = 1,
                      .followers_stats = {{}}};
                    statuses.at(nid).partitions.emplace_back(std::move(status));
                }
            }

            for (model::node_id node{0}; node < nodes; node++) {
                node_topics[node].emplace_back(statuses.at(node));
            }
        }

        report_cache_t reports;
        for (auto& [node_id, topics] : node_topics) {
            reports[node_id]
              = ss::make_lw_shared<const cluster::node_health_report>(
                node_id,
                node::local_state{},
                std::move(topics),
                /* drain_status */ std::nullopt,
                node_liveness_report{});
        }

        perf_tests::start_measuring_time();
        auto res = aggr_fn(reports);
        perf_tests::stop_measuring_time();
    }
};

PERF_TEST_F(health_bench, current) { bench(aggregate); }

} // namespace cluster
