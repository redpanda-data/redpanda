/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_sampler.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <numeric>
#include <variant>

using namespace cloud_topics;

class SamplerTestFixture
  : public l1::l1_reader_fixture
  , public cluster_test_fixture {};

TEST_F(SamplerTestFixture, TestSampler) {
    model::node_id n(0);
    create_node_application(n);
    auto& cache = get_local_cache(n);
    l1::log_sampler sampler(&_metastore, &cache);
    std::vector<std::pair<model::ntp, model::topic_id_partition>> ntidps;
    const auto topic_names = {"topic_a", "topic_b", "topic_c"};
    const auto num_topics = topic_names.size();
    for (const auto& topic : topic_names) {
        ntidps.push_back(make_ntidp(topic));
    }

    std::vector<tidp_batches_t> tidp_batches;
    l1::logs_type_t logs;
    l1::log_list_t logs_list;
    for (const auto& [ntp, tidp] : ntidps) {
        auto [it, success] = logs.emplace(
          std::make_unique<l1::log_compaction_meta>(tidp, ntp));
        logs_list.push_back(*it->get());
        create_topic(model::topic_namespace_view(ntp)).get();
        auto batches
          = model::test::make_random_batches(model::offset{0}, 10).get();
        tidp_batches.emplace_back(tidp, std::move(batches));
    }

    make_l1_objects(tidp_batches);
    auto samples = sampler.sample_logs(logs_list, num_topics).get();
    ASSERT_EQ(samples.size(), num_topics);
    for (const auto& sample : samples) {
        ASSERT_FLOAT_EQ(sample.info.dirty_ratio, 1.0);
        ASSERT_TRUE(sample.info.earliest_dirty_ts.has_value());
    }
}
