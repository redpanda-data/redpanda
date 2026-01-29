/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_info_collector.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <numeric>
#include <variant>

using namespace cloud_topics;

class LogInfoCollectorTestFixture : public l1::l1_reader_fixture {};

// A fake topic config provider which always returns a value.
class fake_cfg_provider : public l1::topic_cfg_provider {
public:
    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
    get_topic_cfg(model::topic_namespace_view) const final {
        return _cfg;
    }

private:
    cluster::topic_configuration _cfg{};
};

// A fake offset provider which always returns kafka::offset::max().
class fake_offset_provider : public l1::max_compactible_offset_provider {
public:
    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final {
        co_return;
    }
};

TEST_F(LogInfoCollectorTestFixture, TestInfoCollector) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));
    std::vector<std::pair<model::ntp, model::topic_id_partition>> ntidps;
    const auto topic_names = {"topic_a", "topic_b", "topic_c"};
    const auto num_topics = topic_names.size();
    for (const auto& topic : topic_names) {
        ntidps.push_back(make_ntidp(topic));
    }

    std::vector<tidp_batches_t> tidp_batches;
    l1::log_set_t logs;
    l1::log_compaction_queue cached_metadata(
      [](
        const l1::log_compaction_meta_ptr& a,
        const l1::log_compaction_meta_ptr& b) { return a->ntp < b->ntp; });
    l1::log_list_t logs_list;
    for (const auto& [ntp, tidp] : ntidps) {
        auto [it, success] = logs.emplace(
          ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp));
        logs_list.push_back(*it->get());
        auto batches
          = model::test::make_random_batches(model::offset{0}, 10).get();
        tidp_batches.emplace_back(tidp, std::move(batches));
    }

    make_l1_objects(std::move(tidp_batches)).get();
    log_info_collector.collect_info_for_logs(logs, logs_list, cached_metadata)
      .get();
    ASSERT_EQ(cached_metadata.size(), num_topics);
    while (!cached_metadata.empty()) {
        auto sample = cached_metadata.top();
        cached_metadata.pop();
        ASSERT_TRUE(sample->info_and_ts.has_value());
        ASSERT_FLOAT_EQ(sample->info_and_ts->info.dirty_ratio, 1.0);
        ASSERT_TRUE(sample->info_and_ts->info.earliest_dirty_ts.has_value());
    }
}
