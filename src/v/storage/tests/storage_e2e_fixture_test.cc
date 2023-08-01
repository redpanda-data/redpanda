// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/tests/produce_consume_utils.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/lowres_clock.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>

using namespace std::chrono_literals;
using std::vector;

struct storage_e2e_fixture : public redpanda_thread_fixture {};

namespace {

// Produces to the given fixture's partition for 10 seconds.
ss::future<> produce_to_fixture(
  storage_e2e_fixture* fix, model::topic topic_name, int* incomplete) {
    tests::kafka_produce_transport producer(co_await fix->make_kafka_client());
    co_await producer.start();
    const int cardinality = 10;
    auto now = ss::lowres_clock::now();
    while (ss::lowres_clock::now() < now + 5s) {
        for (int i = 0; i < cardinality; i++) {
            co_await producer.produce_to_partition(
              topic_name, model::partition_id(0), tests::kv_t::sequence(i, 1));
        }
    }
    *incomplete -= 1;
}
} // namespace

FIXTURE_TEST(test_compaction_segment_ms, storage_e2e_fixture) {
    config::shard_local_cfg().log_segment_ms_min.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1ms));
    const auto topic_name = model::topic("tapioca");
    const auto ntp = model::ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.retention_duration = tristate<std::chrono::milliseconds>(
      tristate<std::chrono::milliseconds>{});
    props.segment_ms = tristate<std::chrono::milliseconds>(1s);
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    stress_config cfg;
    cfg.min_spins_per_scheduling_point = 100;
    cfg.max_spins_per_scheduling_point = 10000;
    cfg.num_fibers = 100;
    BOOST_REQUIRE(app.stress_fiber_manager.local().start(cfg));

    std::vector<ss::future<>> produces;
    produces.reserve(5);
    int incomplete = 5;
    for (int i = 0; i < 5; i++) {
        auto fut = produce_to_fixture(this, topic_name, &incomplete);
        produces.emplace_back(std::move(fut));
    }
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(partition->log().get());

    while (incomplete > 0) {
        log->apply_segment_ms().get();
        ss::sleep(500ms).get();
    }

    for (auto&& p : produces) {
        std::move(p).get();
    }
    app.stress_fiber_manager.local().stop().get();
}
