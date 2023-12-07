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
#include "test_utils/scoped_config.h"

#include <seastar/core/lowres_clock.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>

using namespace std::chrono_literals;
using std::vector;

struct storage_e2e_fixture : public redpanda_thread_fixture {
    scoped_config test_local_cfg;
};

namespace {

// Produces to the given fixture's partition for 10 seconds.
ss::future<> produce_to_fixture(
  storage_e2e_fixture* fix, model::topic topic_name, int* complete) {
    tests::kafka_produce_transport producer(co_await fix->make_kafka_client());
    co_await producer.start();
    const int cardinality = 1;
    auto now = ss::lowres_clock::now();
    while (ss::lowres_clock::now() < now + 30s) {
        for (int i = 0; i < cardinality; i++) {
            co_await producer.produce_to_partition(
              topic_name, model::partition_id(0), tests::kv_t::sequence(i, 1));
        }
    }
    *complete += 1;
}
} // namespace

FIXTURE_TEST(test_compaction_segment_ms, storage_e2e_fixture) {
    test_local_cfg.get("log_segment_ms_min")
      .set_value(std::chrono::duration_cast<std::chrono::milliseconds>(1ms));
    const auto topic_name = model::topic("tapioca");
    const auto ntp = model::ntp(model::kafka_namespace, topic_name, 0);

    auto props = cluster::topic_properties{};
    props.retention_duration = tristate<std::chrono::milliseconds>(10s);
    props.retention_local_target_ms = tristate<std::chrono::milliseconds>(10s);
    props.segment_ms = tristate<std::chrono::milliseconds>(1s);

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    BOOST_REQUIRE(app.stress_fiber_manager.local().start(
      {.min_spins_per_scheduling_point = 100,
       .max_spins_per_scheduling_point = 10000,
       .num_fibers = 100}));

    constexpr auto concurrent_producers = 5;
    std::vector<ss::future<>> produces;
    produces.reserve(concurrent_producers);
    auto complete = 0;
    for (int i = 0; i < concurrent_producers; i++) {
        produces.emplace_back(produce_to_fixture(this, topic_name, &complete));
    }
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(partition->log().get());

    auto dummy_as = ss::abort_source{};
    auto hash_key_offset_map = storage::hash_key_offset_map{};
    hash_key_offset_map.initialize(10_MiB).get();
    while (complete < concurrent_producers) {
        log->apply_segment_ms().get();
        log
          ->housekeeping(storage::housekeeping_config{
            model::timestamp(
              model::timestamp::now().value()
              - props.retention_duration.value().count()),
            std::nullopt,
            log->stm_manager()->max_collectible_offset(),
            ss::default_priority_class(),
            dummy_as,
            std::nullopt,
            &hash_key_offset_map,
          })
          .get();
        ss::sleep(500ms).get();
    }

    for (auto& p : produces) {
        p.get();
    }
    app.stress_fiber_manager.local().stop().get();
}
