// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/disk_log_impl.h"
#include "storage/segment.h"
#include "storage/tests/storage_e2e_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

namespace {
ss::future<> force_roll_log(storage::disk_log_impl* log) {
    try {
        co_await log->force_roll(ss::default_priority_class());
    } catch (...) {
    }
}

} // namespace

FIXTURE_TEST(test_compaction_segment_ms, storage_e2e_fixture) {
    test_local_cfg.get("log_segment_ms_min")
      .set_value(std::chrono::duration_cast<std::chrono::milliseconds>(1ms));
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
        auto fut = produce_to_fixture(topic_name, &incomplete);
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

FIXTURE_TEST(test_concurrent_log_eviction_and_append, storage_e2e_fixture) {
    const auto topic_name = model::topic("tapioca");
    const auto ntp = model::ntp(model::kafka_namespace, topic_name, 0);
    // We will be running housekeeping locally in a loop.
    test_local_cfg.get("log_disable_housekeeping_for_tests").set_value(true);
    cluster::topic_properties props;
    // Try to ensure we stay at 1 segment during our eviction/appending loop.
    props.segment_size = 1_MiB;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(partition->log().get());

    ss::abort_source as;

    // Config is set up to always garbage collect as much as possible.
    storage::housekeeping_config cfg(
      /*gc_upper=*/model::timestamp::max(),
      /*max_bytes_in_log=*/1,
      /*max_collect_offset=*/model::offset::min(),
      /*tombstone_retention_ms=*/std::nullopt,
      ss::default_priority_class(),
      as);

    const auto num_records = 100;
    model::offset produce_base_offset{0};
    model::offset stop_after{num_records * 100};
    tests::kafka_produce_transport producer(make_kafka_client().get());
    producer.start().get();
    auto produce = [&] {
        return producer
          .produce_to_partition(
            topic_name,
            model::partition_id(0),
            tests::kv_t::sequence(produce_base_offset(), num_records))
          .then([&](model::offset base_offset) {
              BOOST_REQUIRE_EQUAL(base_offset, produce_base_offset);
              produce_base_offset += model::offset{num_records};
              if (produce_base_offset >= stop_after) {
                  as.request_abort();
              }
          })
          .then([] {
              return ss::sleep(
                std::chrono::milliseconds(random_generators::get_int(10, 100)));
          });
    };

    auto read = [&] {
        auto lstats = log->offsets();
        return log
          ->make_reader(storage::log_reader_config(
            lstats.start_offset,
            model::offset::max(),
            ss::default_priority_class()))
          .then([](auto reader) {
              return ss::sleep(std::chrono::milliseconds(
                                 random_generators::get_int(15, 30)))
                .then([r = std::move(reader)]() mutable {
                    return model::consume_reader_to_memory(
                      std::move(r), model::no_timeout);
                })
                .then([](ss::circular_buffer<model::record_batch> batches) {
                    model::offset prev_base_offset{model::offset::min()};
                    for (const auto& batch : batches) {
                        BOOST_REQUIRE_GT(batch.base_offset(), prev_base_offset);
                        prev_base_offset = batch.base_offset();
                    }
                });
          });
    };

    auto gc = [&] { return log->housekeeping(cfg); };

    // Concurrent operations
    auto produce_fut = ss::do_until(
      [&] { return as.abort_requested(); }, [&] { return produce(); });
    auto read_fut = ss::do_until(
      [&] { return as.abort_requested(); }, [&] { return read(); });
    auto gc_fut = ss::do_until(
      [&] { return as.abort_requested(); }, [&] { return gc(); });

    ss::when_all(std::move(produce_fut), std::move(read_fut), std::move(gc_fut))
      .get();

    // log_eviction_stm may have removed the active segment after we finished
    // final round of eviction.
    BOOST_REQUIRE_LE(log->segment_count(), 1);
}

FIXTURE_TEST(test_concurrent_segment_roll_and_close, storage_e2e_fixture) {
    const auto topic_name = model::topic("tapioca");
    const auto ntp = model::ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(partition->log().get());
    auto seg = log->segments().back();

    // Hold a read lock, which will force release_appender() to go through
    // release_appender_in_background()
    auto read_lock_holder = seg->read_lock().get();

    auto roll_fut = force_roll_log(log);
    auto release_holder_fut = ss::sleep(100ms).then(
      [read_locker_holder = std::move(read_lock_holder)] {});
    auto remove_segment_fut = remove_segment_permanently(log, seg);

    ss::when_all(
      std::move(roll_fut),
      std::move(remove_segment_fut),
      std::move(release_holder_fut))
      .get();
}
