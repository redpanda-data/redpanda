/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/configuration.h"
#include "coproc/offset_storage_utils.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "model/namespace.h"
#include "storage/api.h"
#include "storage/snapshot.h"
#include "test_utils/fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <filesystem>

class offset_keeper_fixture : public coproc_test_fixture {
public:
    offset_keeper_fixture()
      : coproc_test_fixture() {
        _smk.start().get();
    }

    ~offset_keeper_fixture() override { _smk.stop().get(); }

    storage::simple_snapshot_manager& snapshot_mgr() {
        return _smk.local().snap;
    }

    template<typename Func>
    ss::future<> push_all(
      const model::topic& topic, int32_t n_partitions, Func&& make_reader_fn) {
        auto r = boost::irange<int32_t>(0, n_partitions);
        return ss::parallel_for_each(
          r, [this, topic, fn = std::forward<Func>(make_reader_fn)](int32_t i) {
              model::record_batch_reader rbr = fn();
              return produce(
                model::ntp(
                  model::kafka_namespace, topic, model::partition_id(i)),
                std::move(rbr));
          });
    };

    ss::future<> wait_on(const model::topic& topic, int32_t n_partitions) {
        auto r = boost::irange<int32_t>(0, n_partitions);
        return ss::parallel_for_each(r, [this, topic](int32_t i) {
            return consume(
                     model::ntp(
                       model::kafka_namespace, topic, model::partition_id(i)),
                     1,
                     model::offset{0})
              .discard_result();
        });
    }

private:
    /// Created for the sole use of constructing an item that has access to the
    /// shard id it was constructed on
    struct snapshot_mgr_keeper {
        snapshot_mgr_keeper()
          : snap(
            coproc::offsets_snapshot_path(),
            fmt::format(
              "{}-{}",
              storage::simple_snapshot_manager::default_snapshot_filename,
              ss::this_shard_id()),
            ss::default_priority_class()) {}
        storage::simple_snapshot_manager snap;
    };

    ss::sharded<snapshot_mgr_keeper> _smk;
};

FIXTURE_TEST(offset_keeper_saved_offsets, offset_keeper_fixture) {
    /// Setup a test environement with:
    /// 1. Two topics, w/ 50 partitions each
    /// 2. Two coprocessors each consuming from one of these two topics
    /// 3. 5, 10 batches initially pushed to these input topics
    model::topic foo("foo");
    model::topic bar("bar");
    setup({{foo, 50}, {bar, 50}}).get();
    push_all(foo, 50, []() { return make_random_batch(40); }).get();
    push_all(bar, 50, []() { return make_random_batch(40); }).get();
    using copro_typeid = coproc::registry::type_identifier;
    enable_coprocessors(
      {{.id = 4444,
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{bar, coproc::topic_ingestion_policy::stored}}}},
       {.id = 3333,
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{foo, coproc::topic_ingestion_policy::stored}}}}})
      .get();

    wait_on(
      to_materialized_topic(foo, identity_coprocessor::identity_topic), 50)
      .get();

    wait_on(
      to_materialized_topic(bar, identity_coprocessor::identity_topic), 50)
      .get();

    /// Wait until at-least one attempt to write offsets to disk was made
    tests::cooperative_spin_wait_with_timeout(5s, []() {
        return ss::file_exists(coproc::offsets_snapshot_path().string());
    }).get();

    auto r = boost::irange<unsigned int>(0, ss::smp::count);
    auto mapper = [this](unsigned int c) {
        return ss::smp::submit_to(c, [this]() {
            return coproc::recover_offsets(snapshot_mgr()).then([](auto r) {
                size_t n = 0;
                for (auto& [id, routes] : r) {
                    n += routes.size();
                }
                return n;
            });
        });
    };
    auto results
      = ss::map_reduce(r, std::move(mapper), size_t{0}, std::plus<>()).get();

    /// Created test cache with 50 partitions across 2 topics...
    BOOST_CHECK_EQUAL(results, 100);
}
