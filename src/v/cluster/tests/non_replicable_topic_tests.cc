/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/metadata_cache.h"
#include "cluster/non_replicable_partition_manager.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/metadata.h"
#include "storage/tests/utils/random_batch.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals; // NOLINT

namespace {
class sleep_once_consumer {
public:
    explicit sleep_once_consumer(model::timeout_clock::duration d) noexcept
      : _timeout(d) {}

    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        if (_first_run) {
            _first_run = false;
            _started.set_value();
            co_await ss::sleep(_timeout);
        }
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    /// Used to know when actual consumption will begin
    ss::future<> started() { return _started.get_future(); }

private:
    bool _first_run{true};
    ss::promise<> _started;
    model::timeout_clock::duration _timeout;
};

model::topic_namespace to_tpns(const model::ntp& ntp) {
    return model::topic_namespace(model::topic_namespace_view{ntp});
}

template<typename T>
ss::future<bool>
partition_exists(ss::sharded<T>& pm, ss::shard_id shard, model::ntp ntp) {
    return pm.invoke_on(
      shard, [ntp](T& local_pm) { return (bool)local_pm.get(ntp); });
}

ss::future<model::timeout_clock::duration> timed_delete_partition(
  model::timeout_clock::duration timeout,
  cluster::topics_frontend& topics_frontend,
  cluster::non_replicable_partition_manager& local,
  model::ntp tp2_ntp) {
    auto partition = local.get(tp2_ntp);
    BOOST_REQUIRE(partition);
    /// Begin by writing some sample data into the partition/log
    auto batches = storage::test::make_random_batches(model::offset{0});
    const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    auto rbr = model::make_memory_record_batch_reader(std::move(batches));
    auto rs = co_await std::move(rbr).for_each_ref(
      partition->make_appender(write_cfg), model::no_timeout);
    auto reader_cfg = storage::log_reader_config(
      model::offset{0},
      model::model_limits<model::offset>::max(),
      ss::default_priority_class());
    rbr = co_await partition->make_reader(reader_cfg);

    /// Start the tests by consuming the pushed data, using the
    /// sleep_once_consumer to hold up consumption for at least some known
    /// duration
    auto soc = sleep_once_consumer(timeout);
    auto start_consume = soc.started();
    auto finished = std::move(rbr).consume(std::move(soc), model::no_timeout);
    /// To avoid deletion occuring before the stream actually started, wait on
    /// the 'start_consume' future to known when consume has started
    co_await std::move(start_consume);

    /// Attempt to close while consume is in progress. Expected behavior
    /// should be no crashes, and the close taking at least as long as the
    /// sleep_once_consumers sleep.
    auto start = model::timeout_clock::now();
    co_await local.remove(tp2_ntp);
    auto end = model::timeout_clock::now();

    /// Wait on end of stream, even though by this time it has resolved, this
    /// future must be waited on to prevent stranded future exceptions
    co_await std::move(finished);
    co_return(end - start);
}

} // namespace

/// Tests that streams via non_replicated_partitions can be properly aborted
FIXTURE_TEST(test_non_replicated_shutdown, cluster_test_fixture) {
    model::node_id id{0};
    auto app = create_node_application(id);
    wait_for_controller_leadership(id).get();
    wait_for_all_members(3s).get();
    auto* fixture = get_node_fixture(id);

    /// Test has 2 partitions, 1 normal, 1 non_replicable
    model::ntp tp_ntp = model::ntp(
      model::kafka_namespace, model::topic("abc"), model::partition_id(0));
    model::ntp tp2_ntp = model::ntp(
      model::kafka_namespace, model::topic("def"), model::partition_id(0));

    /// Create underlying topics
    fixture->add_topic(to_tpns(tp_ntp)).get();
    fixture->add_non_replicable_topic(to_tpns(tp_ntp), to_tpns(tp2_ntp)).get();

    /// Wait until the partitions are created
    std::optional<ss::shard_id> home_shard;
    tests::cooperative_spin_wait_with_timeout(
      3s,
      [fixture, tp2_ntp, &home_shard]() mutable {
          home_shard = fixture->app.shard_table.local().shard_for(tp2_ntp);
          return home_shard.has_value();
      })
      .get();
    BOOST_REQUIRE(home_shard);
    info("tp_ntp shard: {}", *home_shard);

    /// Delete partition while its in use, assert that the configured time has
    /// passed between the two calls.
    auto& nr_pm = fixture->app.nr_partition_manager;
    auto delete_time
      = nr_pm
          .invoke_on(
            *home_shard,
            [fixture,
             tp2_ntp](cluster::non_replicable_partition_manager& local) {
                return timed_delete_partition(
                  1s,
                  fixture->app.controller->get_topics_frontend().local(),
                  local,
                  tp2_ntp);
            })
          .get();

    /// Assert that the deletion did occur...
    bool exists = partition_exists(nr_pm, *home_shard, tp2_ntp).get();
    BOOST_REQUIRE(!exists);

    /// ... and that the total time took included the sleep_once_consumers sleep
    /// duration
    BOOST_REQUIRE(delete_time >= 1s);
}
