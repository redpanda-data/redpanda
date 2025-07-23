// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/shard_placement_table.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/loop.hh>

class remake_partition_fixture : public redpanda_thread_fixture {
public:
    void wait_for_remake(const model::ntp& ntp) {
        auto& controller = *app.controller;
        tests::cooperative_spin_wait_with_timeout(2s, [&controller, &ntp] {
            auto& spt = controller.get_shard_placement_table().local();
            auto state = spt.state_on_this_shard(ntp);
            auto group = state->assigned()->group;

            auto& kvstore = controller.get_storage().local().kvs();
            auto marker_opt = kvstore.get(
              storage::kvstore::key_space::shard_placement,
              cluster::current_state_kvstore_key(group));
            if (!marker_opt.has_value()) {
                return false;
            }
            auto marker = serde::from_iobuf<cluster::current_state_marker>(
              std::move(*marker_opt));
            if (
              marker.remake_state
              != cluster::shard_placement_table::remake_partition_state::none) {
                return false;
            }

            if (spt.get_probe()->remade_partitions() == 0) {
                return false;
            }
            auto p = controller.get_partition_manager().local().get(ntp);
            if (!p) {
                return false;
            }
            return true;
        }).get();
    }

    ss::future<> produce(
      storage::log* log,
      size_t num_segments,
      size_t cardinality,
      size_t batches_per_segment,
      size_t records_per_batch = 1,
      size_t starting_value = 0) {
        tests::kafka_produce_transport producer(co_await make_kafka_client());
        co_await producer.start();

        // Generate some segments.
        size_t val_count = starting_value;
        for (size_t i = 0; i < num_segments; i++) {
            for (size_t r = 0; r < batches_per_segment; r++) {
                auto kvs = tests::kv_t::sequence(
                  val_count, records_per_batch, val_count, cardinality);
                co_await producer.produce_to_partition(
                  topic_name, model::partition_id(0), std::move(kvs));
                val_count += records_per_batch;
            }
            co_await log->flush();
            co_await log->force_roll();
        }
    }

    ss::future<std::vector<tests::kv_t>> consume() {
        tests::kafka_consume_transport consumer(co_await make_kafka_client());
        co_await consumer.start();
        auto consumed_kvs = co_await consumer.consume_from_partition(
          test_ntp.tp.topic, test_ntp.tp.partition, model::offset(0));
        co_return consumed_kvs;
    }

protected:
    const model::topic topic_name{"remake_partition_test_topic"};
    const model::ntp test_ntp{
      model::kafka_namespace, topic_name, model::partition_id(0)};
};

FIXTURE_TEST(remake_partition_test, remake_partition_fixture) {
    auto* controller = app.controller.get();

    // create topic
    add_topic(model::topic_namespace_view{test_ntp}).get();
    wait_for_leader(test_ntp).get();

    auto ec = controller->get_api().local().remake_partition(test_ntp).get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);

    // Wait till partition is recreated
    wait_for_remake(test_ntp);
}

FIXTURE_TEST(
  remake_partition_after_shutdown_initiated_test, remake_partition_fixture) {
    // Mocks a situation where the kvstore starts with a partition flagged with
    // remake_partition_state::initiated.

    auto* controller = app.controller.get();

    // create topic
    add_topic(model::topic_namespace_view{test_ntp}).get();
    wait_for_leader(test_ntp).get();

    auto& spt = controller->get_shard_placement_table();
    auto& kvstore = controller->get_storage().local().kvs();

    auto state = spt.local().state_on_this_shard(test_ntp);
    auto log_revision = state->assigned()->log_revision;
    auto shard_revision = state->assigned()->shard_revision;
    auto group = state->assigned()->group;

    auto marker_buf = serde::to_iobuf(cluster::current_state_marker{
      .ntp = test_ntp,
      .log_revision = log_revision,
      .shard_revision = shard_revision,
      .is_complete = true,
      .remake_state
      = cluster::shard_placement_table::remake_partition_state::initiated});
    kvstore
      .put(
        storage::kvstore::key_space::shard_placement,
        cluster::current_state_kvstore_key(group),
        std::move(marker_buf))
      .get();

    restart(redpanda_thread_fixture::should_wipe::no);

    // Wait till partition is recreated
    wait_for_remake(test_ntp);
}

FIXTURE_TEST(
  remake_partition_after_shutdown_deleted_test, remake_partition_fixture) {
    // Mocks a situation where the kvstore starts with a partition flagged with
    // remake_partition_state::deleted.

    auto* controller = app.controller.get();

    // create topic
    add_topic(model::topic_namespace_view{test_ntp}).get();
    wait_for_leader(test_ntp).get();

    auto& spt = controller->get_shard_placement_table();
    auto& kvstore = controller->get_storage().local().kvs();

    auto state = spt.local().state_on_this_shard(test_ntp);
    auto log_revision = state->assigned()->log_revision;
    auto shard_revision = state->assigned()->shard_revision;
    auto group = state->assigned()->group;

    auto marker_buf = serde::to_iobuf(cluster::current_state_marker{
      .ntp = test_ntp,
      .log_revision = log_revision,
      .shard_revision = shard_revision,
      .is_complete = true,
      .remake_state
      = cluster::shard_placement_table::remake_partition_state::deleted});
    kvstore
      .put(
        storage::kvstore::key_space::shard_placement,
        cluster::current_state_kvstore_key(group),
        std::move(marker_buf))
      .get();

    // Manually perform removal of state
    app.partition_manager.local()
      .remove(test_ntp, cluster::partition_removal_mode::local_only)
      .get();
    cluster::remove_persistent_state(test_ntp, group, kvstore).get();

    restart(redpanda_thread_fixture::should_wipe::no);

    // Wait till partition is recreated
    wait_for_remake(test_ntp);
}

FIXTURE_TEST(remake_partition_with_produce_test, remake_partition_fixture) {
    auto* controller = app.controller.get();

    // create topic
    add_topic(model::topic_namespace_view{test_ntp}).get();
    wait_for_leader(test_ntp).get();

    auto num_segments = 10;
    auto cardinality = 100;
    auto batches_per_segment = 10;
    auto records_per_batch = 5;
    auto total_num_records = num_segments * batches_per_segment
                             * records_per_batch;

    auto p = app.partition_manager.local().get(test_ntp).get();
    auto log = p->log().get();
    produce(
      log, num_segments, cardinality, batches_per_segment, records_per_batch)
      .get();

    {
        auto records = consume().get();
        BOOST_REQUIRE_EQUAL(records.size(), total_num_records);
    }

    auto ec = controller->get_api().local().remake_partition(test_ntp).get();
    BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);

    // Wait till partition is recreated
    wait_for_remake(test_ntp);
    wait_for_leader(test_ntp).get();

    {
        auto records = consume().get();
        BOOST_REQUIRE_EQUAL(records.size(), 0);
    }
}
