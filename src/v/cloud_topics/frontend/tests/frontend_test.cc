/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/app.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/tests/random_batch.h"
#include "raft/replicate.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/future.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <expected>

static ss::logger e2e_test_log("e2e_test");
using namespace cloud_topics;
using namespace testing;

class mock_api : public data_plane_api {
public:
    MOCK_METHOD(
      (ss::future<std::expected<staged_write, std::error_code>>),
      stage_write,
      (chunked_vector<model::record_batch>),
      (override));

    MOCK_METHOD(
      (ss::future<std::expected<upload_meta, std::error_code>>),
      execute_write,
      (model::ntp,
       cluster_epoch,
       staged_write,
       model::timeout_clock::time_point),
      (override));

    MOCK_METHOD(
      ss::future<result<chunked_vector<model::record_batch>>>,
      materialize,
      (model::ntp ntp,
       size_t output_size_estimate,
       chunked_vector<extent_meta> metadata,
       model::timeout_clock::time_point timeout,
       model::opt_abort_source_t,
       allow_materialization_failure allow_mat_failure),
      (override));

    MOCK_METHOD(
      void,
      cache_put,
      (const model::topic_id_partition&, const model::record_batch&),
      (override));

    MOCK_METHOD(
      std::optional<model::record_batch>,
      cache_get,
      (const model::topic_id_partition&, model::offset o),
      (override));

    MOCK_METHOD(
      void,
      cache_put_ordered,
      (const model::topic_id_partition&, chunked_vector<model::record_batch>),
      (override));

    MOCK_METHOD(
      ss::future<>,
      cache_wait,
      (const model::topic_id_partition&,
       model::offset,
       model::offset,
       model::timeout_clock::time_point,
       std::optional<std::reference_wrapper<ss::abort_source>>),
      (override));

    MOCK_METHOD(size_t, materialize_max_bytes, (), (const, override));

    MOCK_METHOD(
      ss::future<std::optional<cloud_topics::cluster_epoch>>,
      get_current_epoch,
      (ss::abort_source*),
      (noexcept, override));

    MOCK_METHOD(
      ss::future<>,
      invalidate_epoch_below,
      (cloud_topics::cluster_epoch),
      (noexcept, override));

    MOCK_METHOD(ss::future<>, start, (), (override));

    MOCK_METHOD(ss::future<>, stop, (), (override));
};

auto make_extent_fut(model::offset o, cluster_epoch epoch) {
    extent_meta m{
      .id = object_id::create(epoch),
      .first_byte_offset = first_byte_offset_t{0},
      .byte_range_size = byte_range_size_t{0},
      .base_offset = model::offset_cast(o),
      .last_offset = model::offset_cast(o)};

    chunked_vector<extent_meta> vec;
    vec.push_back(std::move(m));
    return ss::make_ready_future<std::expected<upload_meta, std::error_code>>(
      upload_meta{.shard = ss::this_shard_id(), .extents = std::move(vec)});
}

class frontend_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public Test {
public:
    frontend_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        _data_plane = ss::make_shared<mock_api>();
    }

    scoped_config test_local_cfg;
    ss::shared_ptr<mock_api> _data_plane;
};

TEST_F(frontend_fixture, test_replicate_epoch) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();

    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    ON_CALL(*_data_plane, cache_put_ordered(_, _))
      .WillByDefault([](const auto&, auto) {});
    EXPECT_CALL(*_data_plane, cache_put_ordered(_, _)).Times(2);
    using stage_result = std::expected<staged_write, std::error_code>;
    EXPECT_CALL(*_data_plane, stage_write(_))
      .WillOnce(Return(ss::as_ready_future(stage_result{})))
      .WillOnce(Return(ss::as_ready_future(stage_result{})))
      .WillOnce(Return(ss::as_ready_future(stage_result{})));
    EXPECT_CALL(*_data_plane, execute_write(_, _, _, _))
      .WillOnce(Return(make_extent_fut(model::offset(0), cluster_epoch(1))))
      .WillOnce(Return(make_extent_fut(model::offset(1), cluster_epoch(2))))
      .WillOnce(Return(make_extent_fut(model::offset(2), cluster_epoch(0))));

    {
        // First batch with offset 0 (epoch 1)
        auto batch = model::test::make_random_batch(model::offset{1}, false);
        chunked_vector<model::record_batch> buf;
        buf.push_back(std::move(batch));
        auto res = frontend
                     .replicate(
                       std::move(buf),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        ASSERT_TRUE(res.has_value());
    }

    {
        // First batch with offset 1 (epoch 2)
        auto batch = model::test::make_random_batch(model::offset{2}, false);
        chunked_vector<model::record_batch> buf;
        buf.push_back(std::move(batch));
        auto res = frontend
                     .replicate(
                       std::move(buf),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        ASSERT_TRUE(res.has_value());
    }

    {
        // First batch with offset 2 (epoch 0, breaks invariant)
        auto batch = model::test::make_random_batch(model::offset{2}, false);
        chunked_vector<model::record_batch> buf;
        buf.push_back(std::move(batch));
        auto res = frontend
                     .replicate(
                       std::move(buf),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack))
                     .get();
        ASSERT_FALSE(res.has_value());
    }
}

TEST_F(frontend_fixture, test_replicate_invalidates_epoch_cache) {
    const model::topic topic_name("epoch_invalidate");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    // Advance epoch twice to establish previous_applied_epoch = 5.
    auto r1 = frontend.advance_epoch(cluster_epoch(5), model::no_timeout).get();
    ASSERT_TRUE(r1.has_value());
    auto r2
      = frontend.advance_epoch(cluster_epoch(10), model::no_timeout).get();
    ASSERT_TRUE(r2.has_value());

    using stage_result = std::expected<staged_write, std::error_code>;
    EXPECT_CALL(*_data_plane, stage_write(_))
      .WillOnce(Return(ss::as_ready_future(stage_result{})));
    EXPECT_CALL(*_data_plane, execute_write(_, _, _, _))
      .WillOnce(Return(make_extent_fut(model::offset(0), cluster_epoch(7))));
    EXPECT_CALL(*_data_plane, invalidate_epoch_below(cluster_epoch(10)))
      .WillOnce(Return(ss::now()));
    ON_CALL(*_data_plane, cache_put_ordered(_, _))
      .WillByDefault([](const auto&, auto) {});

    auto batch = model::test::make_random_batch(model::offset{0}, false);
    model::batch_identity batch_id{
      .pid = model::producer_identity{1, 0},
      .first_seq = 0,
      .last_seq = 0,
      .record_count = batch.record_count(),
      .max_timestamp = batch.header().max_timestamp,
      .is_transactional = false,
    };

    auto stages = frontend.replicate(
      batch_id,
      std::move(batch),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    stages.request_enqueued.get();
    auto result = stages.replicate_finished.get();
    ASSERT_TRUE(result.has_value());
}

TEST_F(frontend_fixture, test_tiered_cloud_replicate_skips_l0_upload) {
    const model::topic topic_name("tiered_cloud_topic");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::tiered_cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    // stage_write and execute_write should NOT be called for tiered_cloud
    EXPECT_CALL(*_data_plane, stage_write(_)).Times(0);
    EXPECT_CALL(*_data_plane, execute_write(_, _, _, _)).Times(0);

    auto batch = model::test::make_random_batch(model::offset{0}, false);
    chunked_vector<model::record_batch> buf;
    buf.push_back(std::move(batch));
    auto res = frontend
                 .replicate(
                   std::move(buf),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();

    ASSERT_TRUE(res.has_value());
}

TEST_F(frontend_fixture, test_tiered_cloud_replicate_stages_skips_l0_upload) {
    const model::topic topic_name("tiered_cloud_topic2");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::tiered_cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    // stage_write and execute_write should NOT be called
    EXPECT_CALL(*_data_plane, stage_write(_)).Times(0);
    EXPECT_CALL(*_data_plane, execute_write(_, _, _, _)).Times(0);

    auto batch = model::test::make_random_batch(model::offset{0}, false);
    auto stages = frontend.replicate(
      model::batch_identity{},
      std::move(batch),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    auto res = stages.replicate_finished.get();
    ASSERT_TRUE(res.has_value());
}

TEST_F(frontend_fixture, test_advance_epoch) {
    // This test verifies that frontend::advance_epoch() correctly integrates
    // with the underlying ctp_stm_api to advance the partition's epoch and
    // return consistent epoch_info.
    const model::topic topic_name("advance_epoch_test");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    // Initially, get_epoch_info should return min epochs (no data yet)
    auto initial_info = frontend.get_epoch_info();
    EXPECT_EQ(initial_info.max_applied_epoch, cluster_epoch::min());
    EXPECT_EQ(initial_info.estimated_inactive_epoch, cluster_epoch::min());

    // Call advance_epoch to establish epoch 5
    auto first_advance
      = frontend.advance_epoch(cluster_epoch(5), model::no_timeout).get();
    ASSERT_TRUE(first_advance.has_value())
      << "first advance_epoch should succeed";
    EXPECT_EQ(first_advance.value().max_applied_epoch, cluster_epoch(5));
    // first advance call reflects the new epoch right away because the stm
    // state was empty
    EXPECT_EQ(first_advance.value().estimated_inactive_epoch, cluster_epoch(4));

    // Call advance_epoch with a higher epoch (10)
    auto advance_result
      = frontend.advance_epoch(cluster_epoch(10), model::no_timeout).get();
    ASSERT_TRUE(advance_result.has_value())
      << "advance_epoch should succeed on leader";

    auto epoch_info = advance_result.value();
    // max_applied_epoch should now be 10, inactive epoch is 4 because
    // prev_applied_epoch is 5
    EXPECT_EQ(epoch_info.max_applied_epoch, cluster_epoch(10));
    EXPECT_EQ(epoch_info.estimated_inactive_epoch, cluster_epoch(4));

    // advance the epoch one more time to observe lower bound sync behavior

    auto final_result
      = frontend.advance_epoch(cluster_epoch(15), model::no_timeout).get();
    ASSERT_TRUE(final_result.has_value())
      << "advance_epoch should succeed on leader";
    auto final_info = final_result.value();
    EXPECT_EQ(final_info.max_applied_epoch, cluster_epoch(15));
    EXPECT_EQ(final_info.estimated_inactive_epoch, cluster_epoch(9));
    EXPECT_EQ(final_info, frontend.get_epoch_info());
}

// Regression test for the produce-path reorder bug.
//
// Two consecutive `frontend::replicate` calls for the same
// (producer-id, partition) must replicate through `rm_stm` in call order,
// even if the underlying `data_plane_api::stage_write` futures resolve out
// of order. The current implementation reserves the per-producer ordering
// ticket *inside* the `then_wrapped` continuation that fires after
// stage_write resolves, so reorder of stage_write resolution causes
// reorder of ticket reservation, defeating the ordering guarantee the
// `producer_queue` is supposed to provide.
//
// Concrete failure on buggy code: when B2 (first_seq>0) wins the race and
// reaches rm_stm first, it is accepted via the kafka-compatible
// `skip_sequence_checks` path (rm_stm.cc:1156) because the producer is
// unknown to that partition. `last_known_sequence` advances to B2's
// last_seq, and the subsequent B1 (first_seq=0) is rejected with
// OUT_OF_ORDER_SEQUENCE_NUMBER (cluster::errc::sequence_out_of_order).
TEST_F(
  frontend_fixture,
  test_replicate_preserves_per_producer_order_under_stage_write_reorder) {
    const model::topic topic_name("reorder_regression");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.storage_mode = model::redpanda_storage_mode::cloud;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    // Force stage_write resolution order: the first call (for B1) is held
    // unresolved on a promise; the second call (for B2) returns an
    // already-resolved future. This simulates the cold-start race in
    // production where `prepare_write`'s yield points let B2's stage_write
    // resolve before B1's.
    using stage_result = std::expected<staged_write, std::error_code>;
    ss::promise<stage_result> b1_stage;

    EXPECT_CALL(*_data_plane, stage_write(_))
      .WillOnce(Return(ByMove(b1_stage.get_future())))
      .WillOnce(Return(ss::as_ready_future(stage_result{})));

    // Both batches use the same cluster_epoch so neither fence step is
    // rejected on epoch grounds. The order of `execute_write` calls is the
    // same in both buggy and fixed code (B2 first because its stage_write
    // resolves first; B1 second).
    EXPECT_CALL(*_data_plane, execute_write(_, _, _, _))
      .WillOnce(Return(make_extent_fut(model::offset(0), cluster_epoch(1))))
      .WillOnce(Return(make_extent_fut(model::offset(1), cluster_epoch(1))));

    EXPECT_CALL(*_data_plane, invalidate_epoch_below(_))
      .Times(AnyNumber())
      .WillRepeatedly([](cloud_topics::cluster_epoch) { return ss::now(); });

    ON_CALL(*_data_plane, cache_put_ordered(_, _))
      .WillByDefault([](const auto&, auto) {});

    auto make_idempotent_bid = [](int32_t first_seq, int32_t record_count) {
        return model::batch_identity{
          .pid = model::producer_identity{42, 0},
          .first_seq = first_seq,
          .last_seq = first_seq + record_count - 1,
          .record_count = record_count,
          .max_timestamp = model::timestamp::min(),
          .is_transactional = false,
        };
    };

    auto batch1 = model::test::make_random_batch(model::offset{0}, false);
    auto batch1_count = batch1.record_count();
    auto batch2 = model::test::make_random_batch(model::offset{1}, false);
    auto batch2_count = batch2.record_count();

    // Issue B1 (stage_write blocked on promise) and B2 (stage_write ready).
    // In buggy code, B2's `then_wrapped` continuation reserves its ticket
    // first and reaches rm_stm first. In fixed code, B1's ticket is
    // reserved synchronously before stage_write, so B1 reaches rm_stm
    // first (after we unblock its stage_write) and B2 waits behind it on
    // ticket.redeem().
    auto stages1 = frontend.replicate(
      make_idempotent_bid(0, batch1_count),
      std::move(batch1),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    auto stages2 = frontend.replicate(
      make_idempotent_bid(batch1_count, batch2_count),
      std::move(batch2),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    // Drive the reactor: in the buggy code this gives B2's chain enough
    // runway to reach rm_stm before we unblock B1. In the fixed code B2's
    // chain runs through stage_write but blocks at `ticket.redeem()`
    // waiting for B1's ticket release; that's still consistent with
    // `request_enqueued` resolving (which only requires the
    // `then_wrapped` callback to return).
    stages2.request_enqueued.get();

    // Now release B1's stage_write. From here both batches' chains can
    // run to completion. In fixed code, B1 reaches rm_stm first
    // (first_seq=0, normal accept), then B2 (first_seq matches expected).
    // In buggy code, B2 already reached rm_stm via skip_sequence_checks
    // and B1 is now rejected with OUT_OF_ORDER_SEQUENCE_NUMBER.
    b1_stage.set_value(stage_result{});

    auto r1 = std::move(stages1.replicate_finished).get();
    auto r2 = std::move(stages2.replicate_finished).get();

    ASSERT_TRUE(r1.has_value())
      << "B1 (first_seq=0) should be accepted by rm_stm; got error: "
      << r1.error().message()
      << " — this indicates stage_write reorder caused B2 to reach rm_stm "
         "first, advancing last_known_sequence past B1.";
    ASSERT_TRUE(r2.has_value())
      << "B2 should be accepted by rm_stm; got error: " << r2.error().message();
}
