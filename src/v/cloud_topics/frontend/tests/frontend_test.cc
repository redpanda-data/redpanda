/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/admission_control_types.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/app.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/state_accessors.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/tests/random_batch.h"
#include "raft/replicate.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
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
       allow_materialization_failure allow_mat_failure,
       cloud_io::group_id group),
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

// A fixture wired to the *real* cloud topics data plane (write pipeline,
// write request scheduler, batcher) instead of the mock, so that a produce
// request travels the whole L0 write path before reaching raft.
class real_data_plane_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public Test {
public:
    real_data_plane_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    cloud_topics::data_plane_api* data_plane() {
        return app.cloud_topics_app->get_state()->local().get_data_plane();
    }

    ss::lw_shared_ptr<cluster::partition>
    make_cloud_topic(const model::topic& topic_name) {
        model::ntp ntp(model::kafka_namespace, topic_name, 0);
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        return app.partition_manager.local().get(ntp);
    }

    size_t count_l0_uploads() {
        size_t n = 0;
        for (const auto& r : get_requests()) {
            if (
              r.method == "PUT"
              && r.url.find("level_zero/data/") != ss::sstring::npos) {
                n++;
            }
        }
        return n;
    }
};

namespace {

model::record_batch make_marked_batch(std::string_view marker, int records) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (int i = 0; i < records; i++) {
        builder.add_raw_kv(
          iobuf::from(std::string(marker)), iobuf::from(std::string(marker)));
    }
    return std::move(builder).build();
}

} // namespace

// Kafka requires that records a single producer sends to one partition are
// appended in send order, whether or not the producer is idempotent. A
// non-idempotent client with max.in.flight > 1 may have two produce requests
// for one partition in flight at once: the kafka connection only waits for a
// request's `dispatched` stage (== frontend's request_enqueued) before
// reading the next request off the wire.
//
// This test issues the two requests exactly that way -- the second
// replicate() call is made without waiting for the first request_enqueued --
// and asserts the resulting raft offsets are in submission order.
TEST_F(real_data_plane_fixture, pipelined_no_pid_writes_keep_order) {
    auto partition = make_cloud_topic(model::topic("nopid_order"));
    ASSERT_TRUE(partition);

    cloud_topics::frontend frontend(partition, data_plane());

    auto b1 = make_marked_batch("FIRSTREQUEST", 1);
    auto b2 = make_marked_batch("SECONDREQUEST", 1);
    auto bid1 = model::batch_identity::from(b1.header());
    auto bid2 = model::batch_identity::from(b2.header());
    // Non-idempotent producer: no producer id at all.
    ASSERT_EQ(bid1.pid.get_id(), model::no_producer_id);
    ASSERT_EQ(bid2.pid.get_id(), model::no_producer_id);

    raft::replicate_options opts(raft::consistency_level::quorum_ack);
    auto st1 = frontend.replicate(bid1, std::move(b1), opts);
    auto st2 = frontend.replicate(bid2, std::move(b2), opts);

    st1.request_enqueued.get();
    st2.request_enqueued.get();
    auto r1 = st1.replicate_finished.get();
    auto r2 = st2.replicate_finished.get();

    ASSERT_TRUE(r1.has_value())
      << "first replicate failed: " << r1.error().message();
    ASSERT_TRUE(r2.has_value())
      << "second replicate failed: " << r2.error().message();

    // Both requests were aggregated into a single L0 object; that is the
    // configuration the finding is about.
    EXPECT_EQ(count_l0_uploads(), 1u);

    EXPECT_LT(r1.value().last_offset, r2.value().last_offset)
      << "records must be appended in send order: first request landed at "
      << r1.value().last_offset << ", second at " << r2.value().last_offset;
}

// Same as pipelined_no_pid_writes_keep_order, but with the no-pid
// concurrency limit set to 1. Checks whether the semaphore in
// nopid_ticket_impl acts as an ordering mechanism when it admits only one
// request at a time.
TEST_F(real_data_plane_fixture, pipelined_no_pid_writes_concurrency_one) {
    scoped_config cfg;
    cfg.get("cloud_topics_produce_no_pid_concurrency").set_value(size_t{1});

    auto partition = make_cloud_topic(model::topic("nopid_order_serial"));
    ASSERT_TRUE(partition);

    cloud_topics::frontend frontend(partition, data_plane());

    auto b1 = make_marked_batch("FIRSTREQUEST", 1);
    auto b2 = make_marked_batch("SECONDREQUEST", 1);
    auto bid1 = model::batch_identity::from(b1.header());
    auto bid2 = model::batch_identity::from(b2.header());

    raft::replicate_options opts(raft::consistency_level::quorum_ack);
    auto st1 = frontend.replicate(bid1, std::move(b1), opts);
    auto st2 = frontend.replicate(bid2, std::move(b2), opts);

    st1.request_enqueued.get();
    st2.request_enqueued.get();
    auto r1 = st1.replicate_finished.get();
    auto r2 = st2.replicate_finished.get();

    ASSERT_TRUE(r1.has_value())
      << "first replicate failed: " << r1.error().message();
    ASSERT_TRUE(r2.has_value())
      << "second replicate failed: " << r2.error().message();

    EXPECT_LT(r1.value().last_offset, r2.value().last_offset)
      << "records must be appended in send order: first request landed at "
      << r1.value().last_offset << ", second at " << r2.value().last_offset;
}

// Control for the test above: the same pipelining with a real producer id.
// The producer queue chains tickets per producer id, so ordering must hold
// here. If this control fails the harness is wrong, not the code.
TEST_F(real_data_plane_fixture, pipelined_idempotent_writes_keep_order) {
    auto partition = make_cloud_topic(model::topic("pid_order"));
    ASSERT_TRUE(partition);

    cloud_topics::frontend frontend(partition, data_plane());

    auto b1 = make_marked_batch("FIRSTREQUEST", 1);
    auto b2 = make_marked_batch("SECONDREQUEST", 1);
    model::producer_identity pid{77, 0};
    auto bid1 = model::batch_identity{
      .pid = pid,
      .first_seq = 0,
      .last_seq = 0,
      .record_count = b1.record_count(),
      .max_timestamp = b1.header().max_timestamp,
      .is_transactional = false};
    auto bid2 = model::batch_identity{
      .pid = pid,
      .first_seq = 1,
      .last_seq = 1,
      .record_count = b2.record_count(),
      .max_timestamp = b2.header().max_timestamp,
      .is_transactional = false};

    raft::replicate_options opts(raft::consistency_level::quorum_ack);
    auto st1 = frontend.replicate(bid1, std::move(b1), opts);
    auto st2 = frontend.replicate(bid2, std::move(b2), opts);

    st1.request_enqueued.get();
    st2.request_enqueued.get();
    auto r1 = st1.replicate_finished.get();
    auto r2 = st2.replicate_finished.get();

    ASSERT_TRUE(r1.has_value())
      << "first replicate failed: " << r1.error().message();
    ASSERT_TRUE(r2.has_value())
      << "second replicate failed: " << r2.error().message();

    EXPECT_EQ(count_l0_uploads(), 1u);

    EXPECT_LT(r1.value().last_offset, r2.value().last_offset)
      << "records must be appended in send order: first request landed at "
      << r1.value().last_offset << ", second at " << r2.value().last_offset;
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
