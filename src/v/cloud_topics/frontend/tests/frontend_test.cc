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
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/tests/random_batch.h"
#include "raft/types.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "ssx/sformat.h"
#include "storage/ntp_config.h"
#include "test_utils/async.h"
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
      (ss::future<std::expected<chunked_vector<extent_meta>, std::error_code>>),
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
       model::opt_abort_source_t),
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

    MOCK_METHOD(size_t, materialize_max_bytes, (), (const, override));

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
    return ss::make_ready_future<
      std::expected<chunked_vector<extent_meta>, std::error_code>>(
      std::move(vec));
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
    props.cloud_topic_enabled = true;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();

    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>()
      != nullptr);

    cloud_topics::frontend frontend(std::move(partition), _data_plane.get());

    EXPECT_CALL(*_data_plane, cache_put(_, _)).Times(2);
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
