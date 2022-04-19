// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/protocol/produce.h"
#include "model/metadata.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

FIXTURE_TEST(list_offsets, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    auto query_ts = model::timestamp::now();
    auto ntp = make_data(get_next_partition_revision_id().get());
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    // print the logs for manager at core
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp, this](cluster::partition_manager& mgr) {
            info("Manager:{} - log:{}", mgr, *mgr.get(ntp));
        })
      .get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.data.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        .partition_index = ntp.tp.partition,
        .timestamp = query_ts,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(resp.data.topics[0].partitions[0].timestamp >= query_ts);
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset >= model::offset(0));
}

FIXTURE_TEST(list_offsets_earliest, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    auto ntp = make_data(get_next_partition_revision_id().get());
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.data.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        .partition_index = ntp.tp.partition,
        .timestamp = kafka::list_offsets_request::earliest_timestamp,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset == model::offset(0));
}

FIXTURE_TEST(list_offsets_latest, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    auto ntp = make_data(get_next_partition_revision_id().get());
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.data.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        .partition_index = ntp.tp.partition,
        .timestamp = kafka::list_offsets_request::latest_timestamp,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset > model::offset(0));
}

FIXTURE_TEST(list_offsets_not_found, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    auto ntp = make_data(get_next_partition_revision_id().get());
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::list_offsets_request req;
    req.data.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        .partition_index = ntp.tp.partition,
        .timestamp = model::timestamp::now(),
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    // request is asking for messages with timestamp => timestamp::now(),
    // doesn't find it and returns an empty response
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset == model::offset(-1));
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].leader_epoch
      == kafka::leader_epoch(-1));
}

kafka::produce_request
make_produce_request(model::topic_partition tp, model::record_batch&& batch) {
    std::vector<kafka::produce_request::partition> partitions;
    partitions.emplace_back(kafka::produce_request::partition{
      .partition_index{tp.partition},
      .records = kafka::produce_request_record_data(std::move(batch))});

    std::vector<kafka::produce_request::topic> topics;
    topics.emplace_back(kafka::produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    return kafka::produce_request(t_id, acks, std::move(topics));
}

FIXTURE_TEST(list_offsets_by_time, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    model::ntp ntp(
      model::kafka_namespace,
      model::topic(random_generators::gen_alphanum_string(8)),
      model::partition_id(0));

    add_topic(model::topic_namespace_view{ntp}, 1).get();
    wait_for_partition_offset(ntp, model::offset(0)).get0();

    auto client = make_kafka_client().get0();
    client.connect().get();

    // 3 batches of 2 records, with timestamps, 0, 1, 2.
    const size_t batch_count = 3;
    const size_t record_count = 2;
    std::vector<model::record_batch> batches;
    batches.reserve(batch_count);
    for (long i = 0; i < batch_count; ++i) {
        batches.push_back(make_random_batch(storage::test::record_batch_spec{
          .count = record_count, .timestamp{i}}));
        auto req = make_produce_request(ntp.tp, batches.back().share());
        auto res = client.dispatch(std::move(req), kafka::api_version(7)).get();
        const auto& topics = res.data.responses;
        BOOST_REQUIRE_EQUAL(topics.size(), 1);
        const auto& parts = topics[0].partitions;
        BOOST_REQUIRE_EQUAL(parts.size(), 1);
        BOOST_REQUIRE_EQUAL(parts[0].error_code, kafka::error_code::none);
    }

    for (long i = 0; i < batch_count; ++i) {
        // fetch timestamp i, expect offset 2 * i.
        kafka::list_offsets_request req;
        req.data.topics = {{
          .name = ntp.tp.topic,
          .partitions = {{
            .partition_index = ntp.tp.partition,
            .timestamp = model::timestamp(i),
          }},
        }};

        auto resp = client.dispatch(req, kafka::api_version(1)).get0();

        BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
        BOOST_CHECK(
          resp.data.topics[0].partitions[0].timestamp == model::timestamp(i));
        BOOST_CHECK(
          resp.data.topics[0].partitions[0].offset == model::offset(i * 2));
    }

    client.stop().then([&client] { client.shutdown(); }).get();
}
