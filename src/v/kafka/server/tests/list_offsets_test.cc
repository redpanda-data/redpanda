// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/produce.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

FIXTURE_TEST(list_offsets, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    // Synthetic default timestamp for produced data
    auto base_ts = model::timestamp{10000};

    auto ntp = make_data(base_ts);
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
        [ntp](cluster::partition_manager& mgr) {
            info("Manager:{} - log:{}", mgr, *mgr.get(ntp));
        })
      .get();

    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::list_offsets_request req;
    req.data.topics.emplace_back(kafka::list_offset_topic{
      .name = ntp.get_topic(),
      .partitions = {{
        .partition_index = ntp.get_partition(),
        .timestamp = base_ts,
      }},
    });

    auto resp = client.dispatch(std::move(req), kafka::api_version(1)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset == model::offset(0));
    BOOST_CHECK(resp.data.topics[0].partitions[0].timestamp == base_ts);
}

FIXTURE_TEST(list_offsets_earliest, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    auto ntp = make_data();
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
    req.data.topics.emplace_back(kafka::list_offset_topic{
      .name = ntp.get_topic(),
      .partitions = {{
        .partition_index = ntp.get_partition(),
        .timestamp = kafka::list_offsets_request::earliest_timestamp,
      }},
    });

    auto resp = client.dispatch(std::move(req), kafka::api_version(1)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset == model::offset(0));
}

FIXTURE_TEST(list_offsets_latest, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    auto ntp = make_data();
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
    req.data.topics.emplace_back(kafka::list_offset_topic{
      .name = ntp.get_topic(),
      .partitions = {{
        .partition_index = ntp.get_partition(),
        .timestamp = kafka::list_offsets_request::latest_timestamp,
      }},
    });

    auto resp = client.dispatch(std::move(req), kafka::api_version(1)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
    BOOST_CHECK(
      resp.data.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.data.topics[0].partitions[0].offset > model::offset(0));
}

FIXTURE_TEST(list_offsets_not_found, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    auto base_ts = model::timestamp{100000};
    auto future_ts = model::timestamp{9999999};

    auto ntp = make_data(base_ts);
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
    req.data.topics.emplace_back(kafka::list_offset_topic{
      .name = ntp.get_topic(),
      .partitions = {{
        .partition_index = ntp.get_partition(),
        .timestamp = future_ts,
      }},
    });

    auto resp = client.dispatch(std::move(req), kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    // request is asking for messages with timestamp far ahead of the produced
    // records: doesn't find it and returns an empty response
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
    chunked_vector<kafka::produce_request::partition> partitions;
    partitions.emplace_back(kafka::produce_request::partition{
      .partition_index{tp.partition},
      .records = kafka::produce_request_record_data(std::move(batch))});

    chunked_vector<kafka::produce_request::topic> topics;
    topics.emplace_back(kafka::produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    return kafka::produce_request(t_id, acks, std::move(topics));
}

FIXTURE_TEST(list_offsets_by_time, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    model::ntp ntp(
      model::kafka_namespace,
      model::topic(random_generators::gen_alphanum_string(8)),
      model::partition_id(0));

    add_topic(model::topic_namespace_view{ntp}, 1).get();
    wait_for_partition_offset(ntp, model::offset(0)).get();

    auto client = make_kafka_client().get();
    client.connect().get();

    // 3 batches of 3 records, with timestamps incrementing 1ms per record
    const size_t batch_count = 13;
    const size_t record_count = 3;
    std::vector<model::record_batch> batches;
    batches.reserve(batch_count);

    // Arbitrary synthetic timestamp for start of produce
    auto base_timestamp = 100000;

    for (long i = 0; i < batch_count; ++i) {
        // Mixture of compressed and uncompressed, they have distinct offset
        // lookup behavior when searching by timequery, which will be
        // validated
        bool compressed = i % 3 == 0;
        batches.push_back(make_random_batch(model::test::record_batch_spec{
          // after queries below.
          .allow_compression = compressed,
          .count = record_count,
          .timestamp{base_timestamp + i * record_count}}));
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

        req.data.topics.emplace_back(kafka::list_offset_topic{
          .name = ntp.tp.topic,
          .partitions = {{
            .partition_index = ntp.tp.partition,
            .timestamp = model::timestamp(base_timestamp + i * record_count),
          }},
        });

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(1)).get();

        BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
        BOOST_CHECK(
          resp.data.topics[0].partitions[0].timestamp
          == model::timestamp(base_timestamp + i * record_count));
        BOOST_CHECK(
          resp.data.topics[0].partitions[0].offset
          == model::offset(i * record_count));

        // Fetch a timestamp in the middle of a batch, because these are
        // compressed batches we should still get a result pointing
        // to the start of the batch.
        auto record_offset = 1; // Offset into batch which we will read
        kafka::list_offsets_request req2;
        req2.data.topics.emplace_back(kafka::list_offset_topic{
          .name = ntp.tp.topic,
          .partitions = {{
            .partition_index = ntp.tp.partition,
            .timestamp = model::timestamp(
              base_timestamp + i * record_count + record_offset),
          }},
        });

        const auto& batch = batches[i];
        auto resp_midbatch
          = client.dispatch(std::move(req2), kafka::api_version(1)).get();
        BOOST_REQUIRE_EQUAL(resp_midbatch.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(resp_midbatch.data.topics[0].partitions.size(), 1);
        if (batch.compressed()) {
            // Compressed batch: result will point to start of batch, slightly
            // earlier than the query timestamp
            BOOST_CHECK(
              resp_midbatch.data.topics[0].partitions[0].timestamp
              == model::timestamp(base_timestamp + i * record_count));
            BOOST_CHECK(
              resp_midbatch.data.topics[0].partitions[0].offset
              == model::offset(i * record_count));
        } else {
            // Uncompressed batch: result should have seeked to correct record
            BOOST_CHECK(
              resp_midbatch.data.topics[0].partitions[0].timestamp
              == model::timestamp(
                base_timestamp + i * record_count + record_offset));
            BOOST_CHECK(
              resp_midbatch.data.topics[0].partitions[0].offset
              == model::offset(i * record_count + record_offset));
        }
    }

    client.stop().then([&client] { client.shutdown(); }).get();
}
