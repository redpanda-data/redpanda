// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/list_offsets.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
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
