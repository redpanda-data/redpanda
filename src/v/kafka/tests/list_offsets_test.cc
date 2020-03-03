#include "kafka/requests/list_offsets_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

FIXTURE_TEST(list_offsets, redpanda_thread_fixture) {
    auto now = ss::lowres_clock::now().time_since_epoch();
    auto now_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    auto query_ts = model::timestamp(now_ms);

    auto ntp = make_data();
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        ntp.tp.partition,
        query_ts,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.topics[0].partitions.size(), 1);
    BOOST_CHECK(resp.topics[0].partitions[0].timestamp >= query_ts);
    BOOST_CHECK(resp.topics[0].partitions[0].offset >= model::offset(0));
}

FIXTURE_TEST(list_offsets_earliest, redpanda_thread_fixture) {
    auto ntp = make_data();
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        ntp.tp.partition,
        kafka::list_offsets_request::earliest_timestamp,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.topics[0].partitions.size(), 1);
    BOOST_CHECK(resp.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.topics[0].partitions[0].offset == model::offset(0));
}

FIXTURE_TEST(list_offsets_latest, redpanda_thread_fixture) {
    auto ntp = make_data();
    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::list_offsets_request req;
    req.topics = {{
      .name = ntp.tp.topic,
      .partitions = {{
        ntp.tp.partition,
        kafka::list_offsets_request::latest_timestamp,
      }},
    }};

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.topics[0].partitions.size(), 1);
    BOOST_CHECK(resp.topics[0].partitions[0].timestamp == model::timestamp(-1));
    BOOST_CHECK(resp.topics[0].partitions[0].offset > model::offset(0));
}
