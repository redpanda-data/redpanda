// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/fetch.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <boost/test/tools/old/interface.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(partition_iterator) {
    /*
     * extract topic partitions from the request
     */
    auto transform = [](const kafka::fetch_request& request) {
        std::vector<model::topic_partition> res;
        std::transform(
          request.cbegin(),
          request.cend(),
          std::back_inserter(res),
          [&res](const kafka::fetch_request::const_iterator::value_type& v) {
              if (v.new_topic) {
                  if (!res.empty()) {
                      BOOST_TEST(res.back().topic != v.topic->name);
                  }
              } else {
                  BOOST_TEST(!res.empty());
                  BOOST_TEST(res.back().topic == v.topic->name);
              }
              return model::topic_partition(
                v.topic->name, v.partition->partition_index);
          });
        return res;
    };

    {
        // no topics -> empty
        kafka::fetch_request req;
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, no partitions -> empty
        kafka::fetch_request req;
        req.data.topics.push_back({.name = model::topic("t0")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 2 topics, no partitions -> empty
        kafka::fetch_request req;
        req.data.topics.push_back({.name = model::topic("t0")});
        req.data.topics.push_back({.name = model::topic("t1")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, 1 partition
        kafka::fetch_request req;
        req.data.topics.push_back({
          .name = model::topic("t0"),
          .fetch_partitions = {{.partition_index = model::partition_id(100)}},
        });
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 1);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
    }

    {
        // 1 topic, 2 partitions
        kafka::fetch_request req;
        req.data.topics.push_back(
          {.name = model::topic("t0"),
           .fetch_partitions = {
             {.partition_index = model::partition_id(100)},
             {.partition_index = model::partition_id(101)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 2);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
    }

    {
        // 2 topics, 2/1 partition
        kafka::fetch_request req;
        req.data.topics.push_back(
          {.name = model::topic("t0"),
           .fetch_partitions = {
             {.partition_index = model::partition_id(100)},
             {.partition_index = model::partition_id(101)}}});
        req.data.topics.push_back(
          {.name = model::topic("t1"),
           .fetch_partitions = {
             {.partition_index = model::partition_id(102)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 3);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
        BOOST_TEST(parts[2].topic == model::topic("t1"));
        BOOST_TEST(parts[2].partition == 102);
    }

    {
        // 4 topics, 2/{}/{}/2 partition
        kafka::fetch_request req;
        req.data.topics.push_back(
          {.name = model::topic("t0"),
           .fetch_partitions = {
             {.partition_index = model::partition_id(100)},
             {.partition_index = model::partition_id(101)}}});
        req.data.topics.push_back({.name = model::topic("t1")});
        req.data.topics.push_back({.name = model::topic("t2")});
        req.data.topics.push_back(
          {.name = model::topic("t3"),
           .fetch_partitions = {
             {.partition_index = model::partition_id(102)},
             {.partition_index = model::partition_id(103)}}});
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 4);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
        BOOST_TEST(parts[1].topic == model::topic("t0"));
        BOOST_TEST(parts[1].partition == 101);
        BOOST_TEST(parts[2].topic == model::topic("t3"));
        BOOST_TEST(parts[2].partition == 102);
        BOOST_TEST(parts[3].topic == model::topic("t3"));
        BOOST_TEST(parts[3].partition == 103);
    }
}

// TODO: when we have a more precise log builder tool we can make these finer
// grained tests. for now the test is coarse grained based on the random batch
// builder.
FIXTURE_TEST(read_from_ntp_max_bytes, redpanda_thread_fixture) {
    auto do_read = [this](model::ktp ktp, size_t max_bytes) {
        kafka::fetch_config config{
          .start_offset = model::offset(0),
          .max_offset = model::model_limits<model::offset>::max(),
          .max_bytes = max_bytes,
          .timeout = model::no_timeout,
          .isolation_level = model::isolation_level::read_uncommitted,
        };
        auto rctx = make_fetch_request_context();
        auto octx = kafka::op_context(
          std::move(rctx), ss::default_smp_service_group());
        auto shard = octx.rctx.shards().shard_for(ktp).value();
        kafka::read_result res
          = octx.rctx.partition_manager()
              .invoke_on(
                shard,
                [&octx, ktp, config](cluster::partition_manager& pm) {
                    return kafka::testing::read_from_ntp(
                      pm,
                      octx.rctx.server().local().get_replica_selector(),
                      ktp,
                      config,
                      true,
                      model::no_timeout,
                      false,
                      octx.rctx.server().local().memory(),
                      octx.rctx.server().local().memory_fetch_sem());
                })
              .get();
        BOOST_TEST_REQUIRE(res.has_data());
        return res;
    };
    wait_for_controller_leadership().get();
    auto ntp = make_data();

    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->last_stable_offset() >= model::offset(1);
          });
    }).get();

    auto zero = do_read(ntp, 0).get_data().size_bytes();
    auto one = do_read(ntp, 1).get_data().size_bytes();
    auto maxlimit = do_read(ntp, std::numeric_limits<size_t>::max())
                      .get_data()
                      .size_bytes();

    BOOST_TEST(zero > 0); // read something
    BOOST_TEST(zero == one);
    BOOST_TEST(one <= maxlimit); // read more
}

FIXTURE_TEST(fetch_one, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    {
        using namespace storage;
        storage::disk_log_builder builder(log_config);
        storage::ntp_config ntp_cfg(
          ntp,
          log_config.base_dir,
          nullptr,
          get_next_partition_revision_id().get());
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batch(model::offset(0), 10, maybe_compress_batches::yes)
          | stop();
    }

    add_topic(model::topic_namespace_view(ntp)).get();
    auto shard = app.shard_table.local().shard_for(ntp);

    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    for (auto version : boost::irange<uint16_t>(
           kafka::fetch_handler::min_supported,
           kafka::fetch_handler::max_supported + int16_t(1))) {
        info("Checking fetch api v{}", version);
        kafka::fetch_request req;
        req.data.max_bytes = std::numeric_limits<int32_t>::max();
        req.data.min_bytes = 1;
        req.data.max_wait_ms = std::chrono::milliseconds(0);
        // disable incremental fetches
        req.data.session_id = kafka::invalid_fetch_session_id;
        req.data.session_epoch = kafka::final_fetch_session_epoch;
        req.data.topics.emplace_back(kafka::fetch_topic{
          .name = topic,
          .fetch_partitions = {{
            .partition_index = pid,
            .fetch_offset = offset,
          }},
        });

        auto client = make_kafka_client().get();
        client.connect().get();
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(version)).get();
        client.stop().then([&client] { client.shutdown(); }).get();

        BOOST_REQUIRE(resp.data.topics.size() == 1);
        BOOST_REQUIRE(resp.data.topics[0].name == topic());
        BOOST_REQUIRE(resp.data.topics[0].partitions.size() == 1);
        BOOST_REQUIRE(
          resp.data.topics[0].partitions[0].error_code
          == kafka::error_code::none);
        BOOST_REQUIRE(resp.data.topics[0].partitions[0].partition_index == pid);
        BOOST_REQUIRE(resp.data.topics[0].partitions[0].records);
        BOOST_REQUIRE(
          resp.data.topics[0].partitions[0].records->size_bytes() > 0);
    }
}

FIXTURE_TEST(fetch_response_iterator_test, redpanda_thread_fixture) {
    static auto make_partition = [](ss::sstring topic) {
        return kafka::fetch_response::partition{
          .name = model::topic(std::move(topic))};
    };

    static auto make_partition_response = [](int id) {
        kafka::fetch_response::partition_response resp;
        resp.error_code = kafka::error_code::none;
        resp.partition_index = model::partition_id(id);
        resp.last_stable_offset = model::offset(0);
        return resp;
    };

    auto make_test_fetch_response = []() {
        kafka::fetch_response response;
        response.data.topics.push_back(make_partition("tp-1"));
        response.data.topics.push_back(make_partition("tp-2"));
        response.data.topics.push_back(make_partition("tp-3"));

        response.data.topics[0].partitions.push_back(
          make_partition_response(0));
        response.data.topics[0].partitions.push_back(
          make_partition_response(1));
        response.data.topics[0].partitions.push_back(
          make_partition_response(2));

        response.data.topics[1].partitions.push_back(
          make_partition_response(0));

        response.data.topics[2].partitions.push_back(
          make_partition_response(0));
        response.data.topics[2].partitions.push_back(
          make_partition_response(1));
        return response;
    };
    auto fetch_request = make_fetch_request_context();
    auto response = make_test_fetch_response();

    int i = 0;

    for (auto it = response.begin(); it != response.end(); ++it) {
        if (i < 3) {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-1");
            BOOST_REQUIRE_EQUAL(it->partition_response->partition_index(), i);
        } else if (i == 3) {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-2");
            BOOST_REQUIRE_EQUAL(it->partition_response->partition_index(), 0);
        } else {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-3");
            BOOST_REQUIRE_EQUAL(
              it->partition_response->partition_index(), i - 4);
        }
        ++i;
    }
};

FIXTURE_TEST(fetch_non_existent, redpanda_thread_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    wait_for_controller_leadership().get();
    add_topic(model::topic_namespace_view(ntp)).get();
    kafka::fetch_request non_existent_ntp;
    non_existent_ntp.data.max_wait_ms = std::chrono::milliseconds(1000);
    non_existent_ntp.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic,
      .fetch_partitions = {{
        .partition_index = model::partition_id{-1},
        .current_leader_epoch = kafka::leader_epoch(0),
        .fetch_offset = model::offset(0),
      }}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto defer = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    auto resp = client
                  .dispatch(std::move(non_existent_ntp), kafka::api_version(6))
                  .get();
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE(resp.data.topics.at(0).errored());
    BOOST_REQUIRE_EQUAL(resp.data.topics.at(0).partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics.at(0).partitions.at(0).error_code,
      kafka::error_code::unknown_topic_or_partition);
}

FIXTURE_TEST(fetch_empty, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    wait_for_controller_leadership().get();
    add_topic(model::topic_namespace_view(ntp)).get();

    wait_for_partition_offset(ntp, model::offset(0)).get();

    kafka::fetch_request no_topics;
    no_topics.data.max_bytes = std::numeric_limits<int32_t>::max();
    no_topics.data.min_bytes = 1;
    no_topics.data.max_wait_ms = std::chrono::milliseconds(1000);

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp_1
      = client.dispatch(std::move(no_topics), kafka::api_version(6)).get();

    BOOST_REQUIRE(resp_1.data.topics.empty());

    kafka::fetch_request no_partitions;
    no_partitions.data.max_bytes = std::numeric_limits<int32_t>::max();
    no_partitions.data.min_bytes = 1;
    no_partitions.data.max_wait_ms = std::chrono::milliseconds(1000);
    no_partitions.data.topics.emplace_back(
      kafka::fetch_topic{.name = topic, .fetch_partitions = {}});

    // NOTE(oren): this looks like it was ill-formed before? see surrounding
    // code
    auto resp_2
      = client.dispatch(std::move(no_partitions), kafka::api_version(6)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp_2.data.topics.empty());
}

FIXTURE_TEST(fetch_leader_epoch, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    wait_for_controller_leadership().get();
    add_topic(model::topic_namespace_view(ntp)).get();

    wait_for_partition_offset(ntp, model::offset(0)).get();

    const auto shard = app.shard_table.local().shard_for(ntp);
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp, this](cluster::partition_manager& mgr) {
            auto partition = mgr.get(ntp);
            {
                auto batches
                  = model::test::make_random_batches(model::offset(0), 5).get();
                auto rdr = model::make_memory_record_batch_reader(
                  std::move(batches));
                partition->raft()
                  ->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result()
                  .get();
            }
            partition->raft()->step_down("trigger epoch change").get();
            wait_for_leader(ntp, 10s).get();
            {
                auto batches
                  = model::test::make_random_batches(model::offset(0), 5).get();
                auto rdr = model::make_memory_record_batch_reader(
                  std::move(batches));
                partition->raft()
                  ->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result()
                  .get();
            }
        })
      .get();

    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(1000);
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic,
      .fetch_partitions = {{
        .partition_index = pid,
        .current_leader_epoch = kafka::leader_epoch(1),
        .fetch_offset = model::offset(6),
      }}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(9)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_MESSAGE(
      resp.data.topics[0].partitions[0].error_code
        == kafka::error_code::fenced_leader_epoch,
      fmt::format("error: {}", resp.data.topics[0].partitions[0].error_code));
}

FIXTURE_TEST(fetch_multi_partitions_debounce, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::offset offset(0);

    wait_for_controller_leadership().get();

    add_topic(model::topic_namespace(model::ns("kafka"), topic), 6).get();

    for (int i = 0; i < 6; ++i) {
        auto ntp = make_default_ntp(topic, model::partition_id(i));
        wait_for_partition_offset(ntp, model::offset(0)).get();
    }

    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(3000);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic,
      .fetch_partitions = {},
    });
    for (int i = 0; i < 6; ++i) {
        kafka::fetch_request::partition p;
        p.partition_index = model::partition_id(i);
        p.log_start_offset = offset;
        p.fetch_offset = offset;
        p.max_bytes = std::numeric_limits<int32_t>::max();
        req.data.topics[0].fetch_partitions.push_back(p);
    }
    auto client = make_kafka_client().get();
    client.connect().get();
    auto fresp = client.dispatch(std::move(req), kafka::api_version(4));

    for (int i = 0; i < 6; ++i) {
        model::partition_id partition_id(i);
        auto ntp = make_default_ntp(topic, partition_id);
        auto shard = app.shard_table.local().shard_for(ntp);
        app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                return model::test::make_random_batches(model::offset(0), 5)
                  .then([ntp, &mgr](auto batches) {
                      auto partition = mgr.get(ntp);
                      auto rdr = model::make_memory_record_batch_reader(
                        std::move(batches));
                      return partition->raft()->replicate(
                        std::move(rdr),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack));
                  });
            })
          .discard_result()
          .get();
    }
    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, topic());
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 6);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].partitions[i].error_code,
          kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].partitions[i].partition_index,
          model::partition_id(i));
        BOOST_REQUIRE(resp.data.topics[0].partitions[i].records);

        total_size += resp.data.topics[0].partitions[i].records->size_bytes();
    }
    BOOST_REQUIRE_GT(total_size, 0);
}

FIXTURE_TEST(fetch_leader_ack, redpanda_thread_fixture) {
    // Ensures that fetching works as expected even when data is produced with
    // relaxed consistency.
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);

    wait_for_controller_leadership().get();

    add_topic(model::topic_namespace_view(ntp)).get();
    wait_for_partition_offset(ntp, model::offset(0)).get();

    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(5000);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic,
      .fetch_partitions = {{
        .partition_index = pid,
        .fetch_offset = offset,
      }},
    });

    auto client = make_kafka_client().get();
    client.connect().get();
    auto fresp = client.dispatch(std::move(req), kafka::api_version(4));
    auto shard = app.shard_table.local().shard_for(ntp);
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            return model::test::make_random_batches(model::offset(0), 5)
              .then([ntp, &mgr](auto batches) {
                  auto partition = mgr.get(ntp);
                  auto rdr = model::make_memory_record_batch_reader(
                    std::move(batches));
                  return partition->raft()->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::leader_ack));
              });
        })
      .discard_result()
      .get();

    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.topics.size() == 1);
    BOOST_REQUIRE(resp.data.topics[0].name == topic());
    BOOST_REQUIRE(resp.data.topics[0].partitions.size() == 1);
    BOOST_REQUIRE(
      resp.data.topics[0].partitions[0].error_code == kafka::error_code::none);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].records);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].records->size_bytes() > 0);
}

FIXTURE_TEST(fetch_one_debounce, redpanda_thread_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);

    wait_for_controller_leadership().get();

    add_topic(model::topic_namespace_view(ntp)).get();
    wait_for_partition_offset(ntp, model::offset(0)).get();

    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(5000);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic,
      .fetch_partitions = {{
        .partition_index = pid,
        .fetch_offset = offset,
      }},
    });

    auto client = make_kafka_client().get();
    client.connect().get();
    auto fresp = client.dispatch(std::move(req), kafka::api_version(4));
    auto shard = app.shard_table.local().shard_for(ntp);
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            return model::test::make_random_batches(model::offset(0), 5)
              .then([ntp, &mgr](auto batches) {
                  auto partition = mgr.get(ntp);
                  auto rdr = model::make_memory_record_batch_reader(
                    std::move(batches));
                  return partition->raft()->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .discard_result()
      .get();

    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.topics.size() == 1);
    BOOST_REQUIRE(resp.data.topics[0].name == topic());
    BOOST_REQUIRE(resp.data.topics[0].partitions.size() == 1);
    BOOST_REQUIRE(
      resp.data.topics[0].partitions[0].error_code == kafka::error_code::none);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].records);
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].records->size_bytes() > 0);
}

FIXTURE_TEST(fetch_multi_topics, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic_1("foo");
    model::topic topic_2("bar");
    model::offset zero(0);
    wait_for_controller_leadership().get();

    add_topic(model::topic_namespace(model::ns("kafka"), topic_1), 6).get();
    add_topic(model::topic_namespace(model::ns("kafka"), topic_2), 1).get();

    std::vector<model::ntp> ntps = {};
    // topic 1
    for (int i = 0; i < 6; ++i) {
        ntps.push_back(make_default_ntp(topic_1, model::partition_id(i)));
        wait_for_partition_offset(ntps.back(), model::offset(0)).get();
    }
    // topic 2
    ntps.push_back(make_default_ntp(topic_2, model::partition_id(0)));
    wait_for_partition_offset(ntps.back(), model::offset(0)).get();

    // request
    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(3000);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic_1,
      .fetch_partitions = {},
    });
    req.data.topics.emplace_back(kafka::fetch_topic{
      .name = topic_2,
      .fetch_partitions = {},
    });

    for (auto& ntp : ntps) {
        kafka::fetch_request::partition p;
        p.partition_index = model::partition_id(ntp.tp.partition);
        p.log_start_offset = zero;
        p.fetch_offset = zero;
        p.max_bytes = std::numeric_limits<int32_t>::max();
        auto idx = ntp.tp.topic == topic_1 ? 0 : 1;
        req.data.topics[idx].fetch_partitions.push_back(p);
    }

    auto client = make_kafka_client().get();
    client.connect().get();
    // add date to all partitions
    for (auto& ntp : ntps) {
        auto shard = app.shard_table.local().shard_for(ntp);
        app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                return model::test::make_random_batches(model::offset(0), 5)
                  .then([ntp, &mgr](auto batches) {
                      auto partition = mgr.get(ntp);
                      auto rdr = model::make_memory_record_batch_reader(
                        std::move(batches));
                      return partition->raft()->replicate(
                        std::move(rdr),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack));
                  });
            })
          .discard_result()
          .get();
    }

    auto resp = client.dispatch(std::move(req), kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 2);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, topic_1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[1].name, topic_2);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 6);
    BOOST_REQUIRE_EQUAL(resp.data.topics[1].partitions.size(), 1);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].partitions[i].error_code,
          kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].partitions[i].partition_index,
          model::partition_id(i));
        BOOST_REQUIRE(resp.data.topics[0].partitions[i].records);

        total_size += resp.data.topics[0].partitions[i].records->size_bytes();
    }
    BOOST_REQUIRE_GT(total_size, 0);
}

FIXTURE_TEST(fetch_request_max_bytes, redpanda_thread_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    auto ntp = make_default_ntp(topic, pid);

    wait_for_controller_leadership().get();

    add_topic(model::topic_namespace_view(ntp)).get();
    wait_for_partition_offset(ntp, model::offset(0)).get();
    // append some data
    auto shard = app.shard_table.local().shard_for(ntp);
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            return model::test::make_random_batches(model::offset(0), 20)
              .then([ntp, &mgr](auto batches) {
                  auto partition = mgr.get(ntp);
                  auto rdr = model::make_memory_record_batch_reader(
                    std::move(batches));
                  return partition->raft()->replicate(
                    std::move(rdr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .get();

    auto fetch = [this, &topic, &pid](int32_t max_bytes) {
        kafka::fetch_request req;
        req.data.max_bytes = max_bytes;
        req.data.min_bytes = 1;
        req.data.max_wait_ms = std::chrono::milliseconds(0);
        req.data.session_id = kafka::invalid_fetch_session_id;
        req.data.topics.emplace_back(kafka::fetch_topic{
          .name = topic,
          .fetch_partitions = {{
            .partition_index = pid,
            .fetch_offset = model::offset(0),
          }},
        });

        auto client = make_kafka_client().get();
        client.connect().get();
        auto fresp = client.dispatch(std::move(req), kafka::api_version(4));

        auto resp = fresp.get();
        client.stop().then([&client] { client.shutdown(); }).get();
        return resp;
    };
    auto fetch_one_byte = fetch(1);
    /**
     * At least one record has to be returned since KiP-74
     */
    BOOST_REQUIRE(fetch_one_byte.data.topics.size() == 1);
    BOOST_REQUIRE(fetch_one_byte.data.topics[0].name == topic());
    BOOST_REQUIRE(fetch_one_byte.data.topics[0].partitions.size() == 1);
    BOOST_REQUIRE(
      fetch_one_byte.data.topics[0].partitions[0].error_code
      == kafka::error_code::none);
    BOOST_REQUIRE(
      fetch_one_byte.data.topics[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(fetch_one_byte.data.topics[0].partitions[0].records);
    BOOST_REQUIRE(
      fetch_one_byte.data.topics[0].partitions[0].records->size_bytes() > 0);
}
