// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "cluster/log_eviction_stm.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/fetch.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "redpanda/tests/fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"

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
                      BOOST_TEST(res.back().topic != v.topic->topic);
                  }
              } else {
                  BOOST_TEST(!res.empty());
                  BOOST_TEST(res.back().topic == v.topic->topic);
              }
              return model::topic_partition(
                v.topic->topic, v.partition->partition);
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
        req.data.topics.push_back({.topic = model::topic("t0")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 2 topics, no partitions -> empty
        kafka::fetch_request req;
        req.data.topics.push_back({.topic = model::topic("t0")});
        req.data.topics.push_back({.topic = model::topic("t1")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, 1 partition
        kafka::fetch_request req;
        req.data.topics.push_back({
          .topic = model::topic("t0"),
          .partitions = {{.partition = model::partition_id(100)}},
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
          {.topic = model::topic("t0"),
           .partitions = {
             {.partition = model::partition_id(100)},
             {.partition = model::partition_id(101)}}});
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
          {.topic = model::topic("t0"),
           .partitions = {
             {.partition = model::partition_id(100)},
             {.partition = model::partition_id(101)}}});
        req.data.topics.push_back(
          {.topic = model::topic("t1"),
           .partitions = {{.partition = model::partition_id(102)}}});
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
          {.topic = model::topic("t0"),
           .partitions = {
             {.partition = model::partition_id(100)},
             {.partition = model::partition_id(101)}}});
        req.data.topics.push_back({.topic = model::topic("t1")});
        req.data.topics.push_back({.topic = model::topic("t2")});
        req.data.topics.push_back(
          {.topic = model::topic("t3"),
           .partitions = {
             {.partition = model::partition_id(102)},
             {.partition = model::partition_id(103)}}});
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
          .max_batch_size = 1_MiB,
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
                      octx.rctx.metadata_cache(),
                      octx.rctx.server().local().get_replica_selector(),
                      ktp,
                      config,
                      model::no_timeout,
                      false,
                      octx.rctx.server().local().fetch_units_manager());
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

    const auto& topic_id = app.metadata_cache.local()
                             .get_topic_metadata_ref(
                               model::topic_namespace_view(ntp))
                             ->get()
                             .get_configuration()
                             .tp_id;

    for (auto version : boost::irange<kafka::api_version>(
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
        req.data.topics.emplace_back(
          kafka::fetch_topic{
            .partitions = {{
              .partition = pid,
              .fetch_offset = offset,
            }},
          });
        if (version >= kafka::api_version{13}) {
            req.data.topics.back().topic_id = *topic_id;
        } else {
            req.data.topics.back().topic = topic;
        }

        auto client = make_kafka_client().get();
        client.connect().get();
        auto resp = client.dispatch(std::move(req), version).get();
        client.stop().then([&client] { client.shutdown(); }).get();

        BOOST_REQUIRE(resp.data.responses.size() == 1);
        if (version >= kafka::api_version{13}) {
            BOOST_REQUIRE_EQUAL(resp.data.responses[0].topic_id, *topic_id);
        } else {
            BOOST_REQUIRE(resp.data.responses[0].topic == topic());
        }
        BOOST_REQUIRE(resp.data.responses[0].partitions.size() == 1);
        BOOST_REQUIRE(
          resp.data.responses[0].partitions[0].error_code
          == kafka::error_code::none);
        BOOST_REQUIRE(
          resp.data.responses[0].partitions[0].partition_index == pid);
        BOOST_REQUIRE(resp.data.responses[0].partitions[0].records);
        BOOST_REQUIRE(
          resp.data.responses[0].partitions[0].records->size_bytes() > 0);
    }
}

FIXTURE_TEST(fetch_response_iterator_test, redpanda_thread_fixture) {
    static auto make_partition = [](ss::sstring topic) {
        return kafka::fetch_response::partition{
          .topic = model::topic(std::move(topic))};
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
        response.data.responses.push_back(make_partition("tp-1"));
        response.data.responses.push_back(make_partition("tp-2"));
        response.data.responses.push_back(make_partition("tp-3"));

        response.data.responses[0].partitions.push_back(
          make_partition_response(0));
        response.data.responses[0].partitions.push_back(
          make_partition_response(1));
        response.data.responses[0].partitions.push_back(
          make_partition_response(2));

        response.data.responses[1].partitions.push_back(
          make_partition_response(0));

        response.data.responses[2].partitions.push_back(
          make_partition_response(0));
        response.data.responses[2].partitions.push_back(
          make_partition_response(1));
        return response;
    };
    auto fetch_request = make_fetch_request_context();
    auto response = make_test_fetch_response();

    int i = 0;

    for (auto it = response.begin(); it != response.end(); ++it) {
        if (i < 3) {
            BOOST_REQUIRE_EQUAL(it->partition->topic(), "tp-1");
            BOOST_REQUIRE_EQUAL(it->partition_response->partition_index(), i);
        } else if (i == 3) {
            BOOST_REQUIRE_EQUAL(it->partition->topic(), "tp-2");
            BOOST_REQUIRE_EQUAL(it->partition_response->partition_index(), 0);
        } else {
            BOOST_REQUIRE_EQUAL(it->partition->topic(), "tp-3");
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
    non_existent_ntp.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = model::partition_id{-1},
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
    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE(resp.data.responses.at(0).errored());
    BOOST_REQUIRE_EQUAL(resp.data.responses.at(0).partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses.at(0).partitions.at(0).error_code,
      kafka::error_code::unknown_topic_or_partition);
}

FIXTURE_TEST(fetch_non_existent_v13, redpanda_thread_fixture) {
    auto topic_id = model::topic_id{uuid_t::create()};
    auto log_config = make_default_config();
    wait_for_controller_leadership().get();
    kafka::fetch_request non_existent_ntp;
    non_existent_ntp.data.max_wait_ms = std::chrono::milliseconds(100);
    non_existent_ntp.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic_id = topic_id,
        .partitions = {{
          .partition = model::partition_id{0},
          .current_leader_epoch = kafka::leader_epoch(0),
          .fetch_offset = model::offset(0),
        }}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto defer = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    auto resp = client
                  .dispatch(std::move(non_existent_ntp), kafka::api_version(13))
                  .get();
    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE(resp.data.responses.at(0).errored());
    BOOST_REQUIRE_EQUAL(resp.data.responses.at(0).partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.responses.at(0).topic_id, topic_id);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses.at(0).partitions.at(0).error_code,
      kafka::error_code::unknown_topic_id);
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

    BOOST_REQUIRE(resp_1.data.responses.empty());

    kafka::fetch_request no_partitions;
    no_partitions.data.max_bytes = std::numeric_limits<int32_t>::max();
    no_partitions.data.min_bytes = 1;
    no_partitions.data.max_wait_ms = std::chrono::milliseconds(1000);
    no_partitions.data.topics.emplace_back(
      kafka::fetch_topic{.topic = topic, .partitions = {}});

    // NOTE(oren): this looks like it was ill-formed before? see surrounding
    // code
    auto resp_2
      = client.dispatch(std::move(no_partitions), kafka::api_version(6)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp_2.data.responses.empty());
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

                partition->raft()
                  ->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result()
                  .get();
            }
            partition->raft()->step_down("trigger epoch change").get();
            wait_for_leader(ntp, 10s).get();
            {
                auto batches = chunked_vector<model::record_batch>(
                  std::from_range,
                  model::test::make_random_batches(model::offset(0), 5).get()
                    | std::views::as_rvalue);

                partition->raft()
                  ->replicate(
                    std::move(batches),
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
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
          .current_leader_epoch = kafka::leader_epoch(1),
          .fetch_offset = model::offset(6),
        }}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(9)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_MESSAGE(
      resp.data.responses[0].partitions[0].error_code
        == kafka::error_code::fenced_leader_epoch,
      fmt::format(
        "error: {}", resp.data.responses[0].partitions[0].error_code));
}

FIXTURE_TEST(fetch_current_leader_v12, cluster_test_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);

    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});
    wait_for_all_members(3s).get();

    model::ktp ktp(topic, pid);
    auto ntp = ktp.to_ntp();
    create_topic(ktp.as_tn_view(), 1, 3).get();

    wait_for(10s, [&] {
        auto [app_ptr, _] = get_leader(ntp);
        return app_ptr != nullptr;
    });

    auto [app_ptr, partition] = get_leader(ntp);

    auto publish_some = [ntp](redpanda_thread_fixture* app_ptr) {
        app_ptr->app.partition_manager
          .invoke_on(
            *app_ptr->app.shard_table.local().shard_for(ntp),
            [ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);

                auto batches
                  = model::test::make_random_batches(model::offset(0), 5).get();

                BOOST_TEST_INFO("Replicating batches to leader");

                partition->raft()
                  ->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .discard_result()
                  .get();
            })
          .get();
    };

    publish_some(app_ptr);

    BOOST_TEST_INFO("Shuffling leadership");
    shuffle_leadership(ntp).get();

    BOOST_TEST_INFO("Waiting for new leader");
    wait_for(10s, [&] {
        auto [fixture, _] = get_leader(ntp);
        return fixture != nullptr;
    });

    BOOST_TEST_INFO("Getting new leader");
    auto [new_app_ptr, new_partition] = get_leader(ntp);

    publish_some(new_app_ptr);

    // This is in lieu of a linearizable_barrier.
    BOOST_TEST_INFO("Waiting for metadata cache to update");
    wait_for(10s, [&] {
        cluster::leader_term expected{
          new_app_ptr->app.controller->self(), new_partition->raft()->term()};
        auto term = app_ptr->app.metadata_cache.local().get_leader_term(
          ktp.as_tn_view(), ktp.get_partition());
        return term == expected;
    });

    auto send_request =
      [&](redpanda_thread_fixture* app_ptr, model::term_id term) {
          kafka::fetch_request req;
          req.data.max_bytes = std::numeric_limits<int32_t>::max();
          req.data.min_bytes = 1;
          req.data.max_wait_ms = std::chrono::milliseconds(1000);
          req.data.topics.emplace_back(
            kafka::fetch_topic{
              .topic = topic,
              .partitions = {{
                .partition = pid,
                .current_leader_epoch = kafka::leader_epoch_from_term(term),
                .fetch_offset = model::offset(6),
              }}});

          auto client = instance(app_ptr->app.controller->self())
                          ->make_kafka_client()
                          .get();

          client.connect().get();

          auto response
            = client.dispatch(std::move(req), kafka::api_version(12)).get();
          client.stop().then([&client] { client.shutdown(); }).get();
          return response;
      };

    auto expected = kafka::leader_id_and_epoch{
      new_partition->raft()->get_leader_id().value_or(model::node_id{-1}),
      kafka::leader_epoch_from_term(new_partition->raft()->term())};

    {
        BOOST_TEST_INFO("Dispatching old epoch to old leader");
        auto resp = send_request(app_ptr, model::term_id{1});
        const auto& part = resp.data.responses[0].partitions[0];
        BOOST_REQUIRE_EQUAL(
          part.error_code, kafka::error_code::not_leader_for_partition);
        BOOST_REQUIRE_EQUAL(expected, part.current_leader);
    }
    {
        BOOST_TEST_INFO("Dispatching old epoch to new leader");
        auto resp = send_request(new_app_ptr, model::term_id{1});
        const auto& part = resp.data.responses[0].partitions[0];
        BOOST_REQUIRE_EQUAL(
          part.error_code, kafka::error_code::fenced_leader_epoch);
        BOOST_REQUIRE_EQUAL(expected, part.current_leader);
    }
    {
        BOOST_TEST_INFO("Dispatching new epoch to new leader");
        auto resp = send_request(new_app_ptr, model::term_id{2});
        const auto& part = resp.data.responses[0].partitions[0];
        BOOST_REQUIRE_EQUAL(part.error_code, kafka::error_code::none);
        // When there is not an error, leader_id_and_epoch should not be set
        BOOST_REQUIRE_EQUAL(model::node_id{-1}, part.current_leader.leader_id);
        BOOST_REQUIRE_EQUAL(
          kafka::leader_epoch{-1}, part.current_leader.leader_epoch);
    }
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
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {},
      });
    for (int i = 0; i < 6; ++i) {
        kafka::fetch_request::partition p;
        p.partition = model::partition_id(i);
        p.log_start_offset = offset;
        p.fetch_offset = offset;
        p.partition_max_bytes = std::numeric_limits<int32_t>::max();
        req.data.topics[0].partitions.push_back(p);
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
                      return partition->raft()->replicate(
                        chunked_vector<model::record_batch>(
                          std::from_range,
                          std::move(batches) | std::views::as_rvalue),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack));
                  });
            })
          .discard_result()
          .get();
    }
    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].topic, topic());
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].partitions.size(), 6);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].partitions[i].error_code,
          kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].partitions[i].partition_index,
          model::partition_id(i));
        BOOST_REQUIRE(resp.data.responses[0].partitions[i].records);

        total_size
          += resp.data.responses[0].partitions[i].records->size_bytes();
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
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
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
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::leader_ack));
              });
        })
      .discard_result()
      .get();

    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.responses.size() == 1);
    BOOST_REQUIRE(resp.data.responses[0].topic == topic());
    BOOST_REQUIRE(resp.data.responses[0].partitions.size() == 1);
    BOOST_REQUIRE(
      resp.data.responses[0].partitions[0].error_code
      == kafka::error_code::none);
    BOOST_REQUIRE(resp.data.responses[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(resp.data.responses[0].partitions[0].records);
    BOOST_REQUIRE(
      resp.data.responses[0].partitions[0].records->size_bytes() > 0);
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
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
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
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .discard_result()
      .get();

    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.responses.size() == 1);
    BOOST_REQUIRE(resp.data.responses[0].topic == topic());
    BOOST_REQUIRE(resp.data.responses[0].partitions.size() == 1);
    BOOST_REQUIRE(
      resp.data.responses[0].partitions[0].error_code
      == kafka::error_code::none);
    BOOST_REQUIRE(resp.data.responses[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(resp.data.responses[0].partitions[0].records);
    BOOST_REQUIRE(
      resp.data.responses[0].partitions[0].records->size_bytes() > 0);
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
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic_1,
        .partitions = {},
      });
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic_2,
        .partitions = {},
      });

    for (auto& ntp : ntps) {
        kafka::fetch_request::partition p;
        p.partition = model::partition_id(ntp.tp.partition);
        p.log_start_offset = zero;
        p.fetch_offset = zero;
        p.partition_max_bytes = std::numeric_limits<int32_t>::max();
        auto idx = ntp.tp.topic == topic_1 ? 0 : 1;
        req.data.topics[idx].partitions.push_back(p);
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
                      return partition->raft()->replicate(
                        chunked_vector<model::record_batch>(
                          std::from_range,
                          std::move(batches) | std::views::as_rvalue),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack));
                  });
            })
          .discard_result()
          .get();
    }

    auto resp = client.dispatch(std::move(req), kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 2);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].topic, topic_1);
    BOOST_REQUIRE_EQUAL(resp.data.responses[1].topic, topic_2);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].partitions.size(), 6);
    BOOST_REQUIRE_EQUAL(resp.data.responses[1].partitions.size(), 1);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].partitions[i].error_code,
          kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].partitions[i].partition_index,
          model::partition_id(i));
        BOOST_REQUIRE(resp.data.responses[0].partitions[i].records);

        total_size
          += resp.data.responses[0].partitions[i].records->size_bytes();
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
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
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
        req.data.topics.emplace_back(
          kafka::fetch_topic{
            .topic = topic,
            .partitions = {{
              .partition = pid,
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
    BOOST_REQUIRE(fetch_one_byte.data.responses.size() == 1);
    BOOST_REQUIRE(fetch_one_byte.data.responses[0].topic == topic());
    BOOST_REQUIRE(fetch_one_byte.data.responses[0].partitions.size() == 1);
    BOOST_REQUIRE(
      fetch_one_byte.data.responses[0].partitions[0].error_code
      == kafka::error_code::none);
    BOOST_REQUIRE(
      fetch_one_byte.data.responses[0].partitions[0].partition_index == pid);
    BOOST_REQUIRE(fetch_one_byte.data.responses[0].partitions[0].records);
    BOOST_REQUIRE(
      fetch_one_byte.data.responses[0].partitions[0].records->size_bytes() > 0);
}

FIXTURE_TEST(fetch_offset_out_of_range, redpanda_thread_fixture) {
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
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .get();

    auto trunc_err = app.partition_manager
                       .invoke_on(
                         *shard,
                         [ntp](cluster::partition_manager& mgr) {
                             auto partition = mgr.get(ntp);
                             auto k_trunc_offset = kafka::offset(5);
                             auto rp_trunc_offset
                               = partition->log()->to_log_offset(
                                 model::offset(k_trunc_offset));
                             return partition->prefix_truncate(
                               rp_trunc_offset,
                               k_trunc_offset,
                               ss::lowres_clock::time_point::max());
                         })
                       .get();
    BOOST_REQUIRE(!trunc_err);

    auto hwm = app.partition_manager
                 .invoke_on(
                   *shard,
                   [ntp](cluster::partition_manager& mgr) {
                       auto partition = mgr.get(ntp);
                       return partition->log()->from_log_offset(
                         partition->high_watermark());
                   })
                 .get();

    kafka::fetch_request req;
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(0);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
          .fetch_offset = model::offset(0),
        }},
      });

    auto client = make_kafka_client().get();
    client.connect().get();
    auto fresp = client.dispatch(std::move(req), kafka::api_version(5));

    auto resp = fresp.get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].partitions[0].error_code,
      kafka::error_code::offset_out_of_range);

    // This is used by the clients to determine what should be done with the
    // offset out of range error. See
    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].partitions[0].log_start_offset, model::offset(5));
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].partitions[0].high_watermark, hwm);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].partitions[0].last_stable_offset, hwm);
}

FIXTURE_TEST(fetch_response_bytes_eq_units, redpanda_thread_fixture) {
    // create topic with single partition
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
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .get();
    wait_for_partition_offset(ntp, model::offset(20)).get();

    auto conn_context = make_connection_context();
    conn_context->start().get();

    auto make_rctx = [&] {
        auto fetch_topics = chunked_vector<kafka::fetch_topic>{};

        fetch_topics.push_back(
          kafka::fetch_topic{
            .topic = topic,
            .partitions = {kafka::fetch_partition{
              .partition = pid,
              .current_leader_epoch = kafka::leader_epoch(-1),
              .fetch_offset = model::offset(0),
              .log_start_offset = model::offset(-1),
              .partition_max_bytes = 10 * MiB,
            }}});

        // create a request
        kafka::fetch_request_data frq_data{
          .replica_id = kafka::client::consumer_replica_id,
          .max_wait_ms = 500ms,
          .min_bytes = 1,
          .max_bytes = 50_MiB,
          .isolation_level = model::isolation_level::read_uncommitted,
          .session_id = kafka::invalid_fetch_session_id,
          .session_epoch = kafka::final_fetch_session_epoch,
          .topics = std::move(fetch_topics),
        };

        auto request = kafka::fetch_request{.data = std::move(frq_data)};

        kafka::request_header header{
          .key = kafka::fetch_handler::api::key,
          .version = kafka::api_version{12}};

        return make_request_context(std::move(request), header, conn_context);
    };

    auto octx = kafka::op_context(make_rctx(), ss::default_smp_service_group());
    kafka::testing::do_fetch(octx).get();
    conn_context->stop().get();

    BOOST_REQUIRE(octx.response_size > 0);
    BOOST_REQUIRE(octx.response_size == octx.total_response_memory_units());
}

// Regression test for CORE-14617: When a fetch is retried internally (due to
// min_bytes not being satisfied), partitions with changed metadata (like
// log_start_offset) must still be included in the final response.
FIXTURE_TEST(
  fetch_session_propagates_log_start_offset, redpanda_thread_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    auto ntp = make_default_ntp(topic, pid);

    wait_for_controller_leadership().get();
    add_topic(model::topic_namespace_view(ntp)).get();
    wait_for_partition_offset(ntp, model::offset(0)).get();

    // Produce some data
    auto shard = app.shard_table.local().shard_for(ntp);
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            return model::test::make_random_batches(model::offset(0), 20)
              .then([ntp, &mgr](auto batches) {
                  auto partition = mgr.get(ntp);
                  return partition->raft()->replicate(
                    chunked_vector<model::record_batch>(
                      std::from_range,
                      std::move(batches) | std::views::as_rvalue),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack));
              });
        })
      .get();

    auto client = make_kafka_client().get();
    client.connect().get();

    // Full fetch to establish session (session_epoch=0, invalid session_id)
    kafka::fetch_request req1;
    req1.data.max_bytes = std::numeric_limits<int32_t>::max();
    req1.data.min_bytes = 1;
    req1.data.max_wait_ms = 1000ms;
    req1.data.session_id = kafka::invalid_fetch_session_id;
    req1.data.session_epoch = kafka::initial_fetch_session_epoch;
    req1.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
          .fetch_offset = model::offset(5),
        }},
      });

    auto resp1 = client.dispatch(std::move(req1), kafka::api_version(12)).get();
    BOOST_REQUIRE_EQUAL(resp1.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp1.data.responses[0].partitions[0].error_code,
      kafka::error_code::none);
    BOOST_REQUIRE_NE(resp1.data.session_id, kafka::invalid_fetch_session_id);

    auto session_id = resp1.data.session_id;
    auto initial_log_start
      = resp1.data.responses[0].partitions[0].log_start_offset;

    // Prefix truncate to change log_start_offset
    auto trunc_err = app.partition_manager
                       .invoke_on(
                         *shard,
                         [ntp](cluster::partition_manager& mgr) {
                             auto partition = mgr.get(ntp);
                             auto k_trunc_offset = kafka::offset(5);
                             auto rp_trunc_offset
                               = partition->log()->to_log_offset(
                                 model::offset(k_trunc_offset));
                             return partition->prefix_truncate(
                               rp_trunc_offset,
                               k_trunc_offset,
                               ss::lowres_clock::time_point::max());
                         })
                       .get();
    BOOST_REQUIRE(!trunc_err);

    // Incremental fetch with min_bytes=1 - will retry internally waiting for
    // data, but should still include the partition due to log_start_offset
    // change even if no new data arrives during the retry window.
    kafka::fetch_request req2;
    req2.data.max_bytes = std::numeric_limits<int32_t>::max();
    req2.data.min_bytes = 1;
    req2.data.max_wait_ms = 1000ms;
    req2.data.session_id = session_id;
    req2.data.session_epoch = kafka::fetch_session_epoch(1);
    req2.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {{
          .partition = pid,
          .fetch_offset = model::offset(20),
        }},
      });

    auto resp2 = client.dispatch(std::move(req2), kafka::api_version(12)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    // The partition must be included even though no new data arrived, because
    // log_start_offset changed. This is the regression test for CORE-14617.
    BOOST_REQUIRE_EQUAL(resp2.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(resp2.data.responses[0].partitions.size(), 1);
    auto new_log_start = resp2.data.responses[0].partitions[0].log_start_offset;
    BOOST_REQUIRE_GT(new_log_start, initial_log_start);
    BOOST_REQUIRE_EQUAL(new_log_start, model::offset(5));
}

// Regression test: when one partition's log_eviction_stm has its gate closed
// (e.g. during shutdown/partition move), the multi-partition fetch should
// still succeed for the remaining healthy partitions rather than failing the
// entire request.
FIXTURE_TEST(
  fetch_multi_partition_with_mixed_failures, redpanda_thread_fixture) {
    model::topic topic("foo");
    constexpr int num_partitions = 4;
    // The partition whose STM we will stop.
    constexpr int stopped_partition_idx = 1;

    wait_for_controller_leadership().get();
    add_topic(model::topic_namespace(model::ns("kafka"), topic), num_partitions)
      .get();

    for (int i = 0; i < num_partitions; ++i) {
        auto ntp = make_default_ntp(topic, model::partition_id(i));
        wait_for_partition_offset(ntp, model::offset(0)).get();
    }

    // Write data to all partitions.
    for (int i = 0; i < num_partitions; ++i) {
        auto ntp = make_default_ntp(topic, model::partition_id(i));
        auto shard = app.shard_table.local().shard_for(ntp);
        app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                return model::test::make_random_batches(model::offset(0), 5)
                  .then([ntp, &mgr](auto batches) {
                      auto partition = mgr.get(ntp);
                      return partition->raft()->replicate(
                        chunked_vector<model::record_batch>(
                          std::from_range,
                          std::move(batches) | std::views::as_rvalue),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack));
                  });
            })
          .discard_result()
          .get();
    }

    // Close the log_eviction_stm's gate on one partition to simulate a
    // shutdown race. sync_kafka_start_offset_override will encounter a
    // gate_closed_exception on this partition.
    auto stopped_ntp = make_default_ntp(
      topic, model::partition_id(stopped_partition_idx));
    auto stopped_shard = app.shard_table.local().shard_for(stopped_ntp);
    app.partition_manager
      .invoke_on(
        *stopped_shard,
        [stopped_ntp](cluster::partition_manager& mgr) {
            auto partition = mgr.get(stopped_ntp);
            auto stm = partition->raft()
                         ->stm_manager()
                         ->get<cluster::log_eviction_stm>();
            using accessor = cluster::testing::log_eviction_stm_accessor;
            accessor::request_abort(*stm);
            accessor::break_has_pending_truncation(*stm);
            return accessor::close_gate(*stm);
        })
      .get();

    // Build a fetch request spanning all partitions.
    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1;
    req.data.max_wait_ms = std::chrono::milliseconds(0);
    req.data.session_id = kafka::invalid_fetch_session_id;
    req.data.topics.emplace_back(
      kafka::fetch_topic{
        .topic = topic,
        .partitions = {},
      });
    for (int i = 0; i < num_partitions; ++i) {
        kafka::fetch_request::partition p;
        p.partition = model::partition_id(i);
        p.fetch_offset = model::offset(0);
        p.partition_max_bytes = std::numeric_limits<int32_t>::max();
        req.data.topics[0].partitions.push_back(p);
    }

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    // The fetch must return responses for all partitions.
    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].partitions.size(), num_partitions);

    for (int i = 0; i < num_partitions; ++i) {
        const auto& partition_resp = resp.data.responses[0].partitions[i];
        BOOST_REQUIRE_EQUAL(
          partition_resp.partition_index, model::partition_id(i));

        if (i == stopped_partition_idx) {
            // The stopped partition should return an error, not crash the
            // fetch.
            BOOST_REQUIRE(partition_resp.error_code != kafka::error_code::none);
        } else {
            // Healthy partitions must return data successfully.
            BOOST_REQUIRE_EQUAL(
              partition_resp.error_code, kafka::error_code::none);
            BOOST_REQUIRE(partition_resp.records);
            BOOST_REQUIRE_GT(partition_resp.records->size_bytes(), 0);
        }
    }

    // Reset the STM state so the fixture can shut down cleanly.
    app.partition_manager
      .invoke_on(
        *stopped_shard,
        [stopped_ntp](cluster::partition_manager& mgr) {
            auto partition = mgr.get(stopped_ntp);
            auto stm = partition->raft()
                         ->stm_manager()
                         ->get<cluster::log_eviction_stm>();
            using accessor = cluster::testing::log_eviction_stm_accessor;
            accessor::reset_gate(*stm);
            accessor::reset_abort_source(*stm);
        })
      .get();
}
