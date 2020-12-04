// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/fetch_request.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

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
              return model::topic_partition(v.topic->name, v.partition->id);
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
        req.topics.push_back({.name = model::topic("t0")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 2 topics, no partitions -> empty
        kafka::fetch_request req;
        req.topics.push_back({.name = model::topic("t0")});
        req.topics.push_back({.name = model::topic("t1")});
        auto parts = transform(req);
        BOOST_TEST(parts.empty());
    }

    {
        // 1 topic, 1 partition
        kafka::fetch_request req;
        req.topics.push_back({
          .name = model::topic("t0"),
          .partitions = {{.id = model::partition_id(100)}},
        });
        auto parts = transform(req);
        BOOST_TEST(parts.size() == 1);
        BOOST_TEST(parts[0].topic == model::topic("t0"));
        BOOST_TEST(parts[0].partition == 100);
    }

    {
        // 1 topic, 2 partitions
        kafka::fetch_request req;
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {
             {.id = model::partition_id(100)},
             {.id = model::partition_id(101)}}});
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
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {
             {.id = model::partition_id(100)},
             {.id = model::partition_id(101)}}});
        req.topics.push_back(
          {.name = model::topic("t1"),
           .partitions = {{.id = model::partition_id(102)}}});
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
        req.topics.push_back(
          {.name = model::topic("t0"),
           .partitions = {
             {.id = model::partition_id(100)},
             {.id = model::partition_id(101)}}});
        req.topics.push_back({.name = model::topic("t1")});
        req.topics.push_back({.name = model::topic("t2")});
        req.topics.push_back(
          {.name = model::topic("t3"),
           .partitions = {
             {.id = model::partition_id(102)},
             {.id = model::partition_id(103)}}});
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
    auto do_read = [this](model::ntp ntp, size_t max_bytes) {
        kafka::fetch_config config{
          .start_offset = model::offset(0),
          .max_bytes = max_bytes,
          .timeout = model::no_timeout,
        };
        auto rctx = make_request_context();
        auto octx = kafka::op_context(
          std::move(rctx), ss::default_smp_service_group());
        auto resp = kafka::read_from_ntp(octx, ntp, config).get0();
        BOOST_REQUIRE(resp.record_set);
        return resp;
    };
    wait_for_controller_leadership().get0();
    auto ntp = make_data(storage::ntp_config::ntp_id(2));

    auto shard = app.shard_table.local().shard_for(ntp);
    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp = ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto zero = do_read(ntp, 0).record_set->size_bytes();
    auto one = do_read(ntp, 1).record_set->size_bytes();
    auto maxlimit = do_read(ntp, std::numeric_limits<size_t>::max())
                      .record_set->size_bytes();

    BOOST_TEST(zero > 0); // read something
    BOOST_TEST(zero == one);
    BOOST_TEST(one <= maxlimit); // read more
}

FIXTURE_TEST(fetch_one, redpanda_thread_fixture) {
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
          ntp, log_config.base_dir, nullptr, storage::ntp_config::ntp_id(2));
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batch(model::offset(0), 10, maybe_compress_batches::yes)
          | stop();
    }
    wait_for_controller_leadership().get0();

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
           kafka::fetch_response::api_type::min_supported,
           kafka::fetch_response::api_type::max_supported + int16_t(1))) {
        info("Checking fetch api v{}", version);
        kafka::fetch_request req;
        req.max_bytes = std::numeric_limits<int32_t>::max();
        req.min_bytes = 1;
        req.max_wait_time = std::chrono::milliseconds(0);
        // disable incremental fetches
        req.session_id = kafka::invalid_fetch_session_id;
        req.session_epoch = kafka::final_fetch_session_epoch;
        req.topics = {{
          .name = topic,
          .partitions = {{
            .id = pid,
            .fetch_offset = offset,
          }},
        }};

        auto client = make_kafka_client().get0();
        client.connect().get();
        auto resp = client.dispatch(req, kafka::api_version(version)).get0();
        client.stop().then([&client] { client.shutdown(); }).get();

        BOOST_REQUIRE(resp.partitions.size() == 1);
        BOOST_REQUIRE(resp.partitions[0].name == topic());
        BOOST_REQUIRE(resp.partitions[0].responses.size() == 1);
        BOOST_REQUIRE(
          resp.partitions[0].responses[0].error == kafka::error_code::none);
        BOOST_REQUIRE(resp.partitions[0].responses[0].id == pid);
        BOOST_REQUIRE(resp.partitions[0].responses[0].record_set);
        BOOST_REQUIRE(
          resp.partitions[0].responses[0].record_set->size_bytes() > 0);
    }
}

FIXTURE_TEST(fetch_response_iterator_test, redpanda_thread_fixture) {
    static auto make_partition = [](ss::sstring topic) {
        return kafka::fetch_response::partition(model::topic(std::move(topic)));
    };

    static auto make_partition_response = [](int id) {
        kafka::fetch_response::partition_response resp;
        resp.error = kafka::error_code::none;
        resp.id = model::partition_id(id);
        resp.last_stable_offset = model::offset(0);
        return resp;
    };

    auto make_test_fetch_response = []() {
        kafka::fetch_response response;
        response.partitions.push_back(make_partition("tp-1"));
        response.partitions.push_back(make_partition("tp-2"));
        response.partitions.push_back(make_partition("tp-3"));

        response.partitions[0].responses.push_back(make_partition_response(0));
        response.partitions[0].responses.push_back(make_partition_response(1));
        response.partitions[0].responses.push_back(make_partition_response(2));

        response.partitions[1].responses.push_back(make_partition_response(0));

        response.partitions[2].responses.push_back(make_partition_response(0));
        response.partitions[2].responses.push_back(make_partition_response(1));
        return response;
    };
    kafka::op_context ctx(
      make_request_context(), ss::default_smp_service_group());
    auto response = make_test_fetch_response();
    ctx.response = make_test_fetch_response();
    auto wrapper_iterator = ctx.response_begin();
    int i = 0;

    for (auto it = response.begin(); it != response.end(); ++it) {
        if (i < 3) {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-1");
            BOOST_REQUIRE_EQUAL(it->partition_response->id(), i);
        } else if (i == 3) {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-2");
            BOOST_REQUIRE_EQUAL(it->partition_response->id(), 0);
        } else {
            BOOST_REQUIRE_EQUAL(it->partition->name(), "tp-3");
            BOOST_REQUIRE_EQUAL(it->partition_response->id(), i - 4);
        }
        BOOST_REQUIRE_EQUAL(
          it->partition->name, wrapper_iterator->partition->name);
        BOOST_REQUIRE_EQUAL(
          wrapper_iterator->partition_response->id,
          wrapper_iterator->partition_response->id);
        ++i;
        ++wrapper_iterator;
    }
};

FIXTURE_TEST(fetch_empty, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    wait_for_controller_leadership().get0();
    add_topic(model::topic_namespace_view(ntp)).get();

    wait_for_partition_offset(ntp, model::offset(0)).get0();

    kafka::fetch_request no_topics;
    no_topics.max_bytes = std::numeric_limits<int32_t>::max();
    no_topics.min_bytes = 1;
    no_topics.max_wait_time = std::chrono::milliseconds(1000);

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp_1 = client.dispatch(no_topics, kafka::api_version(6)).get0();

    BOOST_REQUIRE(resp_1.partitions.empty());

    kafka::fetch_request no_partitions;
    no_partitions.max_bytes = std::numeric_limits<int32_t>::max();
    no_partitions.min_bytes = 1;
    no_partitions.max_wait_time = std::chrono::milliseconds(1000);
    no_partitions.topics = {{.name = topic, .partitions = {}}};

    auto resp_2 = client.dispatch(no_topics, kafka::api_version(6)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp_2.partitions.empty());
}

FIXTURE_TEST(fetch_multi_partitions_debounce, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);

    wait_for_controller_leadership().get0();

    add_topic(model::topic_namespace(model::ns("kafka"), topic), 6).get();

    for (int i = 0; i < 6; ++i) {
        auto ntp = make_default_ntp(topic, model::partition_id(i));
        wait_for_partition_offset(ntp, model::offset(0)).get0();
    }

    kafka::fetch_request req;
    req.max_bytes = std::numeric_limits<int32_t>::max();
    req.min_bytes = 1;
    req.max_wait_time = std::chrono::milliseconds(3000);
    req.session_id = kafka::invalid_fetch_session_id;
    req.topics = {{
      .name = topic,
      .partitions = {},
    }};
    for (int i = 0; i < 6; ++i) {
        kafka::fetch_request::partition p;
        p.id = model::partition_id(i);
        p.log_start_offset = offset;
        p.fetch_offset = offset;
        p.partition_max_bytes = std::numeric_limits<int32_t>::max();
        req.topics[0].partitions.push_back(p);
    }
    auto client = make_kafka_client().get0();
    client.connect().get();
    auto fresp = client.dispatch(req, kafka::api_version(4));

    for (int i = 0; i < 6; ++i) {
        model::partition_id partition_id(i);
        auto ntp = make_default_ntp(topic, partition_id);
        auto shard = app.shard_table.local().shard_for(ntp);
        auto r = app.partition_manager
                   .invoke_on(
                     *shard,
                     [ntp](cluster::partition_manager& mgr) {
                         auto partition = mgr.get(ntp);
                         auto batches = storage::test::make_random_batches(
                           model::offset(0), 5);
                         auto rdr = model::make_memory_record_batch_reader(
                           std::move(batches));
                         return partition->replicate(
                           std::move(rdr),
                           raft::replicate_options(
                             raft::consistency_level::quorum_ack));
                     })
                   .get0();
    }
    auto resp = fresp.get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.partitions[0].name, topic());
    BOOST_REQUIRE_EQUAL(resp.partitions[0].responses.size(), 6);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.partitions[0].responses[i].error, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.partitions[0].responses[i].id, model::partition_id(i));
        BOOST_REQUIRE(resp.partitions[0].responses[i].record_set);

        total_size += resp.partitions[0].responses[i].record_set->size_bytes();
    }
    BOOST_REQUIRE_GT(total_size, 0);
}

FIXTURE_TEST(fetch_one_debounce, redpanda_thread_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);

    wait_for_controller_leadership().get0();

    add_topic(model::topic_namespace_view(ntp)).get();
    wait_for_partition_offset(ntp, model::offset(0)).get0();

    kafka::fetch_request req;
    req.max_bytes = std::numeric_limits<int32_t>::max();
    req.min_bytes = 1;
    req.max_wait_time = std::chrono::milliseconds(5000);
    req.session_id = kafka::invalid_fetch_session_id;
    req.topics = {{
      .name = topic,
      .partitions = {{
        .id = pid,
        .fetch_offset = offset,
      }},
    }};

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto fresp = client.dispatch(req, kafka::api_version(4));
    auto shard = app.shard_table.local().shard_for(ntp);
    auto r = app.partition_manager
               .invoke_on(
                 *shard,
                 [ntp](cluster::partition_manager& mgr) {
                     auto partition = mgr.get(ntp);
                     auto batches = storage::test::make_random_batches(
                       model::offset(0), 5);
                     auto rdr = model::make_memory_record_batch_reader(
                       std::move(batches));
                     return partition->replicate(
                       std::move(rdr),
                       raft::replicate_options(
                         raft::consistency_level::quorum_ack));
                 })
               .get0();

    auto resp = fresp.get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.partitions.size() == 1);
    BOOST_REQUIRE(resp.partitions[0].name == topic());
    BOOST_REQUIRE(resp.partitions[0].responses.size() == 1);
    BOOST_REQUIRE(
      resp.partitions[0].responses[0].error == kafka::error_code::none);
    BOOST_REQUIRE(resp.partitions[0].responses[0].id == pid);
    BOOST_REQUIRE(resp.partitions[0].responses[0].record_set);
    BOOST_REQUIRE(resp.partitions[0].responses[0].record_set->size_bytes() > 0);
}

FIXTURE_TEST(fetch_multi_topics, redpanda_thread_fixture) {
    // create a topic partition with some data
    model::topic topic_1("foo");
    model::topic topic_2("bar");
    model::offset zero(0);
    wait_for_controller_leadership().get0();

    add_topic(model::topic_namespace(model::ns("kafka"), topic_1), 6).get();
    add_topic(model::topic_namespace(model::ns("kafka"), topic_2), 1).get();

    std::vector<model::ntp> ntps = {};
    // topic 1
    for (int i = 0; i < 6; ++i) {
        ntps.push_back(make_default_ntp(topic_1, model::partition_id(i)));
        wait_for_partition_offset(ntps.back(), model::offset(0)).get0();
    }
    // topic 2
    ntps.push_back(make_default_ntp(topic_2, model::partition_id(0)));
    wait_for_partition_offset(ntps.back(), model::offset(0)).get0();

    // request
    kafka::fetch_request req;
    req.max_bytes = std::numeric_limits<int32_t>::max();
    req.min_bytes = 1;
    req.max_wait_time = std::chrono::milliseconds(3000);
    req.session_id = kafka::invalid_fetch_session_id;
    req.topics = {
      {
        .name = topic_1,
        .partitions = {},
      },
      {
        .name = topic_2,
        .partitions = {},
      }};

    for (auto& ntp : ntps) {
        kafka::fetch_request::partition p;
        p.id = model::partition_id(ntp.tp.partition);
        p.log_start_offset = zero;
        p.fetch_offset = zero;
        p.partition_max_bytes = std::numeric_limits<int32_t>::max();
        auto idx = ntp.tp.topic == topic_1 ? 0 : 1;
        req.topics[idx].partitions.push_back(p);
    }

    auto client = make_kafka_client().get0();
    client.connect().get();
    // add date to all partitions
    for (auto& ntp : ntps) {
        auto shard = app.shard_table.local().shard_for(ntp);
        auto r = app.partition_manager
                   .invoke_on(
                     *shard,
                     [ntp](cluster::partition_manager& mgr) {
                         auto partition = mgr.get(ntp);
                         auto batches = storage::test::make_random_batches(
                           model::offset(0), 5);
                         auto rdr = model::make_memory_record_batch_reader(
                           std::move(batches));
                         return partition->replicate(
                           std::move(rdr),
                           raft::replicate_options(
                             raft::consistency_level::quorum_ack));
                     })
                   .get0();
    }

    auto resp = client.dispatch(req, kafka::api_version(4)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.partitions.size(), 2);
    BOOST_REQUIRE_EQUAL(resp.partitions[0].name, topic_1);
    BOOST_REQUIRE_EQUAL(resp.partitions[1].name, topic_2);
    BOOST_REQUIRE_EQUAL(resp.partitions[0].responses.size(), 6);
    BOOST_REQUIRE_EQUAL(resp.partitions[1].responses.size(), 1);
    size_t total_size = 0;
    for (int i = 0; i < 6; ++i) {
        BOOST_REQUIRE_EQUAL(
          resp.partitions[0].responses[i].error, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.partitions[0].responses[i].id, model::partition_id(i));
        BOOST_REQUIRE(resp.partitions[0].responses[i].record_set);

        total_size += resp.partitions[0].responses[i].record_set->size_bytes();
    }
    BOOST_REQUIRE_GT(total_size, 0);
}
