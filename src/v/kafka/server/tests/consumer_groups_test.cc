// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_api.h"
#include "kafka/client/client.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group.h"
#include "kafka/server/group_manager.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "utils/base64.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace kafka;
join_group_request make_join_group_request(
  ss::sstring member_id,
  ss::sstring gr,
  std::vector<ss::sstring> protocols,
  ss::sstring protocol_type) {
    join_group_request req;
    req.data.group_id = kafka::group_id(std::move(gr));
    req.data.member_id = kafka::member_id(std::move(member_id));
    req.data.protocol_type = kafka::protocol_type(std::move(protocol_type));
    for (auto& p : protocols) {
        req.data.protocols.push_back(join_group_request_protocol{
          .name = protocol_name(std::move(p)), .metadata = bytes{}});
    }
    req.data.session_timeout_ms = 10s;
    return req;
}
struct consumer_offsets_fixture : public redpanda_thread_fixture {
    void
    wait_for_consumer_offsets_topic(const kafka::group_instance_id& group) {
        auto client = make_kafka_client().get();

        client.connect().get();
        kafka::find_coordinator_request req(group);
        req.data.key_type = kafka::coordinator_type::group;
        client.dispatch(std::move(req), kafka::api_version(1)).get();

        app.controller->get_api()
          .local()
          .wait_for_topic(
            model::kafka_consumer_offsets_nt, model::timeout_clock::now() + 30s)
          .get();

        tests::cooperative_spin_wait_with_timeout(30s, [&group, &client] {
            kafka::describe_groups_request req;
            req.data.groups.emplace_back(group);
            return client.dispatch(std::move(req), kafka::api_version(1))
              .then([](kafka::describe_groups_response response) {
                  return response.data.groups.front().error_code
                         == kafka::error_code::none;
              });
        }).get();

        client.stop().get();
        client.shutdown();
    }
};

FIXTURE_TEST(join_empty_group_static_member, consumer_offsets_fixture) {
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    tests::cooperative_spin_wait_with_timeout(30s, [&gr, &client] {
        auto req = make_join_group_request(
          unknown_member_id, "group-test", {"p1", "p2"}, "random");
        // set group instance id
        req.data.group_instance_id = gr;
        return client.dispatch(std::move(req), kafka::api_version(5))
          .then([&](auto resp) {
              BOOST_REQUIRE(
                resp.data.error_code == kafka::error_code::none
                || resp.data.error_code == kafka::error_code::not_coordinator);
              return resp.data.error_code == kafka::error_code::none
                     && resp.data.member_id != unknown_member_id;
          });
    }).get();
}

FIXTURE_TEST(empty_offset_commit_request, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    add_topic(
      model::topic_namespace_view{model::kafka_namespace, model::topic{"foo"}})
      .get();
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();
    // Regression test for a crash that we would previously see with empty
    // requests.
    // NOTE: errors are stored in partition fields of the response. Since the
    // requests are empty, there are no errors.
    {
        auto req = offset_commit_request{
          .data{.group_id = kafka::group_id{"foo-topics"}, .topics = {}}};
        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
    {
        offset_commit_request req;
        req.data = offset_commit_request_data{
          .group_id = kafka::group_id{"foo-partitions"},
          .topics = chunked_vector<offset_commit_request_topic>::single(
            offset_commit_request_topic{
              .name = model::topic{"foo"}, .partitions = {}})};

        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
}

FIXTURE_TEST(block_test, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);

    model::topic t{"topic"};
    kafka::group_id g{"group"};
    kafka::group_instance_id gi("group-instance");

    add_topic(model::topic_namespace_view{model::kafka_namespace, t}).get();
    wait_for_consumer_offsets_topic(gi);

    auto client = std::make_unique<kafka::client::transport>(
      make_kafka_client().get());
    auto client_stop = [&client] {
        client->stop().then([&client] { client->shutdown(); }).get();
    };
    auto deferred = ss::defer(decltype(client_stop)(client_stop));
    client->connect().get();

    auto g_partition = app.coordinator_ntp_mapper.local().partition_for(g);
    BOOST_REQUIRE(g_partition);
    model::ntp gntp(
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      *g_partition);

    auto can_commit_offset = [&] {
        for (int _ : std::views::iota(0, 5)) {
            auto req = offset_commit_request{.data{
              .group_id = g,
              .group_instance_id = gi,
              .topics = chunked_vector<offset_commit_request_topic>::single(
                offset_commit_request_topic{
                  .name = t,
                  .partitions
                  = chunked_vector<offset_commit_request_partition>::single(
                    offset_commit_request_partition{
                      .partition_index = model::partition_id{0},
                      .committed_offset = model::offset{0}})})}};
            auto res
              = client->dispatch(std::move(req), kafka::api_version(7)).get();
            if (!res.data.errored()) {
                return true;
            }
            if (
              res.data.topics[0].partitions[0].error_code
              != kafka::error_code::not_coordinator) {
                return false;
            }
        }
        return false;
    };

    auto set_blocked = [&](bool blocked) {
        auto res = app._group_manager.local()
                     .set_blocked_for_groups(gntp, {g}, blocked)
                     .get();
        BOOST_REQUIRE(res.has_value());
    };

    auto restart = [&] {
        client_stop();
        consumer_offsets_fixture::restart(should_wipe::no);
        wait_for_consumer_offsets_topic(gi);
        wait_for_leader(gntp).get();
        client = std::make_unique<kafka::client::transport>(
          make_kafka_client().get());
        client->connect().get();
    };

    BOOST_REQUIRE(can_commit_offset());

    // block
    set_blocked(true);
    BOOST_REQUIRE(!can_commit_offset());

    // make sure retart keeps it blocked
    restart();
    BOOST_REQUIRE(!can_commit_offset());

    // unblock
    set_blocked(false);
    BOOST_REQUIRE(can_commit_offset());

    // make sure retart keeps it unblocked
    restart();
    BOOST_REQUIRE(can_commit_offset());
}

FIXTURE_TEST(conditional_retention_test, consumer_offsets_fixture) {
    scoped_config cfg;
    cfg.get("group_topic_partitions").set_value(1);
    // setting to true to begin with, so log_eviction_stm is attached to
    // the partition.
    cfg.get("unsafe_enable_consumer_offsets_delete_retention").set_value(true);
    add_topic(
      model::topic_namespace_view{model::kafka_namespace, model::topic{"foo"}})
      .get();
    kafka::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    // load some data into the topic via offset_commit requests.
    auto client = make_kafka_client().get();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();
    auto offset = 0;
    auto rand_offset_commit = [&] {
        auto req_part = offset_commit_request_partition{
          .partition_index = model::partition_id{0},
          .committed_offset = model::offset{offset++}};
        auto topic = offset_commit_request_topic{
          .name = model::topic{"foo"}, .partitions = {std::move(req_part)}};

        return offset_commit_request{.data{
          .group_id = kafka::group_id{fmt::format("foo-{}", offset)},
          .topics = chunked_vector<offset_commit_request_topic>::single(
            std::move(topic))}};
    };
    for (int i = 0; i < 10; i++) {
        auto req = rand_offset_commit();
        req.data.group_instance_id = gr;
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(7)).get();
        BOOST_REQUIRE(!resp.data.errored());
    }
    auto part = app.partition_manager.local().get(model::ntp{
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      model::partition_id{0}});
    BOOST_REQUIRE(part);
    auto log = part->log();
    storage::ntp_config::default_overrides ov;
    ov.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion
                                 | model::cleanup_policy_bitflags::compaction;
    log->set_overrides(ov);
    log->notify_compaction_update();
    log->flush().get();
    log->force_roll().get();
    for (auto retention_enabled : {false, true}) {
        // number of partitions of CO topic.
        cfg.get("unsafe_enable_consumer_offsets_delete_retention")
          .set_value(retention_enabled);
        // attempt a GC on the partition log.
        // evict the first segment.
        storage::gc_config gc_cfg{model::timestamp::max(), 1};
        log->gc(gc_cfg).get();
        // Check if retention works
        try {
            tests::cooperative_spin_wait_with_timeout(5s, [&] {
                return log.get()->offsets().start_offset > model::offset{0};
            }).get();
        } catch (const ss::timed_out_error& e) {
            if (retention_enabled) {
                std::rethrow_exception(std::make_exception_ptr(e));
            }
        }
    }
}

SEASTAR_THREAD_TEST_CASE(consumer_group_decode) {
    {
        // snatched from a log message after a franz-go client joined
        auto data = bytes_to_iobuf(
          base64_to_bytes("AAEAAAADAAJ0MAACdDEAAnQyAAAACAAAAAAAAAAAAAAAAA=="));
        const auto topics = group::decode_consumer_subscriptions(
          std::move(data));
        BOOST_REQUIRE(
          topics
          == absl::node_hash_set<model::topic>(
            {model::topic("t0"), model::topic("t1"), model::topic("t2")}));
    }
}
