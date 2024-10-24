// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/alter_partition_reassignments.h"
#include "kafka/protocol/list_partition_reassignments.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/list_partition_reassignments_request.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>

using namespace std::chrono_literals; // NOLINT

inline ss::logger test_log("test"); // NOLINT

class partition_reassignments_test_fixture : public redpanda_thread_fixture {
public:
    void create_topic(model::topic name, int partitions) {
        model::topic_namespace tp_ns(model::kafka_namespace, std::move(name));

        add_topic(tp_ns, partitions).get();

        ss::parallel_for_each(
          boost::irange(0, partitions),
          [this, tp_ns](int i) {
              return wait_for_partition_offset(
                model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id(i)),
                model::offset(0));
          })
          .get();
    }

    kafka::alter_partition_reassignments_response alter_partition_reassignments(
      kafka::client::transport& client,
      std::optional<model::topic> topic_name,
      std::vector<kafka::reassignable_partition> reassignable_partitions) {
        kafka::alter_partition_reassignments_request req;

        if (topic_name.has_value()) {
            req.data.topics = chunked_vector<kafka::reassignable_topic>{
              kafka::reassignable_topic{
                .name = *topic_name,
                .partitions = std::move(reassignable_partitions)}};
        }
        return client.dispatch(std::move(req), kafka::api_version(0)).get();
    }

    kafka::list_partition_reassignments_response list_partition_reassignments(
      kafka::client::transport& client,
      std::optional<chunked_vector<kafka::list_partition_reassignments_topics>>
        topics
      = std::nullopt) {
        kafka::list_partition_reassignments_request req;
        req.data.topics = std::move(topics);
        return client.dispatch(std::move(req), kafka::api_version(0)).get();
    }
};

FIXTURE_TEST(
  test_alter_partition_reassignments, partition_reassignments_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    auto client = make_kafka_client().get();
    client.connect().get();
    model::partition_id pid0{0};

    {
        test_log.info("Empty topics");
        auto resp = alter_partition_reassignments(
          client, std::nullopt, std::vector<kafka::reassignable_partition>{});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 0);
    }

    {
        test_log.info("Empty partitions");
        auto resp = alter_partition_reassignments(
          client, test_tp, std::vector<kafka::reassignable_partition>{});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 0);
    }

    {
        test_log.info("Empty replicas (!= undefined replicas)");
        kafka::reassignable_partition new_partition{
          .partition_index = pid0, .replicas = std::vector<model::node_id>{}};
        auto resp = alter_partition_reassignments(
          client,
          test_tp,
          std::vector<kafka::reassignable_partition>{new_partition});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == test_tp());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::invalid_replica_assignment);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{
                    "Empty replica list specified in partition reassignment."});
            }
        }
    }

    {
        test_log.info("Duplicate node ids");
        std::vector<model::node_id> new_replicas = {
          model::node_id{0}, model::node_id{0}};
        kafka::reassignable_partition new_partition{
          .partition_index = pid0, .replicas = std::move(new_replicas)};
        auto resp = alter_partition_reassignments(
          client,
          test_tp,
          std::vector<kafka::reassignable_partition>{new_partition});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == test_tp());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::invalid_replica_assignment);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{"Duplicate replica ids in partition "
                                 "reassignment replica list"});
            }
        }
    }

    {
        test_log.info("Negative node ids");
        std::vector<model::node_id> new_replicas = {model::node_id{-1}};
        kafka::reassignable_partition new_partition{
          .partition_index = pid0, .replicas = std::move(new_replicas)};
        auto resp = alter_partition_reassignments(
          client,
          test_tp,
          std::vector<kafka::reassignable_partition>{new_partition});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == test_tp());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::invalid_replica_assignment);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{"Invalid broker id in replica list"});
            }
        }
    }

    {
        test_log.info("Unknown node id");
        std::vector<model::node_id> new_replicas = {model::node_id{4}};
        kafka::reassignable_partition new_partition{
          .partition_index = pid0, .replicas = std::move(new_replicas)};
        auto resp = alter_partition_reassignments(
          client,
          test_tp,
          std::vector<kafka::reassignable_partition>{new_partition});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == test_tp());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::invalid_replica_assignment);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{
                    "Replica assignment has brokers that are not alive"});
            }
        }
    }

    {
        test_log.info("Cancel request expect fail");
        auto resp = alter_partition_reassignments(
          client,
          test_tp,
          std::vector<kafka::reassignable_partition>{
            kafka::reassignable_partition{
              .partition_index = model::partition_id{0}}});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == test_tp());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::no_reassignment_in_progress);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{"no_reassignment_in_progress"});
            }
        }
    }

    {
        test_log.info("Unknown topic");
        model::topic topic_dne{"topic-dne"};
        std::vector<model::node_id> new_replicas = {model::node_id{1}};
        kafka::reassignable_partition new_partition{
          .partition_index = pid0, .replicas = std::move(new_replicas)};
        auto resp = alter_partition_reassignments(
          client,
          topic_dne,
          std::vector<kafka::reassignable_partition>{new_partition});
        BOOST_CHECK_EQUAL(resp.data.responses.size(), 1);
        for (auto& tp_resp : resp.data.responses) {
            BOOST_CHECK(tp_resp.name() == topic_dne());
            BOOST_CHECK_EQUAL(tp_resp.partitions.size(), 1);
            for (auto& p_resp : tp_resp.partitions) {
                BOOST_CHECK_EQUAL(p_resp.partition_index(), pid0());
                BOOST_CHECK_EQUAL(
                  p_resp.error_code,
                  kafka::error_code::unknown_topic_or_partition);
                BOOST_CHECK(
                  *p_resp.error_message
                  == ss::sstring{"Topic or partition is undefined"});
            }
        }
    }

    client.stop().then([&client] { client.shutdown(); }).get();
}

FIXTURE_TEST(
  test_list_partition_reassignments, partition_reassignments_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    int num_partitions = 6;
    create_topic(test_tp, num_partitions);

    auto client = make_kafka_client().get();
    client.connect().get();

    {
        test_log.info(
          "List partition assignments {} expect an empty response", test_tp);
        std::vector<model::partition_id> pids;
        pids.reserve(num_partitions);
        for (int pid = 0; pid < num_partitions; ++pid) {
            pids.emplace_back(pid);
        }
        auto resp = list_partition_reassignments(
          client,
          chunked_vector<kafka::list_partition_reassignments_topics>{
            kafka::list_partition_reassignments_topics{
              .name = test_tp, .partition_indexes = pids}});
        BOOST_CHECK_EQUAL(resp.data.topics.size(), 0);
    }

    {
        test_log.info("List all ongoing reassignments expect an empty reponse");
        auto resp = list_partition_reassignments(client);
        BOOST_CHECK_EQUAL(resp.data.topics.size(), 0);
    }

    client.stop().then([&client] { client.shutdown(); }).get();
}
