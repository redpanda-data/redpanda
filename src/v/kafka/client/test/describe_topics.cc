// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "test_utils/boost_fixture.h"

#include <boost/test/tools/old/interface.hpp>

class describe_topic_fixture : public kafka_client_fixture {};

FIXTURE_TEST(test_describe_topic, describe_topic_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    info("Describe an unknown topic");
    const auto unknown_tp = model::topic("non-existent");
    const auto unknown_tp_resp = client.describe_topics(unknown_tp).get();
    BOOST_REQUIRE_EQUAL(unknown_tp_resp.data.results.size(), 1);
    BOOST_REQUIRE_EQUAL(
      unknown_tp_resp.data.results[0].error_code,
      kafka::error_code::unknown_topic_or_partition);

    info("Create and describe an existing topic");
    const auto topic = create_topic();
    const auto resp = client.describe_topics(topic.tp).get();

    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp.data.results[0].resource_type, kafka::config_resource_type::topic);
    BOOST_REQUIRE_EQUAL(resp.data.results[0].resource_name, topic.tp());
    BOOST_REQUIRE_GE(resp.data.results[0].configs.size(), 0);
}

namespace {

std::optional<kafka::describe_configs_resource_result> find_config(
  const chunked_vector<kafka::describe_configs_resource_result>& configs,
  std::string_view target) {
    auto it = std::ranges::find(
      configs, target, &kafka::describe_configs_resource_result::name);
    return it != configs.end() ? std::optional{*it} : std::nullopt;
}

} // namespace

FIXTURE_TEST(test_describe_with_configuration_keys, describe_topic_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    const auto topic = model::topic("topic-with-custom-configs");
    const auto custom_config = kafka::createable_topic_config{
      .name = "cleanup.policy", .value = "compact"};

    info("Create a topic with custom topics");
    kafka::creatable_topic req{
      .name = topic,
      .num_partitions = 1,
      .replication_factor = 1,
      .configs = {custom_config},
    };
    const auto created_topic = client.create_topic(std::move(req)).get();

    {
        info("Checking if describing the topic returns the custom config");
        const auto resp = client.describe_topics(topic).get();
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(
          resp.data.results[0].resource_type,
          kafka::config_resource_type::topic);
        BOOST_REQUIRE_EQUAL(resp.data.results[0].resource_name, topic);
        BOOST_REQUIRE_GE(resp.data.results[0].configs.size(), 0);
        const auto found = find_config(
          resp.data.results[0].configs, custom_config.name);
        BOOST_REQUIRE(found);
        BOOST_REQUIRE(found->value == custom_config.value);
    }

    {
        info("Checking if requesting to describe the custom config returns it");
        const auto req_confs = chunked_vector<ss::sstring>{custom_config.name};
        const auto resp = client.describe_topics(topic, req_confs.copy()).get();
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);
        BOOST_REQUIRE_GE(resp.data.results[0].configs.size(), req_confs.size());
        BOOST_REQUIRE(
          find_config(resp.data.results[0].configs, custom_config.name));
    }

    {
        info("Checking if requesting multiple configs works");
        const auto req_confs = chunked_vector<ss::sstring>{
          custom_config.name, "max.message.bytes"};
        auto resp = client.describe_topics(topic, req_confs.copy()).get();
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);
        BOOST_REQUIRE_GE(resp.data.results[0].configs.size(), req_confs.size());
        for (const auto& conf : req_confs) {
            BOOST_REQUIRE(find_config(resp.data.results[0].configs, conf));
        }
    }
}

FIXTURE_TEST(test_describe_topic_multiple, describe_topic_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    const auto t1 = create_topic(1, 1);
    const auto t2 = create_topic(1, 2);

    info("Describe two topics at the same time");
    const auto resp
      = client.describe_topics(chunked_vector<model::topic>{t1.tp, t2.tp})
          .get();

    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 2);
    BOOST_REQUIRE_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results[0].resource_name, t1.tp());
    BOOST_REQUIRE_EQUAL(
      resp.data.results[1].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results[1].resource_name, t2.tp());
}
