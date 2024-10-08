// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/config_frontend.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/alter_configs.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/incremental_alter_configs.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/describe_configs_request.h"
#include "kafka/protocol/schemata/describe_configs_response.h"
#include "kafka/protocol/schemata/incremental_alter_configs_request.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>

using namespace std::chrono_literals; // NOLINT

inline ss::logger test_log("test"); // NOLINT

class alter_config_test_fixture : public redpanda_thread_fixture {
public:
    void create_topic(
      model::topic name,
      int partitions,
      std::optional<cluster::topic_properties> props = std::nullopt) {
        model::topic_namespace tp_ns(model::kafka_namespace, std::move(name));

        add_topic(tp_ns, partitions, std::move(props)).get();

        ss::parallel_for_each(
          boost::irange(0, partitions),
          [this, tp_ns](int i) {
              return wait_for_partition_offset(
                model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id(i)),
                model::offset(0));
          })
          .get();
    }

    template<typename Func>
    auto do_with_client(Func&& f) {
        return make_kafka_client().then(
          [f = std::forward<Func>(f)](kafka::client::transport client) mutable {
              return ss::do_with(
                std::move(client),
                [f = std::forward<Func>(f)](
                  kafka::client::transport& client) mutable {
                    return client.connect().then(
                      [&client, f = std::forward<Func>(f)]() mutable {
                          return f(client);
                      });
                });
          });
    }

    kafka::create_topics_response create_topic(
      const model::topic& tp,
      const absl::flat_hash_map<ss::sstring, ss::sstring>& properties,
      int num_partitions = 1,
      int16_t replication_factor = 1) {
        kafka::creatable_topic topic{
          .name = model::topic(tp),
          .num_partitions = num_partitions,
          .replication_factor = replication_factor,
        };
        for (auto& [k, v] : properties) {
            kafka::createable_topic_config config;
            config.name = k;
            config.value = v;
            topic.configs.push_back(std::move(config));
        }

        auto req = kafka::create_topics_request{.data{
          .topics = {topic},
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        return do_with_client([req = std::move(req)](
                                kafka::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), kafka::api_version(0));
               })
          .get();
    }

    kafka::alter_configs_resource make_alter_topic_config_resource(
      const model::topic& topic,
      const absl::flat_hash_map<ss::sstring, ss::sstring>& properties) {
        chunked_vector<kafka::alterable_config> cfg_list;
        cfg_list.reserve(properties.size());
        for (auto& [k, v] : properties) {
            cfg_list.push_back(kafka::alterable_config{.name = k, .value = v});
        }

        return kafka::alter_configs_resource{
          .resource_type = static_cast<int8_t>(
            kafka::config_resource_type::topic),
          .resource_name = topic,
          .configs = std::move(cfg_list),
        };
    }

    chunked_vector<kafka::alter_configs_resource>
    make_alter_topic_config_resource_cv(
      const model::topic& topic,
      const absl::flat_hash_map<ss::sstring, ss::sstring>& properties) {
        chunked_vector<kafka::alter_configs_resource> cv;
        cv.push_back(make_alter_topic_config_resource(topic, properties));
        return cv;
    }

    kafka::incremental_alter_configs_resource
    make_incremental_alter_topic_config_resource(
      const model::topic& topic,
      const absl::flat_hash_map<
        ss::sstring,
        std::
          pair<std::optional<ss::sstring>, kafka::config_resource_operation>>&
        operations) {
        chunked_vector<kafka::incremental_alterable_config> cfg_list;
        cfg_list.reserve(operations.size());
        for (auto& [k, v] : operations) {
            cfg_list.push_back(kafka::incremental_alterable_config{
              .name = k,
              .config_operation = static_cast<int8_t>(v.second),
              .value = v.first,
            });
        }

        return kafka::incremental_alter_configs_resource{
          .resource_type = static_cast<int8_t>(
            kafka::config_resource_type::topic),
          .resource_name = topic,
          .configs = std::move(cfg_list),
        };
    }

    chunked_vector<kafka::incremental_alter_configs_resource>
    make_incremental_alter_topic_config_resource_cv(
      const model::topic& topic,
      const absl::flat_hash_map<
        ss::sstring,
        std::
          pair<std::optional<ss::sstring>, kafka::config_resource_operation>>&
        operations) {
        chunked_vector<kafka::incremental_alter_configs_resource> cv;
        cv.push_back(
          make_incremental_alter_topic_config_resource(topic, operations));
        return cv;
    }

    kafka::describe_configs_response describe_configs(
      const ss::sstring& resource_name,
      std::optional<chunked_vector<ss::sstring>> configuration_keys
      = std::nullopt,
      kafka::config_resource_type resource_type
      = kafka::config_resource_type::topic) {
        kafka::describe_configs_request req;

        kafka::describe_configs_resource res{
          .resource_type = resource_type,
          .resource_name = resource_name,
          .configuration_keys = std::move(configuration_keys),
        };
        req.data.resources.push_back(std::move(res));
        return do_with_client([req = std::move(req)](
                                kafka::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), kafka::api_version(0));
               })
          .get();
    }

    kafka::alter_configs_response
    alter_configs(chunked_vector<kafka::alter_configs_resource> resources) {
        kafka::alter_configs_request req;
        req.data.resources = std::move(resources);
        return do_with_client([req = std::move(req)](
                                kafka::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), kafka::api_version(0));
               })
          .get();
    }

    kafka::incremental_alter_configs_response incremental_alter_configs(
      chunked_vector<kafka::incremental_alter_configs_resource> resources) {
        kafka::incremental_alter_configs_request req;
        req.data.resources = std::move(resources);
        return do_with_client([req = std::move(req)](
                                kafka::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), kafka::api_version(0));
               })
          .get();
    }

    void assert_property_presented(
      const ss::sstring& resource_name,
      const ss::sstring& key,
      const kafka::describe_configs_response& resp,
      const bool presented) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&resource_name](const kafka::describe_configs_result& res) {
              return res.resource_name == resource_name;
          });
        BOOST_REQUIRE(it != resp.data.results.end());

        auto cfg_it = std::find_if(
          it->configs.begin(),
          it->configs.end(),
          [&key](const kafka::describe_configs_resource_result& res) {
              return res.name == key;
          });
        if (presented) {
            BOOST_REQUIRE(cfg_it != it->configs.end());
        } else {
            BOOST_REQUIRE(cfg_it == it->configs.end());
        }
    }

    void assert_properties_amount(
      const ss::sstring& resource_name,
      const kafka::describe_configs_response& resp,
      const size_t amount) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&resource_name](const kafka::describe_configs_result& res) {
              return res.resource_name == resource_name;
          });
        BOOST_REQUIRE(it != resp.data.results.end());
        vlog(test_log.trace, "amount: {}", amount);
        vlog(test_log.trace, "it->configs.size(): {}", it->configs.size());
        vlog(test_log.trace, "it->configs: {}", it->configs);
        BOOST_REQUIRE(it->configs.size() == amount);
    }

    void assert_property_value(
      const model::topic& topic,
      const ss::sstring& key,
      const ss::sstring& value,
      const kafka::describe_configs_response& resp) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&topic](const kafka::describe_configs_result& res) {
              return res.resource_name == topic;
          });
        BOOST_REQUIRE(it != resp.data.results.end());

        auto cfg_it = std::find_if(
          it->configs.begin(),
          it->configs.end(),
          [&key](const kafka::describe_configs_resource_result& res) {
              return res.name == key;
          });
        BOOST_REQUIRE(cfg_it != it->configs.end());
        BOOST_REQUIRE_EQUAL(cfg_it->value, value);
    }
};

FIXTURE_TEST(
  test_broker_describe_configs_requested_properties,
  alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    kafka::metadata_request req;
    req.data.topics = std::nullopt;
    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(1)).get();
    client.stop().then([&client] { client.shutdown(); }).get();
    auto broker_id = std::to_string(resp.data.brokers[0].node_id());

    std::vector<ss::sstring> all_properties = {
      "listeners",
      "advertised.listeners",
      "log.segment.bytes",
      "log.retention.bytes",
      "log.retention.ms",
      "num.partitions",
      "default.replication.factor",
      "log.dirs",
      "auto.create.topics.enable"};

    // All properies_request
    auto all_describe_resp = describe_configs(
      broker_id, std::nullopt, kafka::config_resource_type::broker);
    assert_properties_amount(
      broker_id, all_describe_resp, all_properties.size());
    for (const auto& property : all_properties) {
        assert_property_presented(broker_id, property, all_describe_resp, true);
    }

    // Single properies_request
    for (const auto& request_property : all_properties) {
        chunked_vector<ss::sstring> request_properties{request_property};
        auto single_describe_resp = describe_configs(
          broker_id,
          std::make_optional(std::move(request_properties)),
          kafka::config_resource_type::broker);
        assert_properties_amount(broker_id, single_describe_resp, 1);
        for (const auto& property : all_properties) {
            assert_property_presented(
              broker_id,
              property,
              single_describe_resp,
              property == request_property);
        }
    }

    // Group properties_request
    chunked_vector<ss::sstring> first_group_config_properties = {
      "listeners",
      "advertised.listeners",
      "log.segment.bytes",
      "log.retention.bytes",
      "log.retention.ms"};

    chunked_vector<ss::sstring> second_group_config_properties = {
      "num.partitions",
      "default.replication.factor",
      "log.dirs",
      "auto.create.topics.enable"};

    auto first_group_describe_resp = describe_configs(
      broker_id,
      std::make_optional(first_group_config_properties.copy()),
      kafka::config_resource_type::broker);
    assert_properties_amount(
      broker_id,
      first_group_describe_resp,
      first_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          broker_id, property, first_group_describe_resp, true);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          broker_id, property, first_group_describe_resp, false);
    }

    auto second_group_describe_resp = describe_configs(
      broker_id,
      std::make_optional(second_group_config_properties.copy()),
      kafka::config_resource_type::broker);
    assert_properties_amount(
      broker_id,
      second_group_describe_resp,
      second_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          broker_id, property, second_group_describe_resp, false);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          broker_id, property, second_group_describe_resp, true);
    }
}

FIXTURE_TEST(
  test_topic_describe_configs_requested_properties, alter_config_test_fixture) {
    wait_for_controller_leadership().get();

    cluster::config_update_request r{
      .upsert = {{"enable_schema_id_validation", "compat"}}};
    app.controller->get_config_frontend()
      .local()
      .patch(r, model::timeout_clock::now() + 1s)
      .discard_result()
      .get();

    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    std::vector<ss::sstring> all_properties = {
      "retention.ms",
      "retention.bytes",
      "segment.bytes",
      "cleanup.policy",
      "compression.type",
      "message.timestamp.type",
      "redpanda.remote.read",
      "redpanda.remote.write",
      "max.message.bytes",
      "retention.local.target.bytes",
      "retention.local.target.ms",
      "redpanda.remote.delete",
      "segment.ms",
      "redpanda.key.schema.id.validation",
      "confluent.key.schema.validation",
      "redpanda.key.subject.name.strategy",
      "confluent.key.subject.name.strategy",
      "redpanda.value.schema.id.validation",
      "confluent.value.schema.validation",
      "redpanda.value.subject.name.strategy",
      "confluent.value.subject.name.strategy",
      "initial.retention.local.target.bytes",
      "initial.retention.local.target.ms",
      "write.caching",
      "flush.ms",
      "flush.bytes",
      "redpanda.iceberg.enabled",
      "redpanda.leaders.preference",
    };

    // All properties_request
    auto all_describe_resp = describe_configs(test_tp);
    assert_properties_amount(test_tp, all_describe_resp, all_properties.size());
    for (const auto& property : all_properties) {
        assert_property_presented(test_tp, property, all_describe_resp, true);
    }

    // Single properties_request
    for (const auto& request_property : all_properties) {
        chunked_vector<ss::sstring> request_properties = {request_property};
        auto single_describe_resp = describe_configs(
          test_tp, std::make_optional(std::move(request_properties)));
        assert_properties_amount(test_tp, single_describe_resp, 1);
        for (const auto& property : all_properties) {
            assert_property_presented(
              test_tp,
              property,
              single_describe_resp,
              property == request_property);
        }
    }

    // Group properties_request
    chunked_vector<ss::sstring> first_group_config_properties = {
      "retention.ms",
      "retention.bytes",
      "segment.bytes",
      "redpanda.remote.read",
      "redpanda.remote.write"};

    chunked_vector<ss::sstring> second_group_config_properties = {
      "cleanup.policy",
      "compression.type",
      "message.timestamp.type",
      "write.caching"};

    auto first_group_describe_resp = describe_configs(
      test_tp, std::make_optional(first_group_config_properties.copy()));
    vlog(
      test_log.debug,
      "first_group_describe_resp: {}",
      first_group_describe_resp);
    assert_properties_amount(
      test_tp, first_group_describe_resp, first_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          test_tp, property, first_group_describe_resp, true);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          test_tp, property, first_group_describe_resp, false);
    }

    auto second_group_describe_resp = describe_configs(
      test_tp, std::make_optional(second_group_config_properties.copy()));
    vlog(
      test_log.debug,
      "second_group_describe_resp: {}",
      second_group_describe_resp);
    assert_properties_amount(
      test_tp,
      second_group_describe_resp,
      second_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          test_tp, property, second_group_describe_resp, false);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          test_tp, property, second_group_describe_resp, true);
    }
}

FIXTURE_TEST(test_alter_single_topic_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("retention.ms", "1234");
    properties.emplace("cleanup.policy", "compact");
    properties.emplace("redpanda.remote.read", "true");
    properties.emplace("replication.factor", "1");
    properties.emplace("write.caching", "true");
    properties.emplace("flush.ms", "225");
    properties.emplace("flush.bytes", "32468");

    auto resp = alter_configs(
      make_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);
    assert_property_value(test_tp, "cleanup.policy", "compact", describe_resp);
    assert_property_value(
      test_tp, "redpanda.remote.read", "true", describe_resp);
}

FIXTURE_TEST(test_alter_multiple_topics_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic topic_1{"topic-1"};
    model::topic topic_2{"topic-2"};
    create_topic(topic_1, 1);
    create_topic(topic_2, 3);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties_1;
    properties_1.emplace("retention.ms", "1234");
    properties_1.emplace("cleanup.policy", "compact");
    properties_1.emplace("replication.factor", "1");
    properties_1.emplace("write.caching", "true");
    properties_1.emplace("flush.ms", "225");
    properties_1.emplace("flush.bytes", "32468");

    absl::flat_hash_map<ss::sstring, ss::sstring> properties_2;
    properties_2.emplace("retention.bytes", "4096");
    properties_2.emplace("replication.factor", "1");
    properties_2.emplace("write.caching", "false");
    properties_2.emplace("flush.ms", "100");
    properties_2.emplace("flush.bytes", "990");

    auto cv = make_alter_topic_config_resource_cv(topic_1, properties_1);
    cv.push_back(make_alter_topic_config_resource(topic_2, properties_2));
    auto resp = alter_configs(std::move(cv));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 2);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[1].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, topic_1);
    BOOST_REQUIRE_EQUAL(resp.data.responses[1].resource_name, topic_2);

    auto describe_resp_1 = describe_configs(topic_1);
    assert_property_value(
      topic_1, "retention.ms", fmt::format("{}", 1234ms), describe_resp_1);
    assert_property_value(
      topic_1, "cleanup.policy", "compact", describe_resp_1);
    assert_property_value(topic_1, "write.caching", "true", describe_resp_1);
    assert_property_value(topic_1, "flush.ms", "225", describe_resp_1);
    assert_property_value(topic_1, "flush.bytes", "32468", describe_resp_1);

    auto describe_resp_2 = describe_configs(topic_2);
    assert_property_value(topic_2, "retention.bytes", "4096", describe_resp_2);
    assert_property_value(topic_2, "write.caching", "false", describe_resp_2);
    assert_property_value(topic_2, "flush.ms", "100", describe_resp_2);
    assert_property_value(topic_2, "flush.bytes", "990", describe_resp_2);
}

FIXTURE_TEST(
  test_alter_topic_kafka_config_allowlist, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("unclean.leader.election.enable", "true");
    properties.emplace("replication.factor", "1");

    auto resp = alter_configs(
      make_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
}

FIXTURE_TEST(test_alter_topic_error, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    std::vector<std::pair<std::string, std::string>> properties_to_check = {
      {"not.exists", "1234"},
      {"write.caching", "disabled"},
      {"write.caching", "gnihcac.etirw"},
      {"flush.ms", "0"}};

    for (auto& property : properties_to_check) {
        vlog(
          test_log.info,
          "checking property: {}, {}",
          property.first,
          property.second);
        absl::flat_hash_map<ss::sstring, ss::sstring> properties;
        properties.emplace(property);
        auto resp = alter_configs(
          make_alter_topic_config_resource_cv(test_tp, properties));
        BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].error_code, kafka::error_code::invalid_config);
        BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);
    }
}

FIXTURE_TEST(
  test_alter_configuration_should_override, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    /**
     * Set custom properties
     */
    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("retention.ms", "1234");
    properties.emplace("replication.factor", "1");
    properties.emplace("write.caching", "true");
    properties.emplace("flush.ms", "225");
    properties.emplace("flush.bytes", "32468");

    auto resp = alter_configs(
      make_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);
    assert_property_value(test_tp, "write.caching", "true", describe_resp);
    assert_property_value(test_tp, "flush.ms", "225", describe_resp);
    assert_property_value(test_tp, "flush.bytes", "32468", describe_resp);

    /**
     * Set custom properties again, previous settings should be overriden
     */
    absl::flat_hash_map<ss::sstring, ss::sstring> new_properties;
    new_properties.emplace("retention.bytes", "4096");
    new_properties.emplace("replication.factor", "1");
    new_properties.emplace("write.caching", "false");
    new_properties.emplace("flush.ms", "9999");
    new_properties.emplace("flush.bytes", "8888");

    alter_configs(make_alter_topic_config_resource_cv(test_tp, new_properties));

    auto new_describe_resp = describe_configs(test_tp);
    // properties should be set back to default
    assert_property_value(
      test_tp,
      "retention.ms",
      fmt::format(
        "{}", config::shard_local_cfg().log_retention_ms().value_or(-1ms)),
      new_describe_resp);
    assert_property_value(
      test_tp, "retention.bytes", "4096", new_describe_resp);
    assert_property_value(test_tp, "write.caching", "false", new_describe_resp);
    assert_property_value(test_tp, "flush.ms", "9999", new_describe_resp);
    assert_property_value(test_tp, "flush.bytes", "8888", new_describe_resp);
}

FIXTURE_TEST(test_incremental_alter_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    // set custom properties
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
      properties;
    properties.emplace(
      "retention.ms",
      std::make_pair("1234", kafka::config_resource_operation::set));
    properties.emplace(
      "write.caching",
      std::make_pair("true", kafka::config_resource_operation::set));
    properties.emplace(
      "flush.ms",
      std::make_pair("1234", kafka::config_resource_operation::set));
    properties.emplace(
      "flush.bytes",
      std::make_pair("5678", kafka::config_resource_operation::set));

    auto resp = incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);
    assert_property_value(test_tp, "write.caching", "true", describe_resp);
    assert_property_value(test_tp, "flush.ms", "1234", describe_resp);
    assert_property_value(test_tp, "flush.bytes", "5678", describe_resp);

    /**
     * Set only few properties, only they should be updated
     */
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
      new_properties;
    new_properties.emplace(
      "retention.bytes",
      std::pair{"4096", kafka::config_resource_operation::set});
    new_properties.emplace(
      "write.caching",
      std::make_pair("false", kafka::config_resource_operation::set));

    incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, new_properties));

    auto new_describe_resp = describe_configs(test_tp);
    // retention.ms should stay untouched
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), new_describe_resp);
    assert_property_value(
      test_tp, "retention.bytes", "4096", new_describe_resp);
    assert_property_value(test_tp, "write.caching", "false", new_describe_resp);
    assert_property_value(test_tp, "flush.ms", "1234", new_describe_resp);
    assert_property_value(test_tp, "flush.bytes", "5678", new_describe_resp);
}

FIXTURE_TEST(
  test_incremental_alter_config_kafka_config_allowlist,
  alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
      properties;
    properties.emplace(
      "unclean.leader.election.enable",
      std::pair{"true", kafka::config_resource_operation::set});

    auto resp = incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
}

FIXTURE_TEST(test_incremental_alter_config_remove, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    // set custom properties
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
      properties;
    properties.emplace(
      "retention.ms",
      std::make_pair("1234", kafka::config_resource_operation::set));
    properties.emplace(
      "write.caching",
      std::make_pair("true", kafka::config_resource_operation::set));
    properties.emplace(
      "flush.ms",
      std::make_pair("9999", kafka::config_resource_operation::set));
    properties.emplace(
      "flush.bytes",
      std::make_pair("8888", kafka::config_resource_operation::set));

    auto resp = incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);
    assert_property_value(test_tp, "write.caching", "true", describe_resp);
    assert_property_value(test_tp, "flush.ms", "9999", describe_resp);
    assert_property_value(test_tp, "flush.bytes", "8888", describe_resp);

    /**
     * Remove custom properties
     */
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
      new_properties;
    new_properties.emplace(
      "retention.ms",
      std::pair{std::nullopt, kafka::config_resource_operation::remove});
    new_properties.emplace(
      "write.caching",
      std::pair{std::nullopt, kafka::config_resource_operation::remove});
    new_properties.emplace(
      "flush.ms",
      std::pair{std::nullopt, kafka::config_resource_operation::remove});
    new_properties.emplace(
      "flush.bytes",
      std::pair{std::nullopt, kafka::config_resource_operation::remove});

    resp = incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, new_properties));
    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto new_describe_resp = describe_configs(test_tp);
    // properties should use default values.
    assert_property_value(
      test_tp,
      "retention.ms",
      fmt::format(
        "{}", config::shard_local_cfg().log_retention_ms().value_or(-1ms)),
      new_describe_resp);
    assert_property_value(
      test_tp,
      "write.caching",
      fmt::format("{}", config::shard_local_cfg().write_caching_default()),
      new_describe_resp);
    assert_property_value(
      test_tp,
      "flush.ms",
      fmt::format(
        "{}",
        config::shard_local_cfg().raft_replica_max_flush_delay_ms().count()),
      new_describe_resp);
    assert_property_value(
      test_tp,
      "flush.bytes",
      fmt::format(
        "{}",
        config::shard_local_cfg()
          .raft_replica_max_pending_flush_bytes()
          .value()),
      new_describe_resp);
}

FIXTURE_TEST(test_iceberg_property, alter_config_test_fixture) {
    wait_for_controller_leadership().get();

    auto do_create_topic = [&](model::topic tp, bool iceberg) {
        absl::flat_hash_map<ss::sstring, ss::sstring> properties;
        properties.emplace(
          "redpanda.iceberg.enabled", (iceberg ? "true" : "false"));
        return create_topic(tp, properties);
    };

    model::topic topic1{"test1"};
    model::topic topic2{"topic2"};
    {
        // Try creating a topic with iceberg enabled while it is
        // disabled in cluster config.
        auto resp = do_create_topic(topic1, true);
        BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].error_code, kafka::error_code::invalid_config);
    }

    {
        // create a topic without iceberg and try enabling iceberg
        // while it is disabled at the cluster lvel.
        auto resp = do_create_topic(topic2, false);
        BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].error_code, kafka::error_code::none);

        absl::flat_hash_map<ss::sstring, ss::sstring> properties;
        properties.emplace("redpanda.iceberg.enabled", "true");
        auto alter_resp = alter_configs(
          make_alter_topic_config_resource_cv(topic2, properties));
        BOOST_REQUIRE_EQUAL(alter_resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          alter_resp.data.responses[0].error_code,
          kafka::error_code::invalid_config);
    }

    {
        // same as above, with incremental alter
        absl::flat_hash_map<
          ss::sstring,
          std::
            pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
          properties;
        properties.emplace(
          "redpanda.iceberg.enabled",
          std::make_pair("true", kafka::config_resource_operation::set));

        auto resp = incremental_alter_configs(
          make_incremental_alter_topic_config_resource_cv(topic2, properties));

        BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].error_code, kafka::error_code::invalid_config);
    }

    // Enable iceberg at the cluster level
    scoped_config config;
    config.get("iceberg_enabled").set_value(true);

    {
        // Attempt to create the topic again.
        auto resp = do_create_topic(topic1, true);
        BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.topics[0].error_code, kafka::error_code::none);
    }

    {
        // alter the iceberg config of an existing topic, should work.
        for (auto prop : {false, true}) {
            ss::sstring prop_str = prop ? "true" : "false";
            absl::flat_hash_map<ss::sstring, ss::sstring> properties;
            properties.emplace("redpanda.iceberg.enabled", prop_str);

            auto resp = alter_configs(
              make_alter_topic_config_resource_cv(topic2, properties));

            BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
            BOOST_REQUIRE_EQUAL(
              resp.data.responses[0].error_code, kafka::error_code::none);
            BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, topic2);

            auto describe_resp = describe_configs(topic2);
            assert_property_value(
              topic2, "redpanda.iceberg.enabled", prop_str, describe_resp);
        }
    }

    {
        // same as above, with incremental alter
        for (auto prop : {false, true}) {
            ss::sstring prop_str = prop ? "true" : "false";
            absl::flat_hash_map<
              ss::sstring,
              std::pair<
                std::optional<ss::sstring>,
                kafka::config_resource_operation>>
              properties;
            properties.emplace(
              "redpanda.iceberg.enabled",
              std::make_pair(prop_str, kafka::config_resource_operation::set));

            auto resp = incremental_alter_configs(
              make_incremental_alter_topic_config_resource_cv(
                topic2, properties));

            BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
            BOOST_REQUIRE_EQUAL(
              resp.data.responses[0].error_code, kafka::error_code::none);
            BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, topic2);

            auto describe_resp = describe_configs(topic2);
            assert_property_value(
              topic2, "redpanda.iceberg.enabled", prop_str, describe_resp);
        }
    }

    {
        // Altering iceberg configuration on an internal topic should fail
        // create an internal topic
        BOOST_REQUIRE(kafka::try_create_consumer_group_topic(
                        app.coordinator_ntp_mapper.local(),
                        app.controller->get_topics_frontend().local(),
                        1)
                        .get());
        // enable authorization on it, to be able to make alter requests.
        config.get("kafka_nodelete_topics")
          .set_value(std::vector<ss::sstring>{});

        absl::flat_hash_map<ss::sstring, ss::sstring> properties;
        properties.emplace("redpanda.iceberg.enabled", "true");

        auto resp = alter_configs(make_alter_topic_config_resource_cv(
          model::kafka_consumer_offsets_topic, properties));
        BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].error_code, kafka::error_code::invalid_config);

        absl::flat_hash_map<
          ss::sstring,
          std::
            pair<std::optional<ss::sstring>, kafka::config_resource_operation>>
          incr_properties;
        incr_properties.emplace(
          "redpanda.iceberg.enabled",
          std::make_pair("true", kafka::config_resource_operation::set));
        auto incr_resp = incremental_alter_configs(
          make_incremental_alter_topic_config_resource_cv(
            model::kafka_consumer_offsets_topic, incr_properties));
        BOOST_REQUIRE_EQUAL(incr_resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          incr_resp.data.responses[0].error_code,
          kafka::error_code::invalid_config);
    }
}

FIXTURE_TEST(test_shadow_indexing_alter_configs, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    using map_t = absl::flat_hash_map<ss::sstring, ss::sstring>;
    std::vector<map_t> test_cases;

    {
        map_t properties;
        properties.emplace("redpanda.remote.write", "false");
        properties.emplace("redpanda.remote.read", "true");
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace("redpanda.remote.write", "true");
        properties.emplace("redpanda.remote.read", "false");
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace("redpanda.remote.write", "true");
        properties.emplace("redpanda.remote.read", "true");
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace("redpanda.remote.write", "false");
        properties.emplace("redpanda.remote.read", "false");
        test_cases.push_back(std::move(properties));
    }

    for (const auto& test_case : test_cases) {
        auto resp = alter_configs(
          make_alter_topic_config_resource_cv(test_tp, test_case));

        BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].error_code, kafka::error_code::none);
        auto describe_resp = describe_configs(test_tp);
        assert_property_value(
          test_tp,
          "redpanda.remote.write",
          test_case.at("redpanda.remote.write"),
          describe_resp);
        assert_property_value(
          test_tp,
          "redpanda.remote.read",
          test_case.at("redpanda.remote.read"),
          describe_resp);
    }
}

FIXTURE_TEST(
  test_shadow_indexing_incremental_alter_configs, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    using map_t = absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>;
    std::vector<map_t> test_cases;
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("false", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("true", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("true", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("false", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("true", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("true", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("false", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("false", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair(
            std::nullopt, kafka::config_resource_operation::remove));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("true", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("true", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair(
            std::nullopt, kafka::config_resource_operation::remove));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair("true", kafka::config_resource_operation::set));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair("true", kafka::config_resource_operation::set));
        test_cases.push_back(std::move(properties));
    }
    {
        map_t properties;
        properties.emplace(
          "redpanda.remote.write",
          std::make_pair(
            std::nullopt, kafka::config_resource_operation::remove));
        properties.emplace(
          "redpanda.remote.read",
          std::make_pair(
            std::nullopt, kafka::config_resource_operation::remove));
        test_cases.push_back(std::move(properties));
    }

    for (const auto& test_case : test_cases) {
        auto resp = incremental_alter_configs(
          make_incremental_alter_topic_config_resource_cv(test_tp, test_case));

        BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(
          resp.data.responses[0].error_code, kafka::error_code::none);
        auto describe_resp = describe_configs(test_tp);
        assert_property_value(
          test_tp,
          "redpanda.remote.write",
          test_case.at("redpanda.remote.write").first.value_or("false"),
          describe_resp);
        assert_property_value(
          test_tp,
          "redpanda.remote.read",
          test_case.at("redpanda.remote.read").first.value_or("false"),
          describe_resp);
    }
}

FIXTURE_TEST(
  test_shadow_indexing_uppercase_alter_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    using map_t = absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, kafka::config_resource_operation>>;
    map_t properties;
    // Case is on purpose.
    properties.emplace(
      "redpanda.remote.write",
      std::make_pair("True", kafka::config_resource_operation::set));
    properties.emplace(
      "redpanda.remote.read",
      std::make_pair("TRUE", kafka::config_resource_operation::set));

    auto resp = incremental_alter_configs(
      make_incremental_alter_topic_config_resource_cv(test_tp, properties));

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, kafka::error_code::none);
    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "redpanda.remote.write", "true", describe_resp);
    assert_property_value(
      test_tp, "redpanda.remote.read", "true", describe_resp);
}
