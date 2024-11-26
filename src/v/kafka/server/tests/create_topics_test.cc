// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/leaders_preference.h"
#include "container/fragmented_vector.h"
#include "features/enterprise_feature_messages.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/tests/topic_properties_helpers.h"
#include "model/errc.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>

// rougly equivalent to the test harness:
//   https://github.com/apache/kafka/blob/8e16158/core/src/test/scala/unit/kafka/server/AbstractCreateTopicsRequestTest.scala
class create_topic_fixture : public topic_properties_test_fixture {
public:
    kafka::create_topics_request make_req(
      chunked_vector<kafka::creatable_topic> topics,
      bool validate_only = false) {
        return kafka::create_topics_request{.data{
          .topics = std::move(topics),
          .timeout_ms = 10s,
          .validate_only = validate_only,
        }};
    }

    kafka::creatable_topic make_topic(
      ss::sstring name,
      std::optional<int> num_partitions = std::nullopt,
      std::optional<int> replication_factor = std::nullopt,
      std::optional<std::map<ss::sstring, ss::sstring>> config = std::nullopt,
      std::optional<std::map<int, std::vector<int>>> assignment
      = std::nullopt) {
        kafka::creatable_topic topic;

        topic.name = model::topic(name);

        if (num_partitions) {
            topic.num_partitions = *num_partitions;
        } else if (assignment) {
            topic.num_partitions = -1;
        } else {
            topic.num_partitions = 1;
        }

        if (replication_factor) {
            topic.replication_factor = *replication_factor;
        } else if (assignment) {
            topic.replication_factor = -1;
        } else {
            topic.replication_factor = 1;
        }

        if (config) {
            for (auto& c : *config) {
                topic.configs.push_back({c.first, c.second});
            }
        }

        if (assignment) {
            for (auto& a : *assignment) {
                kafka::creatable_replica_assignment pa;
                pa.partition_index = model::partition_id(a.first);
                for (auto& b : a.second) {
                    pa.broker_ids.push_back(model::node_id(b));
                }
                topic.assignments.push_back(std::move(pa));
            }
        }

        return topic;
    }

    void test_create_topic(
      kafka::create_topics_request req,
      kafka::api_version version = kafka::api_version(2)) {
        auto client = make_kafka_client().get();
        client.connect().get();
        auto topics = req.data.topics
                        .copy(); // save a copy because we will move out of req
        bool validate_only = req.data.validate_only;
        auto resp = client.dispatch(std::move(req), version).get();

        BOOST_REQUIRE_MESSAGE(
          std::all_of(
            std::cbegin(resp.data.topics),
            std::cend(resp.data.topics),
            [](const kafka::creatable_topic_result& t) {
                return t.error_code == kafka::error_code::none;
            }),
          fmt::format("expected no errors. received response: {}", resp));

        for (auto& topic : topics) {
            verify_metadata(client, validate_only, topic);

            auto it = std::find_if(
              resp.data.topics.begin(),
              resp.data.topics.end(),
              [name = topic.name](const auto& t) { return t.name == name; });

            BOOST_CHECK(it != resp.data.topics.end());
            verify_response(topic, *it, version, validate_only);

            // TODO: one we combine the cluster fixture with the redpanda
            // fixture and enable multiple RP instances to run at the same time
            // in the test, then we should create two clients in this test where
            // one of the client is not connected to the controller, and verify
            // that the topic creation is correctly propogated to the
            // non-controller broker.
        }

        client.stop().then([&client] { client.shutdown(); }).get();
    }

    void verify_response(
      const kafka::creatable_topic& req,
      const kafka::creatable_topic_result& topic_res,
      kafka::api_version version,
      bool validate_only) {
        if (version < kafka::api_version(5)) {
            /// currently this method only verifies configurations in v5
            /// responses
            return;
        }
        if (validate_only) {
            /// Server should return default configs
            BOOST_TEST(topic_res.configs, "empty config response");
            auto cfg_map = config_map(*topic_res.configs);
            const auto default_topic_properties = config_map(
              kafka::report_topic_configs(
                app.metadata_cache.local(),
                app.metadata_cache.local().get_default_properties()));
            BOOST_TEST(
              cfg_map == default_topic_properties,
              "incorrect default properties");
            BOOST_CHECK_EQUAL(
              topic_res.topic_config_error_code, kafka::error_code::none);
            return;
        }
        if (req.configs.empty()) {
            /// no custom configs were passed
            return;
        }
        BOOST_TEST(topic_res.configs, "Expecting configs");
        auto resp_cfgs = kafka::config_map(*topic_res.configs);
        auto cfg = app.metadata_cache.local().get_topic_cfg(
          model::topic_namespace_view{model::kafka_namespace, topic_res.name});
        BOOST_TEST(cfg, "missing topic config");
        auto cfg_map = config_map(kafka::report_topic_configs(
          app.metadata_cache.local(), cfg->properties));
        BOOST_TEST(cfg_map == resp_cfgs, "configs didn't match");
        BOOST_CHECK_EQUAL(
          topic_res.topic_config_error_code, kafka::error_code::none);
    }

    void verify_metadata(
      kafka::client::transport& client,
      bool validate_only,
      kafka::creatable_topic& request_topic) {
        // query the server for this topic's metadata
        kafka::metadata_request metadata_req;
        metadata_req.data.topics
          = std::make_optional<chunked_vector<kafka::metadata_request_topic>>();
        metadata_req.data.topics->push_back(
          kafka::metadata_request_topic{request_topic.name});
        auto metadata_resp
          = client.dispatch(std::move(metadata_req), kafka::api_version(1))
              .get();

        // yank out the metadata for the topic from the response
        auto topic_metadata = std::find_if(
          metadata_resp.data.topics.cbegin(),
          metadata_resp.data.topics.cend(),
          [&request_topic](const kafka::metadata_response::topic& topic) {
              return topic.name == request_topic.name;
          });

        BOOST_TEST_REQUIRE(
          (topic_metadata != metadata_resp.data.topics.cend()),
          "expected topic not returned from metadata query");

        int partitions;
        if (!request_topic.assignments.empty()) {
            partitions = request_topic.assignments.size();
        } else {
            partitions = request_topic.num_partitions;
        }

        int replication;
        if (!request_topic.assignments.empty()) {
            replication = request_topic.assignments[0].broker_ids.size();
        } else {
            replication = request_topic.replication_factor;
        }

        if (validate_only) {
            BOOST_TEST(
              topic_metadata->error_code != kafka::error_code::none,
              fmt::format(
                "error {} for topic {}",
                topic_metadata->error_code,
                request_topic.name));
            BOOST_TEST(
              topic_metadata->partitions.empty(),
              "topic should have no partitions");
        } else {
            BOOST_TEST(topic_metadata->error_code == kafka::error_code::none);
            if (partitions == -1) {
                // FIXME: where does the default partition count come from?
                BOOST_TEST(topic_metadata->partitions.size() == 99999999);
            } else {
                BOOST_TEST(topic_metadata->partitions.size() == partitions);
            }

            // FIXME: this is a temporary fix. what we really want is to use
            // BOOST_TEST_REQUIRE for this condition, but there does seem to be
            // something preventing the partitions from being reported
            // reliabily.
            BOOST_TEST(!topic_metadata->partitions.empty());
            if (topic_metadata->partitions.empty()) {
                return;
            }

            if (replication == -1) {
                // FIXME: where does the default replication come from?
                BOOST_TEST(
                  topic_metadata->partitions[0].replica_nodes.size()
                  == 99999999);
            } else {
                BOOST_TEST(
                  topic_metadata->partitions[0].replica_nodes.size()
                  == replication);
            }
        }
    }

    void foo() { BOOST_TEST(false); }
};

// This is rougly equivalent to
//   https://github.com/apache/kafka/blob/8e16158/core/src/test/scala/unit/kafka/server/CreateTopicsRequestTest.scala#L27
FIXTURE_TEST(create_topics, create_topic_fixture) {
    test_create_topic(make_req({make_topic("topic1")}));

    // FIXME: these all crash with undefined behavior
#if 0
    // replication factor = 3
    test_create_topic(make_req({make_topic("topic2", std::nullopt, 3)}));

    test_create_topic(make_req({make_topic(
      "topic2",
      5,
      2,
      std::map<ss::sstring, ss::sstring>{{
        {"min.insync.replicas", "2"},
      }})}));

    // defaults
    test_create_topic(make_req({make_topic("topic12", -1, -1)}));
    test_create_topic(make_req({make_topic("topic13", -1, 2)}));
    test_create_topic(make_req({make_topic("topic13", 2, -1)}));
#endif

    // FIXME: redpanda does not currently support manual partition assignments.
    // however we should handle gracefully clients that try.
#if 0
    // Manual assignments
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic4", assignment = Map(0 -> List(0))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic5",
      assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)),
      config = Map("min.insync.replicas" -> "2")))))
    // Mixed
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic6"),
      topicReq("topic7", numPartitions = 5, replicationFactor = 2),
      topicReq("topic8", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic9"),
      topicReq("topic10", numPartitions = 5, replicationFactor = 2),
      topicReq("topic11", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))),
      validateOnly = true))
#endif
}

FIXTURE_TEST(read_replica_and_remote_write, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.readreplica", "panda-bucket"},
        {"redpanda.remote.write", "true"}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(make_req({topic}), kafka::api_version(2)).get();

    BOOST_CHECK_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].error_message,
      "remote read and write are not supported for read replicas");
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, "topic1");
}

FIXTURE_TEST(test_v5_validate_configs_resp, create_topic_fixture) {
    /// Test conditions in create_topic_fixture::verify_metadata will run
    test_create_topic(
      make_req({make_topic("topicA"), make_topic("topicB")}, true),
      kafka::api_version(5));

    /// Test create topic with custom configs, verify that they have been set
    /// and correctly returned in response
    std::map<ss::sstring, ss::sstring> config_map{
      {ss::sstring(kafka::topic_property_retention_bytes), "1234567"},
      {ss::sstring(kafka::topic_property_segment_size), "7654321"}};

    test_create_topic(
      make_req(
        {make_topic("topicC", 3, 1, config_map),
         make_topic("topicD", 3, 1, config_map)},
        false),
      kafka::api_version(5));
}

FIXTURE_TEST(create_multiple_topics_mixed_invalid, create_topic_fixture) {
    auto topic_a = make_topic(
      "topic_a",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{{"redpanda.remote.write", "true"}});

    auto topic_b = make_topic(
      "topic_b",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"retention.bytes", "this_should_be_an_integer"}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client
                  .dispatch(make_req({topic_a, topic_b}), kafka::api_version(5))
                  .get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 2);

    BOOST_CHECK_EQUAL(resp.data.topics[0].error_code, kafka::error_code::none);
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, "topic_a");

    BOOST_CHECK_EQUAL(
      resp.data.topics[1].error_code, kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(resp.data.topics[1].name, "topic_b");
}

FIXTURE_TEST(create_multiple_topics_all_invalid, create_topic_fixture) {
    auto topic_a = make_topic(
      "topic_a",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.write", "yes_this_is_true"}});

    auto topic_b = make_topic(
      "topic_b",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"retention.bytes", "this_should_be_an_integer"}});

    auto topic_c = make_topic(
      "topic_c",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{{"segment.ms", "0x2A"}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client
                  .dispatch(
                    make_req({topic_a, topic_b, topic_c}),
                    kafka::api_version(5))
                  .get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 3);

    BOOST_CHECK_EQUAL(resp.data.topics[0].name, "topic_a");
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::invalid_config);

    BOOST_CHECK_EQUAL(resp.data.topics[1].name, "topic_b");
    BOOST_CHECK_EQUAL(
      resp.data.topics[1].error_code, kafka::error_code::invalid_config);

    BOOST_CHECK_EQUAL(resp.data.topics[2].name, "topic_c");
    BOOST_CHECK_EQUAL(
      resp.data.topics[2].error_code, kafka::error_code::invalid_config);
}

FIXTURE_TEST(create_multiple_topics_all_invalid_name, create_topic_fixture) {
    static constexpr size_t kafka_max_topic_name_length = 249;
    constexpr auto check_message = [](const auto& res, model::errc ec) {
        return res.error_message.value_or("").contains(
          make_error_code(ec).message());
    };

    auto topic_empty = make_topic("");
    auto topic_forbidden = make_topic(".");
    auto topic_too_long = make_topic(
      ss::sstring(kafka_max_topic_name_length + 1, 'a'));
    auto topic_invalid = make_topic("$nope");

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp
      = client
          .dispatch(
            make_req(
              {topic_empty, topic_forbidden, topic_too_long, topic_invalid}),
            kafka::api_version(5))
          .get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 4);
    const auto& res = resp.data.topics;

    BOOST_CHECK_EQUAL(res[0].name, topic_empty.name);
    BOOST_CHECK_EQUAL(
      res[0].error_code, kafka::error_code::invalid_topic_exception);
    BOOST_CHECK(check_message(res[0], model::errc::topic_name_empty));

    BOOST_CHECK_EQUAL(res[1].name, topic_forbidden.name);
    BOOST_CHECK_EQUAL(
      res[1].error_code, kafka::error_code::invalid_topic_exception);
    BOOST_CHECK(check_message(res[1], model::errc::forbidden_topic_name));

    BOOST_CHECK_EQUAL(res[2].name, topic_too_long.name);
    BOOST_CHECK_EQUAL(
      res[2].error_code, kafka::error_code::invalid_topic_exception);
    BOOST_CHECK(check_message(res[2], model::errc::topic_name_len_exceeded));

    BOOST_CHECK_EQUAL(res[3].name, topic_invalid.name);
    BOOST_CHECK_EQUAL(
      res[3].error_code, kafka::error_code::invalid_topic_exception);
    BOOST_CHECK(check_message(res[3], model::errc::invalid_topic_name));
}

FIXTURE_TEST(invalid_boolean_property, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.write", "affirmative"}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(make_req({topic}), kafka::api_version(5)).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].error_message, "Configuration is invalid");
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, "topic1");
}

FIXTURE_TEST(case_insensitive_boolean_property, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.write", "tRuE"}, {"redpanda.remote.read", "FALSE"}});

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(make_req({topic}), kafka::api_version(5)).get();

    BOOST_CHECK_EQUAL(resp.data.topics[0].error_code, kafka::error_code::none);
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, "topic1");
}

FIXTURE_TEST(unlicensed_rejected, create_topic_fixture) {
    // NOTE(oren): w/o schema validation enabled at the cluster level, related
    // properties will be ignored on the topic create path. stick to COMPAT here
    // because it's a superset of REDPANDA.
    lconf().enable_schema_id_validation.set_value(
      pandaproxy::schema_registry::schema_id_validation_mode::compat);

    revoke_license();
    using prop_t = std::map<ss::sstring, ss::sstring>;
    const auto with = [](const std::string_view prop, const auto value) {
        return std::make_pair(
          prop, prop_t{{ss::sstring{prop}, ssx::sformat("{}", value)}});
    };
    std::vector<std::pair<std::string_view, prop_t>> enterprise_props{
      // si_props
      with(kafka::topic_property_remote_read, true),
      with(kafka::topic_property_remote_write, true),
      with(kafka::topic_property_recovery, true),
      with(kafka::topic_property_read_replica, true),
      // schema id validation
      with(kafka::topic_property_record_key_schema_id_validation, true),
      with(kafka::topic_property_record_key_schema_id_validation_compat, true),
      with(kafka::topic_property_record_value_schema_id_validation, true),
      with(
        kafka::topic_property_record_value_schema_id_validation_compat, true),
      // pin_leadership_props
      with(
        kafka::topic_property_leaders_preference,
        config::leaders_preference{
          .type = config::leaders_preference::type_t::racks,
          .racks = {model::rack_id{"A"}}})};

    auto client = make_kafka_client().get();
    client.connect().get();

    for (const auto& [name, props] : enterprise_props) {
        auto topic = make_topic(
          ssx::sformat("topic_{}", name), std::nullopt, std::nullopt, props);

        auto resp
          = client.dispatch(make_req({topic}), kafka::api_version(5)).get();

        BOOST_CHECK_EQUAL(
          resp.data.topics[0].error_code, kafka::error_code::invalid_config);
        auto expected_message
          = (name == kafka::topic_property_recovery
             || name == kafka::topic_property_read_replica)
              ? "Tiered storage is not enabled"
              : features::enterprise_error_message::required;
        BOOST_CHECK(resp.data.topics[0].error_message.value_or("").contains(
          expected_message));
    }
}

FIXTURE_TEST(unlicensed_reject_defaults, create_topic_fixture) {
    revoke_license();

    const std::initializer_list<std::string_view> si_configs{
      lconf().cloud_storage_enable_remote_read.name(),
      lconf().cloud_storage_enable_remote_write.name()};

    auto client = make_kafka_client().get();
    client.connect().get();

    for (const auto& config : si_configs) {
        update_cluster_config(config, "true");
        auto topic = make_topic(ssx::sformat("topic_{}", config));

        auto resp
          = client.dispatch(make_req({topic}), kafka::api_version(5)).get();

        BOOST_CHECK_EQUAL(
          resp.data.topics[0].error_code, kafka::error_code::invalid_config);
        BOOST_CHECK(resp.data.topics[0].error_message.value_or("").contains(
          features::enterprise_error_message::required));
        update_cluster_config(config, "false");
    }
}

FIXTURE_TEST(create_dry_run_rejects_existing, create_topic_fixture) {
    auto client = make_kafka_client().get();
    client.connect().get();

    auto topic = make_topic(ssx::sformat("topic_foo"));

    // create the topic
    auto resp = client
                  .dispatch(
                    make_req({topic}, /*validate_only = */ false),
                    kafka::api_version(5))
                  .get();
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::none);

    // rejects attempt to create it again
    resp = client
             .dispatch(
               make_req({topic}, /*validate_only = */ false),
               kafka::api_version(5))
             .get();
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::topic_already_exists);

    // also rejects dry run create
    resp = client
             .dispatch(
               make_req({topic}, /*validate_only = */ true),
               kafka::api_version(5))
             .get();
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::topic_already_exists);
}
