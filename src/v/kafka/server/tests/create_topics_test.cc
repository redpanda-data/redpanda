// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/metadata.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "s3_imposter_fixture.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <limits>
#include <optional>

inline ss::logger test_log("test"); // NOLINT

// rougly equivalent to the test harness:
//   https://github.com/apache/kafka/blob/8e16158/core/src/test/scala/unit/kafka/server/AbstractCreateTopicsRequestTest.scala
class create_topic_fixture
  : public s3_imposter_fixture
  , public enable_cloud_storage_fixture
  , public redpanda_thread_fixture {
public:
    kafka::create_topics_request make_req(
      std::vector<kafka::creatable_topic> topics, bool validate_only = false) {
        return kafka::create_topics_request{.data{
          .topics = std::move(topics),
          .timeout_ms = 10s,
          .validate_only = validate_only,
        }};
    }

    cluster::non_replicable_topic
    make_non_rep(ss::sstring src, ss::sstring name) {
        return cluster::non_replicable_topic{
          .source = model::
            topic_namespace{model::kafka_namespace, model::topic(std::move(src))},
          .name = model::topic_namespace{
            model::kafka_namespace, model::topic(std::move(name))}};
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
      std::optional<int> partition_count = std::nullopt,
      std::optional<int> revision_id = std::nullopt) {
        auto client = make_kafka_client().get0();
        client.connect().get();
        auto resp = client.dispatch(req, kafka::api_version(2)).get0();

        // todo: here
        for (auto req : get_requests()) {
            vlog(test_log.info, "{} {}", req._method, req._url);
        }

        BOOST_TEST(
          std::all_of(
            std::cbegin(resp.data.topics),
            std::cend(resp.data.topics),
            [](const kafka::creatable_topic_result& t) {
                return t.error_code == kafka::error_code::none;
            }),
          fmt::format("expected no errors. received response: {}", resp));

        for (auto& topic : req.data.topics) {
            verify_metadata(client, req, topic, partition_count, revision_id);
            // TODO: one we combine the cluster fixture with the redpanda
            // fixture and enable multiple RP instances to run at the same time
            // in the test, then we should create two clients in this test where
            // one of the client is not connected to the controller, and verify
            // that the topic creation is correctly propogated to the
            // non-controller broker.
        }

        client.stop().then([&client] { client.shutdown(); }).get();
    }

    void test_create_read_replica_topic(
      kafka::create_topics_request req, int partition_count, int revision_id) {
        test_create_topic(req, partition_count, revision_id);
    }

    void test_create_non_replicable_topic(
      model::topic src, kafka::create_topics_request req) {
        std::vector<cluster::non_replicable_topic> non_reps;
        std::transform(
          req.data.topics.begin(),
          req.data.topics.end(),
          std::back_inserter(non_reps),
          [&src](const kafka::creatable_topic& t) {
              return cluster::non_replicable_topic{
                .source = model::topic_namespace{model::kafka_namespace, src},
                .name = model::topic_namespace{model::kafka_namespace, t.name}};
          });

        // Creating a materialized topic is not part of the kafka API
        // Must do this through the cluster::topics_frontend class
        auto& topics_frontend = app.controller->get_topics_frontend();
        const auto resp = topics_frontend.local()
                            .create_non_replicable_topics(
                              std::move(non_reps), model::no_timeout)
                            .get();
        BOOST_TEST(
          std::all_of(
            std::cbegin(resp),
            std::cend(resp),
            [](const cluster::topic_result& t) {
                return t.ec == cluster::errc::success;
            }),
          fmt::format("expected no errors. received response: {}", resp));

        auto client = make_kafka_client().get0();
        client.connect().get();
        for (auto& topic : req.data.topics) {
            verify_metadata(client, req, topic);
        }
        client.stop().then([&client] { client.shutdown(); }).get();
    }

    void verify_metadata(
      kafka::client::transport& client,
      kafka::create_topics_request& create_req,
      kafka::creatable_topic& request_topic,
      std::optional<int> partition_count = std::nullopt,
      std::optional<int> revision_id = std::nullopt) {
        // query the server for this topic's metadata
        kafka::metadata_request metadata_req;
        metadata_req.data.topics
          = std::make_optional<std::vector<kafka::metadata_request_topic>>();
        metadata_req.data.topics->push_back(
          kafka::metadata_request_topic{request_topic.name});
        auto metadata_resp
          = client.dispatch(metadata_req, kafka::api_version(1)).get0();

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
        if (partition_count) {
            partitions = partition_count.value();
        } else if (!request_topic.assignments.empty()) {
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

        if (create_req.data.validate_only) {
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
FIXTURE_TEST_EXPECTED_FAILURES(create_topics, create_topic_fixture, 2) {
    wait_for_controller_leadership().get();

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

FIXTURE_TEST(create_non_replicable_topics, create_topic_fixture) {
    wait_for_controller_leadership().get();

    test_create_topic(make_req({make_topic("topic1")}));
    test_create_non_replicable_topic(
      model::topic("topic1"), make_req({make_topic("topic2")}));

    // Test failure cases
    cluster::non_replicable_topic no_exist(make_non_rep("abc", "def"));
    cluster::non_replicable_topic already_exist(
      make_non_rep("topic1", "topic2"));
    auto& topics_frontend = app.controller->get_topics_frontend();
    const auto resp = topics_frontend.local()
                        .create_non_replicable_topics(
                          {std::move(no_exist), std::move(already_exist)},
                          model::no_timeout)
                        .get();
    BOOST_CHECK(resp[0].ec == cluster::errc::source_topic_not_exists);
    BOOST_CHECK(resp[0].tp_ns.tp() == "def");
    BOOST_CHECK(resp[1].ec == cluster::errc::topic_already_exists);
    BOOST_CHECK(resp[1].tp_ns.tp() == "topic2");
}

FIXTURE_TEST(read_replica, create_topic_fixture) {
    ss::sstring manifest_url = ssx::sformat(
      "/f0000000/meta/kafka/test-topic/topic_manifest.json");

    std::string_view manifest_payload = R"json({
        "version": 1,
        "namespace": "kafka",
        "topic": "test-topic",
        "partition_count": 32,
        "replication_factor": 3,
        "revision_id": 10,
        "compression": null,
        "cleanup_policy_bitflags": null,
        "compaction_strategy": null,
        "timestamp_type": null,
        "segment_size": null
    })json";

    set_expectations_and_listen({expectation{
      .url = manifest_url, .body = ss::sstring(manifest_payload)}});

    auto topic = make_topic(
      "test-topic",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.readreplica", "true"},
        {"redpanda.remote.readreplica.bucket", "panda-bucket"}});

    test_create_read_replica_topic(make_req({topic}), 32, 10);
}

FIXTURE_TEST(s3bucket_is_missing, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.readreplica", "true"}});

    auto req = make_req({topic});

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();

    BOOST_CHECK(
      resp.data.topics[0].error_code == kafka::error_code::invalid_config);
    BOOST_CHECK(
      resp.data.topics[0].error_message
      == "s3 bucket should be provided for read replica topic");
    BOOST_CHECK(resp.data.topics[0].name == "topic1");
}

FIXTURE_TEST(s3bucket_but_not_read_replica, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.readreplica.bucket", "panda-bucket"}});

    auto req = make_req({topic});

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();

    BOOST_CHECK(
      resp.data.topics[0].error_code == kafka::error_code::invalid_config);
    BOOST_CHECK(
      resp.data.topics[0].error_message
      == "s3 bucket is supported only when redpanda.remote.readreplica is "
         "enabled");
    BOOST_CHECK(resp.data.topics[0].name == "topic1");
}

FIXTURE_TEST(read_replica_and_remote_write, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"redpanda.remote.readreplica", "true"},
        {"redpanda.remote.write", "true"}});

    auto req = make_req({topic});

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();

    BOOST_CHECK(
      resp.data.topics[0].error_code == kafka::error_code::invalid_config);
    BOOST_CHECK(
      resp.data.topics[0].error_message
      == "remote read and write are not supported for read replicas");
    BOOST_CHECK(resp.data.topics[0].name == "topic1");
}
