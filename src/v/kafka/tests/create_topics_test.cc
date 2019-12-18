#include "kafka/requests/create_topics_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/smp.hh>

#include <algorithm>
#include <limits>

// rougly equivalent to the test harness:
//   https://github.com/apache/kafka/blob/8e16158/core/src/test/scala/unit/kafka/server/AbstractCreateTopicsRequestTest.scala
class create_topic_fixture : public redpanda_thread_fixture {
public:
    kafka::create_topics_request make_req(
      std::vector<kafka::new_topic_configuration> topics,
      int timeout = 10000,
      bool validate_only = false) {
        return kafka::create_topics_request{
          .topics = std::move(topics),
          .timeout = std::chrono::milliseconds(timeout),
          .validate_only = validate_only,
        };
    }

    kafka::new_topic_configuration make_topic(
      sstring name,
      std::optional<int> num_partitions = std::nullopt,
      std::optional<int> replication_factor = std::nullopt,
      std::optional<std::map<sstring, sstring>> config = std::nullopt,
      std::optional<std::map<int, std::vector<int>>> assignment
      = std::nullopt) {
        kafka::new_topic_configuration topic;

        topic.topic = model::topic(name);

        if (num_partitions) {
            topic.partition_count = *num_partitions;
        } else if (assignment) {
            topic.partition_count = -1;
        } else {
            topic.partition_count = 1;
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
                topic.config.push_back({c.first, c.second});
            }
        }

        if (assignment) {
            for (auto& a : *assignment) {
                kafka::partition_assignment pa;
                pa.partition = model::partition_id(a.first);
                for (auto& b : a.second) {
                    pa.assignments.push_back(model::node_id(b));
                }
                topic.assignments.push_back(std::move(pa));
            }
        }

        return topic;
    }

    void test_create_topic(kafka::create_topics_request req) {
        auto client = make_kafka_client().get0();
        client.connect().get();
        auto resp = client.dispatch(req, kafka::api_version(2)).get0();

        BOOST_TEST(
          std::all_of(
            std::cbegin(resp.topics),
            std::cend(resp.topics),
            [](const kafka::create_topics_response::topic& t) {
                return t.error == kafka::error_code::none;
            }),
          fmt::format("expected no errors. received response: {}", resp));

        for (auto& topic : req.topics) {
            verify_metadata(client, req, topic);
            // TODO: one we combine the cluster fixture with the redpanda
            // fixture and enable multiple RP instances to run at the same time
            // in the test, then we should create two clients in this test where
            // one of the client is not connected to the controller, and verify
            // that the topic creation is correctly propogated to the
            // non-controller broker.
        }

        client.stop().then([&client] { client.shutdown(); }).get();
    }

    void verify_metadata(
      kafka::client& client,
      kafka::create_topics_request& create_req,
      kafka::new_topic_configuration& request_topic) {
        // query the server for this topic's metadata
        kafka::metadata_request metadata_req;
        metadata_req.topics.push_back(request_topic.topic);
        auto metadata_resp
          = client.metadata(metadata_req, kafka::api_version(1)).get0();

        // yank out the metadata for the topic from the response
        auto topic_metadata = std::find_if(
          metadata_resp.topics.cbegin(),
          metadata_resp.topics.cend(),
          [&request_topic](const kafka::metadata_response::topic& topic) {
              return topic.name == request_topic.topic;
          });

        BOOST_TEST_REQUIRE(
          (topic_metadata != metadata_resp.topics.cend()),
          "expected topic not returned from metadata query");

        int partitions;
        if (!request_topic.assignments.empty()) {
            partitions = request_topic.assignments.size();
        } else {
            partitions = request_topic.partition_count;
        }

        int replication;
        if (!request_topic.assignments.empty()) {
            replication = request_topic.assignments[0].assignments.size();
        } else {
            replication = request_topic.replication_factor;
        }

        if (create_req.validate_only) {
            BOOST_TEST(
              topic_metadata->err_code != kafka::error_code::none,
              fmt::format(
                "error {} for topic {}",
                topic_metadata->err_code,
                request_topic.topic));
            BOOST_TEST(
              topic_metadata->partitions.empty(),
              "topic should have no partitions");
        } else {
            BOOST_TEST(topic_metadata->err_code == kafka::error_code::none);
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
      std::map<sstring, sstring>{{
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
