/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/client/cluster.h"
#include "kafka/client/test/cluster_mock.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

using namespace kafka::client;
using namespace std::chrono_literals;

struct cluster_mock_fixture : public ::testing::Test {
    kafka::client::cluster create_client_cluster() {
        return kafka::client::cluster(
          kafka::client::connection_configuration{
            .initial_brokers = {net::unresolved_address{"localhost", 9092}},
            .client_id = "test-client",
            .max_metadata_age = 1s,
          },
          std::make_unique<kafka::client::broker_mock_factory>(&cluster_mock));
    }

    cluster_mock cluster_mock;
};

TEST_F(cluster_mock_fixture, TestBrokerDiscovery) {
    cluster_mock.register_default_handlers();
    auto cluster = create_client_cluster();

    cluster_mock.add_broker(
      model::node_id(1), net::unresolved_address{"localhost", 9092});
    cluster.start().get();
    auto deferred_stop = ss::defer([&cluster] { cluster.stop().get(); });
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&cluster]() { return cluster.get_brokers().size() == 1; });

    cluster_mock.add_broker(
      model::node_id(2), net::unresolved_address{"localhost", 9093});

    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&cluster]() { return cluster.get_brokers().size() == 2; });

    cluster_mock.add_broker(
      model::node_id(3), net::unresolved_address{"localhost", 9094});

    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&cluster]() { return cluster.get_brokers().size() == 3; });

    cluster_mock.remove_broker(model::node_id(1));
    cluster_mock.remove_broker(model::node_id(2));
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&cluster]() { return cluster.get_brokers().size() == 1; });
}

TEST_F(cluster_mock_fixture, TestMetadataCallback) {
    cluster_mock.register_default_handlers();
    auto cluster = create_client_cluster();
    auto deferred_stop = ss::defer([&cluster] { cluster.stop().get(); });
    int callback_invocations = 0;

    cluster_mock.add_broker(
      model::node_id(1), net::unresolved_address{"localhost", 9092});
    cluster.start().get();
    auto cid = cluster.register_metadata_cb(
      [&](const kafka::client::metadata_update&) { callback_invocations++; });
    RPTEST_REQUIRE_EVENTUALLY(30s, [&]() { return callback_invocations >= 5; });
    cluster.unregister_metadata_cb(cid);
}

TEST_F(cluster_mock_fixture, TestApiVersionDiscovery) {
    cluster_mock.register_default_handlers();
    auto cluster = create_client_cluster();

    model::node_id broker_0(0);
    cluster_mock.add_broker(
      broker_0, net::unresolved_address{"localhost", 9092});
    cluster_mock.set_supported_versions(
      broker_0,
      kafka::metadata_api::key,
      api_version_range{
        .min = kafka::api_version(0), .max = kafka::api_version(8)});
    cluster.start().get();
    auto deferred_stop = ss::defer([&cluster] { cluster.stop().get(); });

    /**
     * In this single node cluster the supported API versions for the
     * cluster and the broker should be the same.
     */
    auto cluster_versions
      = cluster.supported_api_versions(kafka::metadata_api::key).get();
    auto broker_versions
      = cluster.supported_api_versions(broker_0, kafka::metadata_api::key)
          .get();
    ASSERT_TRUE(cluster_versions.has_value());
    ASSERT_TRUE(broker_versions.has_value());
    ASSERT_EQ(cluster_versions->min, kafka::api_version(0));
    ASSERT_EQ(cluster_versions->max, kafka::api_version(8));
    ASSERT_EQ(cluster_versions.value(), broker_versions.value());

    // Add a second broker with different API versions
    model::node_id broker_1(1);
    cluster_mock.add_broker(
      broker_1, net::unresolved_address{"localhost", 9093});
    cluster_mock.set_supported_versions(
      broker_1,
      kafka::metadata_api::key,
      api_version_range{
        .min = kafka::api_version(1), .max = kafka::api_version(10)});
    // wait for the cluster to update its metadata
    cluster.request_metadata_update().get();
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&cluster]() { return cluster.get_brokers().size() == 2; });

    // The cluster should now report the intersection of the two brokers'
    // supported API versions.
    cluster_versions
      = cluster.supported_api_versions(kafka::metadata_api::key).get();
    broker_versions
      = cluster.supported_api_versions(broker_1, kafka::metadata_api::key)
          .get();
    ASSERT_TRUE(cluster_versions.has_value());
    ASSERT_TRUE(broker_versions.has_value());
    ASSERT_EQ(cluster_versions->min, kafka::api_version(1));
    ASSERT_EQ(cluster_versions->max, kafka::api_version(8));
    ASSERT_EQ(broker_versions->min, kafka::api_version(1));
    ASSERT_EQ(broker_versions->max, kafka::api_version(10));
}

TEST_F(cluster_mock_fixture, TestTopicMetadata) {
    auto cluster_authorized_ops = kafka::cluster_authorized_operations{0x101};
    cluster_mock.register_default_handlers();
    cluster_mock.set_cluster_authorized_operations(cluster_authorized_ops);
    auto cluster = create_client_cluster();

    cluster_mock.add_broker(
      model::node_id(1), net::unresolved_address{"localhost", 9092});
    cluster.start().get();
    auto deferred_stop = ss::defer([&cluster] { cluster.stop().get(); });
    auto topic_id = model::topic_id{uuid_t::create()};
    cluster_mock.add_topic(
      model::topic{"test-topic"},
      3,
      1,
      kafka::topic_authorized_operations{0x508},
      topic_id);
    EXPECT_NO_THROW(cluster.request_metadata_update().get())
      << "request_metadata_update threw";
    RPTEST_REQUIRE_EVENTUALLY(30s, [&] {
        const auto& topics = cluster.get_topics().topics();
        return topics.size() == 1 && topics[0] == model::topic{"test-topic"};
    });
    const auto& topics = cluster.get_topics();
    auto num_parts = topics.partition_count(model::topic_view{"test-topic"});
    EXPECT_EQ(num_parts.value(), 3);
    auto auth_ops = topics.authorized_operations_for_topic(
      model::topic_view{"test-topic"});
    EXPECT_EQ(auth_ops.value(), 0x508);
    EXPECT_EQ(
      cluster.get_cluster_authorized_operations(), cluster_authorized_ops);
    EXPECT_EQ(topics.topic_id_for_name(model::topic{"test-topic"}), topic_id);
}
