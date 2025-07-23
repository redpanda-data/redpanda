

#include "kafka/client/cluster.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace kafka::client;

class ClusterFixture
  : public redpanda_thread_fixture
  , public seastar_test {
public:
    ClusterFixture()
      : redpanda_thread_fixture() {}
    virtual seastar::future<> SetUpAsync() {
        return seastar::make_ready_future<>();
    }
    virtual seastar::future<> TearDownAsync() {
        return seastar::make_ready_future<>();
    }

    kafka::client::cluster create_cluster() {
        kafka::client::connection_configuration config{
          .initial_brokers = {net::unresolved_address{"localhost", 9092}},
          .client_id = "test_client",
        };
        return kafka::client::cluster{std::move(config)};
    }
};

TEST_F(ClusterFixture, TestClusterMetadataDiscovery) {
    wait_for_controller_leadership().get();

    auto cluster = create_cluster();
    cluster.start().get();
    auto deferred_stop = ss::defer([&] { cluster.stop().get(); });

    auto& brokers = cluster.get_brokers();
    ASSERT_FALSE(brokers.empty());
    brokers.find(model::node_id(1)).get();

    ASSERT_EQ(app.controller->self(), cluster.get_controller_id());
    model::topic tp("test-topic-for-metadata");
    add_topic(
      model::topic_namespace(
        model::kafka_namespace, model::topic("test-topic-for-metadata")))
      .get();
    // refresh metadata to include the new topic
    cluster.request_metadata_update().get();
    auto leader_id = cluster.get_topics().leader(
      model::topic_partition(tp, model::partition_id(0)));

    ASSERT_EQ(leader_id, app.controller->self());
}

TEST_F(ClusterFixture, TestClusterRestartAndClientReconnection) {
    wait_for_controller_leadership().get();

    // Create and start the client cluster
    auto cluster = create_cluster();
    cluster.start().get();
    auto deferred_stop = ss::defer([&] { cluster.stop().get(); });

    // Create a test topic
    model::topic tp("test-topic-for-restart");
    add_topic(model::topic_namespace(model::kafka_namespace, tp)).get();

    // Verify initial connection and metadata discovery
    auto& brokers = cluster.get_brokers();
    ASSERT_FALSE(brokers.empty());
    brokers.find(model::node_id(1)).get();
    ASSERT_EQ(app.controller->self(), cluster.get_controller_id());

    // Refresh metadata to include the new topic
    cluster.request_metadata_update().get();
    auto leader_id = cluster.get_topics().leader(
      model::topic_partition(tp, model::partition_id(0)));
    ASSERT_EQ(leader_id, app.controller->self());

    // Restart the fixture cluster (keeping existing data)
    restart(should_wipe::no);
    wait_for_controller_leadership().get();

    // Verify client can still dispatch requests after cluster restart
    cluster.request_metadata_update().get();

    // Check that brokers are still discoverable
    auto& brokers_after_restart = cluster.get_brokers();
    ASSERT_FALSE(brokers_after_restart.empty());
    brokers_after_restart.find(model::node_id(1)).get();

    // Verify controller is still accessible
    ASSERT_EQ(app.controller->self(), cluster.get_controller_id());

    // Verify topic metadata is still accessible
    auto leader_id_after_restart = cluster.get_topics().leader(
      model::topic_partition(tp, model::partition_id(0)));
    ASSERT_EQ(leader_id_after_restart, app.controller->self());
}
