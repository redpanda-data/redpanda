#include "kafka/client/cluster.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/produce.h"
#include "redpanda/tests/fixture.h"
#include "storage/types.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace kafka::client;

kafka::client::connection_configuration
make_cluster_config(std::chrono::milliseconds max_metadata_age) {
    return kafka::client::connection_configuration{
      .initial_brokers = {net::unresolved_address{"localhost", 9092}},
      .client_id = "test_client",
      .max_metadata_age = max_metadata_age,
    };
}

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

    kafka::client::cluster
    create_cluster(std::chrono::milliseconds max_metadata_age = 10s) {
        return kafka::client::cluster{make_cluster_config(max_metadata_age)};
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
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return cluster.request_metadata_update()
          .then([&] {
              cluster.get_topics().leader(
                model::topic_partition(tp, model::partition_id(0)));
              return true;
          })
          .handle_exception([](const std::exception_ptr&) { return false; });
    });

    auto leader_id = cluster.get_topics().leader(
      model::topic_partition(tp, model::partition_id(0)));
    ASSERT_EQ(leader_id, app.controller->self());
    // Restart the fixture cluster (keeping existing data)
    restart(should_wipe::no);
    wait_for_controller_leadership().get();

    // Verify client can still dispatch requests after cluster restart
    try {
        cluster.request_metadata_update().get();
    } catch (const std::exception& e) {
        FAIL() << "Cluster restart failed to allow metadata update: "
               << e.what();
    }
    // Check that brokers are still discoverable
    auto& brokers_after_restart = cluster.get_brokers();
    ASSERT_FALSE(brokers_after_restart.empty());
    brokers_after_restart.find(model::node_id(1)).get();

    // Verify controller is still accessible
    ASSERT_EQ(app.controller->self(), cluster.get_controller_id());

    // Verify topic metadata is still accessible
    // Refresh metadata to include the new topic
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return cluster.request_metadata_update()
          .then([&] {
              cluster.get_topics().leader(
                model::topic_partition(tp, model::partition_id(0)));
              return true;
          })
          .handle_exception([](const std::exception_ptr&) { return false; });
    });
}

kafka::produce_request make_produce_request(
  model::topic_partition tp, model::record_batch&& batch, acks acks) {
    chunked_vector<kafka::produce_request::partition> partitions;
    partitions.emplace_back(
      kafka::produce_request::partition{
        .partition_index{tp.partition},
        .records = kafka::produce_request_record_data(std::move(batch))});

    chunked_vector<kafka::produce_request::topic> topics;
    topics.emplace_back(
      kafka::produce_request::topic{
        .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
    return {t_id, acks, std::move(topics)};
}

model::record_batch test_batch(int i) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    builder.add_raw_kv(iobuf::from("test"), serde::to_iobuf(i));
    return std::move(builder).build();
}

ss::future<chunked_vector<model::record_batch>>
read_partition_data(ss::lw_shared_ptr<::cluster::partition> p) {
    storage::local_log_reader_config r_cfg(
      model::offset(0), model::offset::max());
    r_cfg.translate_offsets = model::translate_offsets::yes;
    r_cfg.type_filter = model::record_batch_type::raft_data;
    auto r = co_await p->make_local_reader(std::move(r_cfg));

    co_return co_await model::consume_reader_to_chunked_vector(
      std::move(r), model::no_timeout);
};

TEST_F(ClusterFixture, TestDispatchingMultipleRequests) {
    wait_for_controller_leadership().get();

    // Create and start the client cluster
    auto cluster = create_cluster();
    cluster.start().get();
    auto deferred_stop = ss::defer([&] { cluster.stop().get(); });

    // Create a test topic
    model::topic tp("test-topic-mr");
    add_topic(model::topic_namespace(model::kafka_namespace, tp)).get();

    // Verify initial connection and metadata discovery
    auto& brokers = cluster.get_brokers();
    ASSERT_FALSE(brokers.empty());
    brokers.find(model::node_id(1)).get();
    ASSERT_EQ(app.controller->self(), cluster.get_controller_id());

    // Refresh metadata to include the new topic
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        return cluster.request_metadata_update()
          .then([&] {
              cluster.get_topics().leader(
                model::topic_partition(tp, model::partition_id(0)));
              return true;
          })
          .handle_exception([](const std::exception_ptr&) { return false; });
    });

    model::ntp ntp(model::kafka_namespace, tp, model::partition_id(0));
    auto shard = app.shard_table.local().shard_for(ntp);

    ASSERT_TRUE(shard.has_value());
    /**
     * wait for leader, Kafka API gets leadership information before it is
     * elected.
     */
    RPTEST_REQUIRE_EVENTUALLY(10s, [this, shard, ntp] {
        return app.partition_manager.invoke_on(
          shard.value(), [ntp](::cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);

              return partition->is_leader();
          });
    });
    auto leader_id = cluster.get_topics().leader(
      model::topic_partition(tp, model::partition_id(0)));
    ASSERT_EQ(leader_id, app.controller->self());
    /**
     * Run concurrent produce requests
     */
    auto range
      = std::ranges::iota_view(0, 10) | std::views::transform([&](int i) {
            return make_produce_request(
              model::topic_partition(tp, model::partition_id(0)),
              test_batch(i),
              acks_all);
        })
        | std::views::transform([&](kafka::produce_request req) {
              return cluster
                .dispatch_to(
                  leader_id.value(), std::move(req), kafka::api_version{7})
                .discard_result();
          });
    ss::when_all_succeed(std::ranges::to<std::vector<ss::future<>>>(range))
      .get();
    /**
     * check that order is preserved
     */
    auto batches = app.partition_manager
                     .invoke_on(
                       shard.value(),
                       [ntp](::cluster::partition_manager& mgr) {
                           auto partition = mgr.get(ntp);
                           return read_partition_data(partition);
                       })
                     .get();

    ASSERT_EQ(batches.size(), 10);
    for (auto& b : batches) {
        auto records = b.copy_records();
        auto r_number = serde::from_iobuf<int>(records[0].value().copy());
        ASSERT_EQ(static_cast<int64_t>(r_number), b.base_offset()());
    }
}

TEST_F(ClusterFixture, TestTopicTimeout) {
    wait_for_controller_leadership().get();

    // Create and start the client cluster
    auto cluster = create_cluster(500ms);
    cluster.start().get();
    auto deferred_stop = ss::defer([&] { cluster.stop().get(); });

    model::topic tpa("test-topic-a");
    model::topic_namespace tpa_ns{model::kafka_namespace, tpa};

    add_topic(tpa_ns).get();
    RPTEST_REQUIRE_EVENTUALLY(3s, [&cluster, &tpa] {
        return std::ranges::contains(cluster.get_topics().topics(), tpa);
    });
    delete_topic(tpa_ns).get();

    // The timings in these checks are meant to bring us half a second before
    // the stale time and then half a second after the stale time
    ss::sleep(1000ms).get();
    cluster.request_metadata_update().get();
    ASSERT_TRUE(std::ranges::contains(cluster.get_topics().topics(), tpa));

    ss::sleep(1000ms).get();
    cluster.request_metadata_update().get();
    ASSERT_FALSE(std::ranges::contains(cluster.get_topics().topics(), tpa));

    // Check that updates happen correctly, as well
    kafka::client::connection_configuration new_config = make_cluster_config(
      1s);
    cluster.update_configuration(std::move(new_config));

    add_topic(tpa_ns).get();
    RPTEST_REQUIRE_EVENTUALLY(3s, [&cluster, &tpa] {
        return std::ranges::contains(cluster.get_topics().topics(), tpa);
    });
    delete_topic(tpa_ns).get();

    ss::sleep(2500ms).get();
    cluster.request_metadata_update().get();
    ASSERT_TRUE(std::ranges::contains(cluster.get_topics().topics(), tpa));

    ss::sleep(1000ms).get();
    cluster.request_metadata_update().get();
    ASSERT_FALSE(std::ranges::contains(cluster.get_topics().topics(), tpa));
}
