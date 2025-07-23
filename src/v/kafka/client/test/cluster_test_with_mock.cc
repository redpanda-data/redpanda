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
