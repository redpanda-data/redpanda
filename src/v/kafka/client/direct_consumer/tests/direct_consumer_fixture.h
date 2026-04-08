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

#pragma once

#include "cluster/tests/cluster_test_fixture.h"
#include "container/chunked_vector.h"
#include "gtest/gtest.h"
#include "kafka/client/direct_consumer/direct_consumer.h"

#include <chrono>

using namespace kafka::client;

namespace kafka::client::tests {

class consumer_fixture : public cluster_test_fixture {
public:
    kafka::client::connection_configuration make_connection_config();

    ss::future<std::unique_ptr<kafka::client::cluster>> create_client_cluster(
      std::optional<std::unique_ptr<kafka::client::broker_factory>>
        broker_factory = std::nullopt);

    topic_assignment make_assignment(
      const model::topic& topic,
      std::vector<int> partitions,
      std::optional<kafka::offset> initial_offset = std::nullopt);

    // you can't perform a .get() inside a coroutine
    ss::future<model::node_id>
    get_partition_leader_async(const model::ntp& ntp);

    model::node_id get_partition_leader(const model::ntp& ntp);

    ss::future<kafka::offset> produce_to_partition(
      const model::topic& topic,
      int partition,
      model::record_batch batch,
      std::chrono::milliseconds timeout = default_retry_timeout,
      std::chrono::milliseconds backoff = default_retry_backoff);

    ss::future<> produce_to_partition(
      const model::topic& topic,
      int partition,
      size_t record_count,
      std::chrono::milliseconds timeout = default_retry_timeout,
      std::chrono::milliseconds backoff = default_retry_backoff);

    ss::future<chunked_vector<model::record>> consume_from_partition(
      const model::topic& topic, int partition, kafka::offset offset);

    chunked_hash_map<
      model::topic_partition,
      chunked_vector<model::record_batch>>
    fetch_until_empty(direct_consumer& consumer);
    void assign_partitions(topic_assignment assgn);
    void unassign_partition(model::topic_partition tp);
    void unassign_topic(model::topic topic);

    // shuffle leadership, wait for leadership change to become visible to the
    // test
    void wait_for_visible_leadership_shuffle(const model::ntp& ntp);

    application* create_node_application(model::node_id node_id);

protected:
    redpanda_thread_fixture* rp;
    std::unique_ptr<kafka::client::cluster> cluster;
    std::unique_ptr<kafka::client::direct_consumer> consumer;
    model::topic topic{"test-topic"};

    static constexpr auto default_retry_timeout = 1s;
    static constexpr auto default_retry_backoff = 100ms;
};

enum class session_config : uint8_t {
    with_sessions,
    without_sessions,
    toggle_sessions,
};

[[maybe_unused]] inline auto format_as(session_config c) {
    return fmt::underlying(c);
}

class basic_consumer_fixture
  : public consumer_fixture
  , public ::testing::TestWithParam<session_config> {
public:
    void SetUp() override;
    void TearDown() override;
    virtual void StartConsumer();
    virtual void StopConsumer();
    void maybe_toggle_fetch_sessions();

protected:
    std::unique_ptr<kafka::client::direct_consumer> make_consumer();
};
} // namespace kafka::client::tests
