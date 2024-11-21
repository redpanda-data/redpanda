/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/tests/fixture.h"
#include "kafka/client/client.h"
#include "kafka/client/test/utils.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

namespace kc = kafka::client;

class PartitionTranslatorTestFixture
  : public seastar_test
  , public datalake::tests::datalake_cluster_test_fixture {
    static constexpr size_t num_brokers = 3;

    ss::future<> SetUpAsync() override {
        for (int i = 0; i < num_brokers; i++) {
            add_node();
        }
        co_await wait_for_all_members(5s);
        reduced_commit_interval.get("iceberg_catalog_commit_interval_ms")
          .set_value(10000ms);

        co_await create_iceberg_topic(test_topic.tp);
        // create a kafka client for the cluster.
        auto* rp = instance(model::node_id{0});
        _client = std::make_unique<kc::client>(rp->proxy_client_config());
        _client->config().retry_base_backoff.set_value(1ms);
        _client->config().retries.set_value(size_t(20));
        co_await _client->connect();
    }

    ss::future<> TearDownAsync() override {
        if (_client) {
            co_await _client->stop();
        }
    }

public:
    ss::future<>
    produce_data_for(model::ntp& ntp, std::chrono::seconds duration) {
        auto stop_time = ss::lowres_clock::now() + duration;
        return ss::do_until(
          [stop_time] { return ss::lowres_clock::now() > stop_time; },
          [this, &ntp] {
              return _client
                ->produce_record_batch(ntp.tp, make_batch(model::offset(0), 5))
                .then([](kafka::partition_produce_response resp) {
                    if (resp.errored()) {
                        // maybe possible with leadership changes.
                        return ss::now();
                    }
                    return ss::sleep(1ms);
                });
          });
    }

    ss::future<>
    shuffle_leadership(model::ntp& ntp, std::chrono::seconds duration) {
        auto stop_time = ss::lowres_clock::now() + duration;
        return ss::do_until(
          [stop_time] { return ss::lowres_clock::now() > stop_time; },
          [this, &ntp] {
              vlog(logger.info, "shuffling leadership {}", ntp);
              return cluster_test_fixture::shuffle_leadership(ntp).then(
                [] { return ss::sleep(500ms); });
          });
    }

    scoped_config reduced_commit_interval;

    model::topic_namespace test_topic{
      model::kafka_namespace, model::topic{"test"}};

    std::unique_ptr<kc::client> _client;
};

TEST_F_CORO(PartitionTranslatorTestFixture, TestBasic) {
    auto ntp = model::ntp(test_topic.ns, test_topic.tp, model::partition_id{0});
    std::chrono::seconds test_runtime = 30s;
    std::vector<ss::future<>> background;
    background.reserve(5);
    background.push_back(produce_data_for(ntp, test_runtime));
    // roundrobin leadership across test topic replicas
    background.push_back(shuffle_leadership(ntp, test_runtime));
    for (auto i = 0; i < 3; i++) {
        auto f = ss::do_with(
          model::ntp(
            model::datalake_coordinator_nt.ns,
            model::datalake_coordinator_nt.tp,
            model::partition_id{i}),
          [this, test_runtime](auto& c_ntp) {
              return shuffle_leadership(c_ntp, test_runtime);
          });
        background.push_back(std::move(f));
    }
    co_await ss::when_all_succeed(background.begin(), background.end());
    co_await validate_translated_files(ntp);
}
