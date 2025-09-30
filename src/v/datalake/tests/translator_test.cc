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
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"

namespace kc = kafka::client;

class partition_translator_test_fixture
  : public datalake::tests::datalake_cluster_test_fixture {
    static constexpr size_t num_brokers = 3;
    using base = datalake::tests::datalake_cluster_test_fixture;

public:
    partition_translator_test_fixture() {
        for (size_t i = 0; i < num_brokers; i++) {
            add_node();
        }
        wait_for_all_members(5s).get();
        reduced_lag.get("iceberg_target_lag_ms").set_value(10000ms);

        create_iceberg_topic(test_topic.tp).get();
        // create a kafka client for the cluster.
        auto* rp = instance(model::node_id{0});
        _client = std::make_unique<kc::client>(rp->proxy_client_config());
        _client->set_retry_base_backoff(1ms);
        _client->set_max_retries(size_t(20));
        _client->connect().get();
    }

    ~partition_translator_test_fixture() {
        if (_client) {
            _client->stop().get();
        }
    }

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

    scoped_config reduced_lag;

    model::topic_namespace test_topic{
      model::kafka_namespace, model::topic{"test"}};

    std::unique_ptr<kc::client> _client;
};

FIXTURE_TEST(test_basic, partition_translator_test_fixture) {
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
    ss::when_all_succeed(background.begin(), background.end()).get();
    validate_translated_files(ntp).get();
}
