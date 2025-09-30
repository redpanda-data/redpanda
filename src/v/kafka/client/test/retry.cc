// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_frontend.h"
#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/exceptions.h"
#include "model/timeout_clock.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

inline const model::topic_partition unknown_tp{
  model::topic{"unknown"}, model::partition_id{0}};

FIXTURE_TEST(test_retry_produce, kafka_client_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(5));

    auto res = client
                 .produce_record_batch(
                   unknown_tp, make_batch(model::offset{0}, 12))
                 .get();
    BOOST_REQUIRE_EQUAL(
      res.error_code, kafka::error_code::unknown_topic_or_partition);
}

class kafka_client_create_topic_fixture : public kafka_client_fixture {
public:
    kafka_client_create_topic_fixture()
      : kafka_client_fixture() {
        using cluster::client_quota::alter_delta_cmd_data;
        using cluster::client_quota::entity_key;
        using cluster::client_quota::entity_value_diff;

        const entity_key key0{entity_key::client_id_default_match{}};
        auto cmd = alter_delta_cmd_data{
            .ops = {
            alter_delta_cmd_data::op{
                .key = key0,
                .diff = {
                  .entries = {
                    entity_value_diff::entry(
                      entity_value_diff::key::controller_mutation_rate, 1),
                    },
                  },
                },
            },
        };
        app.controller->get_quota_frontend()
          .local()
          .alter_quotas(cmd, model::timeout_clock::now() + 10s)
          .get();
    }
};

FIXTURE_TEST(test_retry_create_topic, kafka_client_create_topic_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(5));

    auto make_topic = [](ss::sstring name) {
        return kafka::creatable_topic{
          .name = model::topic{name},
          .num_partitions = 1,
          .replication_factor = 1};
    };

    size_t num_topics = 20;
    for (size_t i = 0; i < num_topics; ++i) {
        auto creatable_topic = make_topic(fmt::format("topic-{}", i));
        try {
            auto res = client.create_topic(creatable_topic).get();
            BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
            for (auto& topic : res.data.topics) {
                BOOST_REQUIRE_EQUAL(topic.name, creatable_topic.name);
                BOOST_REQUIRE_EQUAL(topic.error_code, kafka::error_code::none);
            }
        } catch (const kafka::client::topic_error& ex) {
            // If we do get an error, then it should be
            // throttling_quota_exceeded. Anything else is a problem
            BOOST_REQUIRE_EQUAL(ex.topic, creatable_topic.name);
            BOOST_REQUIRE_EQUAL(
              ex.error, kafka::error_code::throttling_quota_exceeded);
        }
    }
}
