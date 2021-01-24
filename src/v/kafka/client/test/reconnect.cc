// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/errors.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/produce_request.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/unresolved_address.h"

#include <chrono>

namespace kc = kafka::client;

FIXTURE_TEST(reconnect, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    kc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    kc::shard_local_cfg().retries.set_value(size_t(0));

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();

    {
        info("Checking no topics");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 0);
    }

    {
        info("Adding known topic");
        auto ntp = make_default_ntp(tp.topic, tp.partition);
        add_topic(model::topic_namespace_view(ntp)).get();
    }

    {
        info("Checking for known topic");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.topics[0].name(), "t");
    }

    {
        info("Restarting broker");
        restart();
    }

    {
        info("Checking for known topic - expect controller not ready");
        auto res = client.dispatch(make_list_topics_req());
        BOOST_REQUIRE_THROW(res.get(), kc::broker_error);
    }

    {
        kc::shard_local_cfg().retries.set_value(size_t(5));
        info("Checking for known topic - controller ready");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.topics[0].name(), "t");
    }

    info("Stopping client");
    client.stop().get();
}
