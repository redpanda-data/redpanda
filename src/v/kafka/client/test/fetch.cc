/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/protocol/fetch.h"

#include "cluster/partition_leaders_table.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "redpanda/tests/fixture.h"
#include "utils/unresolved_address.h"

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

namespace kc = kafka::client;

FIXTURE_TEST(fetch, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    kc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    kc::shard_local_cfg().retries.set_value(size_t(1));
    auto client = make_connected_client();
    client.connect().get();

    {
        info("Fetching from unknown topic");
        auto ntp = make_default_ntp(
          model::topic("unknown"), model::partition_id(0));
        auto res{
          client.fetch_partition(ntp.tp, model::offset(0), 1024, 1000ms).get()};
        const auto& p = res.partitions[0];
        BOOST_REQUIRE_EQUAL(p.name, ntp.tp.topic);
        BOOST_REQUIRE_EQUAL(p.responses.size(), 1);
        BOOST_REQUIRE_EQUAL(p.responses[0].id, ntp.tp.partition);
        BOOST_REQUIRE_EQUAL(
          p.responses[0].error, kafka::error_code::unknown_topic_or_partition);
    }

    info("Adding known topic");
    auto tp_ns = make_data(model::revision_id(2), 1);
    auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id{0});

    info("Waiting for topic data");
    wait_for_partition_offset(ntp, model::offset{1}).get();

    {
        info("Fetching from nonempty known topic");
        kc::shard_local_cfg().retries.set_value(size_t(3));
        auto res{
          client.fetch_partition(ntp.tp, model::offset(0), 1024, 1000ms).get()};
        const auto& p = res.partitions[0];
        BOOST_REQUIRE_EQUAL(p.name, ntp.tp.topic);
        BOOST_REQUIRE_EQUAL(p.responses.size(), 1);
        auto const& r = p.responses[0];
        BOOST_REQUIRE_EQUAL(r.id, ntp.tp.partition);
        BOOST_REQUIRE_EQUAL(r.error, kafka::error_code::none);
    }

    client.stop().get();
}
