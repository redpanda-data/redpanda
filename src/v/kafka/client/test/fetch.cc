/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "redpanda/tests/fixture.h"
#include "utils/unresolved_address.h"

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

FIXTURE_TEST(fetch, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_connected_client();
    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(1));
    client.connect().get();

    {
        info("Fetching from unknown topic");
        auto ntp = make_default_ntp(
          model::topic("unknown"), model::partition_id(0));
        auto res{
          client.fetch_partition(ntp.tp, model::offset(0), 1024, 1000ms).get()};
        const auto& p = res.data.topics[0];
        BOOST_REQUIRE_EQUAL(p.name, ntp.tp.topic);
        BOOST_REQUIRE_EQUAL(p.partitions.size(), 1);
        BOOST_REQUIRE_EQUAL(p.partitions[0].partition_index, ntp.tp.partition);
        BOOST_REQUIRE_EQUAL(
          p.partitions[0].error_code,
          kafka::error_code::unknown_topic_or_partition);
    }

    info("Adding known topic");
    auto tp_ns = make_data(get_next_partition_revision_id().get(), 1);
    auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id{0});

    info("Waiting for topic data");
    wait_for_partition_offset(ntp, model::offset{1}).get();

    {
        info("Fetching from nonempty known topic");
        client.config().retries.set_value(size_t(3));
        auto res{
          client.fetch_partition(ntp.tp, model::offset(0), 1024, 1000ms).get()};
        const auto& p = res.data.topics[0];
        BOOST_REQUIRE_EQUAL(p.name, ntp.tp.topic);
        BOOST_REQUIRE_EQUAL(p.partitions.size(), 1);
        const auto& r = p.partitions[0];
        BOOST_REQUIRE_EQUAL(r.partition_index, ntp.tp.partition);
        BOOST_REQUIRE_EQUAL(r.error_code, kafka::error_code::none);
    }

    client.stop().get();
}
