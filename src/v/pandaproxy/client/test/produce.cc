// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/errors.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/produce_request.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "pandaproxy/client/client.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/test/pandaproxy_client_fixture.h"
#include "pandaproxy/client/test/utils.h"
#include "utils/unresolved_address.h"

#include <chrono>

namespace ppc = pandaproxy::client;

FIXTURE_TEST(pandaproxy_produce_reconnect, ppc_test_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    ppc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    ppc::shard_local_cfg().retries.set_value(size_t(0));

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();

    info("Connecting client");
    wait_for_controller_leadership().get();

    {
        info("Producing to unknown topic");
        auto bat = make_batch(model::offset(0), 2);
        auto res = client.produce_record_batch(tp, std::move(bat)).get();
        BOOST_REQUIRE_EQUAL(
          res.error, kafka::error_code::unknown_topic_or_partition);
        BOOST_REQUIRE_EQUAL(res.base_offset, model::offset(-1));
    }

    auto ntp = make_default_ntp(tp.topic, tp.partition);
    info("Adding known topic");
    add_topic(model::topic_namespace_view(ntp)).get();

    info("Client.dispatch metadata");
    auto res
      = client.dispatch(kafka::metadata_request{.list_all_topics = true}).get();
    BOOST_REQUIRE_EQUAL(res.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(res.topics[0].name(), "t");

    ppc::shard_local_cfg().produce_batch_record_count.set_value(3);
    ppc::shard_local_cfg().produce_batch_size_bytes.set_value(1024);
    ppc::shard_local_cfg().produce_batch_delay.set_value(1000ms);

    auto bat0 = make_batch(model::offset(0), 2);
    auto bat1 = make_batch(model::offset(2), 1);
    auto bat2 = make_batch(model::offset(3), 3);

    ppc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    ppc::shard_local_cfg().retries.set_value(size_t(10));

    info("Producing to known topic");
    auto req0_fut = client.produce_record_batch(tp, std::move(bat0));
    auto req1_fut = client.produce_record_batch(tp, std::move(bat1));
    auto req2_fut = client.produce_record_batch(tp, std::move(bat2));

    info("Waiting for results");
    auto req0 = req0_fut.get();
    auto req1 = req1_fut.get();
    auto req2 = req2_fut.get();

    info("Testing assertions");
    BOOST_REQUIRE_EQUAL(req0.error, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(req0.base_offset, model::offset(1));

    client.stop().get();
}
