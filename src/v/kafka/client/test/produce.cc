// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/produce.h"

#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/unresolved_address.h"

#include <chrono>

FIXTURE_TEST(produce_reconnect, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();
    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(0));

    info("Connecting client");
    wait_for_controller_leadership().get();

    {
        info("Producing to unknown topic");
        auto bat = make_batch(model::offset(0), 2);
        auto res = client.produce_record_batch(tp, std::move(bat)).get();
        BOOST_REQUIRE_EQUAL(
          res.error_code, kafka::error_code::unknown_topic_or_partition);
        BOOST_REQUIRE_EQUAL(res.base_offset, model::offset(-1));
    }

    auto ntp = make_default_ntp(tp.topic, tp.partition);
    info("Adding known topic");
    add_topic(model::topic_namespace_view(ntp)).get();

    info("Client.dispatch metadata");
    auto res = client.dispatch(make_list_topics_req()).get();
    BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(res.data.topics[0].name(), "t");

    client.config().produce_batch_record_count.set_value(3);
    client.config().produce_batch_size_bytes.set_value(1024);
    client.config().produce_batch_delay.set_value(1000ms);

    auto bat0 = make_batch(model::offset(0), 2);
    auto bat1 = make_batch(model::offset(2), 1);
    auto bat2 = make_batch(model::offset(3), 3);

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(10));

    info("Producing to known topic");
    auto req0_fut = client.produce_record_batch(tp, std::move(bat0));
    auto req1_fut = client.produce_record_batch(tp, std::move(bat1));
    auto req2_fut = client.produce_record_batch(tp, std::move(bat2));

    info("Waiting for results");
    auto req0 = req0_fut.get();
    auto req1 = req1_fut.get();
    auto req2 = req2_fut.get();

    info("Testing assertions");
    BOOST_REQUIRE_EQUAL(req0.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(req0.base_offset, model::offset(0));

    client.stop().get();
}

FIXTURE_TEST(produce_clean_shutdown, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();
    client.config().retry_base_backoff.set_value(1ms);

    /// Set a high retry value that previous to the changes to the
    /// retry_with_migitation loops, would hold up the client from exiting
    /// during shutdown
    client.config().retries.set_value(size_t(10000));

    info("Connecting client");
    wait_for_controller_leadership().get();

    auto produce_to_unknown_topic = [&client, tp]() {
        info("Producing to unknown topic");
        auto bat = make_batch(model::offset(0), 2);
        return client.produce_record_batch(tp, std::move(bat))
          .then([](auto res) {
              BOOST_REQUIRE_EQUAL(
                res.error_code, kafka::error_code::operation_not_attempted);
              BOOST_REQUIRE_EQUAL(res.base_offset, model::offset(-1));
          });
    };

    auto shutdown_client = [&client]() {
        info("Shutting down client");
        /// Sleep for 2s to allow the client to send its first produce request
        /// (from the method above) and be stuck in a never ending retry /
        /// mitigation state.
        return ss::sleep(2s).then([&client] {
            return client.stop().then([] { info("Client shutdown"); });
        });
    };

    /// Ensure the test explicity fails and terminates in the case stop() fails
    /// to work, as the program would indefinitely hang
    ss::abort_source as;
    auto verify
      = ss::sleep_abortable(10s, as)
          .then([] {
              vassert(
                false,
                "Failed to shutdown client within 10 seconds after stop() was "
                "called");
          })
          .handle_exception_type([](const ss::sleep_aborted&) {});

    /// If this line hangs for more then 10s, the vassert above will shutdown
    /// the program
    ss::when_all_succeed(produce_to_unknown_topic(), shutdown_client()).get();

    /// ... otherwise on success the above async work will be cancelled and the
    /// program can exit gracefully
    as.request_abort();
    verify.get();
}
