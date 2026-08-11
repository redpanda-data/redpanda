/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace kafka::client;
using namespace std::chrono_literals;

namespace {

client_configuration make_config() {
    // A seed broker that refuses connections: nothing listens on this
    // port. The large connection_timeout makes the broker reconnect loop
    // park in long sleep_abortable backoffs (connection_timeout / 20 per
    // sleep) tied to the *cluster's* abort source, not the client's.
    client_configuration cfg{};
    cfg.connection_cfg = connection_configuration{
      .initial_brokers = {net::unresolved_address{"127.0.0.1", 1}},
      .client_id = "client-stop-test",
      .connection_timeout = 10min,
    };
    cfg.producer_cfg = producer_configuration{
      .batch_record_count = 1,
      .batch_size_bytes = 1,
      .batch_delay = 0ms,
      .compression_type = model::compression::none,
      .shutdown_delay = 0ms,
      .ack_level = acks_all,
    };
    // A tiny retry backoff so a failed request proceeds immediately into
    // its mitigation (update_metadata -> seed reconnect) rather than
    // parking in the retry sleep, which the client/producer abort sources
    // already cover. The wedge under test is the cluster-layer reconnect
    // backoff above.
    cfg.retries_cfg = retries_configuration{
      .max_retries = 3,
      .retry_base_backoff = 1ms,
    };
    return cfg;
}

} // namespace

// Reproduces a shutdown deadlock: a request fiber holds the client gate
// while parked inside the cluster metadata layer (seed-broker reconnect
// backoff, guarded by the cluster's abort source). client::stop() waits on
// _gate.close() before anything aborts the cluster layer, and the only
// call that does (cluster::stop()) is sequenced after the gate close — so
// stop() can never complete.
//
// The request path that wedges: dispatch -> no brokers -> broker_error ->
// mitigate_error -> update_metadata -> take _update_lock ->
// initialize_metadata_with_seed -> connect to (dead) seed ->
// sleep_abortable(backoff, cluster::_as).
TEST_CORO(client_stop, stop_completes_with_wedged_metadata_request) {
    client c{make_config(), std::nullopt};

    auto req = c.dispatch([]() { return kafka::metadata_request{}; });

    co_await tests::drain_task_queue();
    ASSERT_FALSE_CORO(req.available());

    // stop() must complete promptly even though a request is wedged in the
    // cluster layer.
    co_await c.stop();

    // The wedged request must have been unwound exceptionally.
    ASSERT_TRUE_CORO(req.available());
    EXPECT_THROW(req.get(), std::exception);
}

// The produce-path variant: the wedged fiber holds the *producer's* gate
// (send -> unknown leader -> mitigate_error -> update_metadata -> seed
// reconnect backoff), and producer::stop() awaits that gate before
// client::stop() reaches the gate close that the dispatch test exercises.
// Catches a regression of cluster shutdown_input() being sequenced after
// _producer.stop().
TEST_CORO(client_stop, stop_completes_with_wedged_produce) {
    client c{make_config(), std::nullopt};

    auto req = c.produce_record_batch(
      model::topic_partition{model::topic{"t"}, model::partition_id{0}},
      make_batch(model::offset{0}, 1));

    // A real sleep, not just a task-queue drain: the batcher hands the
    // batch to send() off a timer, and the send must fail its first
    // attempt and enter the seed-reconnect backoff for the fiber to be
    // wedged in the cluster layer.
    co_await ss::sleep(500ms);
    ASSERT_FALSE_CORO(req.available());

    co_await c.stop();

    ASSERT_TRUE_CORO(req.available());
}
