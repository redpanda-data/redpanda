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
#include "kafka/client/broker.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/logger.h"
#include "kafka/client/producer.h"
#include "kafka/client/test/utils.h"
#include "kafka/client/topic_cache.h"
#include "test_utils/async.h"
#include "test_utils/test.h"
#include "utils/prefix_logger.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace kafka::client {
struct producer_test_access {
    static ss::future<>
    send(producer& p, model::topic_partition tp, model::record_batch batch) {
        return p.send(std::move(tp), std::move(batch));
    }
    static bool send_in_flight(producer& p, const model::topic_partition& tp) {
        return p._in_flight_sends.contains(tp);
    }
};
} // namespace kafka::client

namespace {
namespace kc = kafka::client;

struct null_broker_factory final : kc::broker_factory {
    ss::future<kc::shared_broker_t>
    create_broker(model::node_id, net::unresolved_address) final {
        throw std::runtime_error("unexpected broker creation");
    }
};
} // namespace

// producer::send() asserts against a concurrent dispatch to the same
// partition. The public API cannot trigger it (produce_partition serializes
// dispatch; see test_produce_partition_serializes_dispatch), so the second
// dispatch goes through test access.
TEST(ProducerDeathTest, ConcurrentDispatchHitsGuard) {
    kc::configuration cfg;
    // dispatch a produce request on the first record
    cfg.produce_batch_record_count.set_value(1);
    cfg.produce_batch_size_bytes.set_value(1);
    kc::retries_configuration retries{
      .max_retries = 3,
      // parks the first dispatch in retry backoff while the second is issued
      .retry_base_backoff = 10s,
    };
    kc::topic_cache topic_cache;
    prefix_logger logger(kc::kclog, "producer_test");
    kc::brokers brokers(logger, std::make_unique<null_broker_factory>());
    kc::producer p(
      kc::producer_configuration::from_config_store(cfg),
      retries,
      topic_cache,
      brokers,
      logger,
      [](std::exception_ptr) { return ss::now(); });

    model::topic_partition tp(model::topic("t"), model::partition_id(0));

    // The batch is dispatched through produce_partition's consumer; with no
    // leader known it fails into retry backoff, holding the in-flight guard.
    auto produce_fut = p.produce(tp, make_batch(model::offset(0), 1));
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return kc::producer_test_access::send_in_flight(p, tp); });

    ASSERT_DEATH(
      {
          (void)kc::producer_test_access::send(
            p, tp, make_batch(model::offset(1), 1));
      },
      "concurrent produce dispatch");

    // stop() aborts the parked dispatch; the record resolves with an error
    p.stop().get();
    auto res = produce_fut.get();
    EXPECT_NE(res.error_code, kafka::error_code::none);
}
