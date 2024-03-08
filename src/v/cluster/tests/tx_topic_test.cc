// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/types.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

FIXTURE_TEST(test_tm_stm_new_tx, redpanda_thread_fixture) {
    // call find coordinator to initialize the topic
    auto coordinator = app.tx_gateway_frontend.local()
                         .find_coordinator(
                           kafka::transactional_id{"test-tx-id"})
                         .get();

    auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
      model::tx_manager_nt);

    BOOST_REQUIRE(
      cfg->properties.retention_duration.value()
      == config::shard_local_cfg()
           .transaction_coordinator_delete_retention_ms());
    BOOST_REQUIRE(
      cfg->properties.segment_size.value()
      == config::shard_local_cfg().transaction_coordinator_log_segment_size());

    /**
     * Change property
     */
    static size_t new_segment_size = 1024 * 1024;
    config::shard_local_cfg()
      .transaction_coordinator_log_segment_size.set_value(new_segment_size);

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return new_segment_size == cfg->properties.segment_size.value();
    });

    static std::chrono::milliseconds new_retention_ms(100000);
    config::shard_local_cfg()
      .transaction_coordinator_delete_retention_ms.set_value(new_retention_ms);

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return new_retention_ms == cfg->properties.retention_duration.value();
    });

    cfg = app.controller->get_topics_state().local().get_topic_cfg(
      model::tx_manager_nt);
    // segment size should state the same
    BOOST_REQUIRE_EQUAL(new_segment_size, cfg->properties.segment_size.value());
    /**
     * Change both properties at once
     */
    static size_t newer_segment_size = 20000000;
    static std::chrono::milliseconds newer_retention_ms(500000);
    config::shard_local_cfg()
      .transaction_coordinator_log_segment_size.set_value(newer_segment_size);

    config::shard_local_cfg()
      .transaction_coordinator_delete_retention_ms.set_value(
        newer_retention_ms);
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return newer_retention_ms == cfg->properties.retention_duration.value()
               && cfg->properties.segment_size.value() == newer_segment_size;
    });
}
