/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cluster/types.h"
#include "kafka/client/test/fixture.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/tests/audit_test_utils.h"
#include "test_utils/scoped_config.h"

#include <seastar/util/log.hh>

#include <gtest/gtest.h>

namespace sa = security::audit;

namespace {
class audit_log_fixture
  : public kafka_client_fixture
  , public testing::TestWithParam<bool> {
public:
    scoped_config local_cfg;

    void SetUp() override {
        local_cfg.get("audit_use_rpc").set_value(!GetParam());
    }
};

ss::logger logger{"audit-test-log"};

} // namespace

TEST_P(audit_log_fixture, test_audit_init_phase) {
    /// Knowing the size of one event allows to set a predetermined maximum
    /// shard allowance for auditing that way backpressure is applied when
    /// anticipated
    const size_t event_size = sa::authentication::construct(
                                sa::test::make_random_authn_options())
                                .estimated_size();
    vlog(logger.info, "Single event size bytes: {}", event_size);

    ss::global_logger_registry().set_logger_level(
      "auditing", ss::log_level::trace);

    sa::test::set_auditing_config_options(event_size).get();
    enable_sasl_and_restart("username");

    wait_for_controller_leadership().get();
    auto& audit_mgr = app.audit_mgr;

    /// with auditing disabled, calls to enqueue should be no-ops
    const auto n_events
      = sa::test::pending_audit_events(audit_mgr.local()).get();
    audit_mgr
      .invoke_on_all([]([[maybe_unused]] sa::audit_log_manager& m) {
          for ([[maybe_unused]] int i = 0; i < 20; ++i) {
              ASSERT_TRUE(
                m.enqueue_authn_event(sa::test::make_random_authn_options()));
          }
      })
      .get();

    ASSERT_EQ(
      sa::test::pending_audit_events(audit_mgr.local()).get(), n_events);

    /// with auditing enabled, the system should block when the threshold of
    /// audit_queue_max_buffer_size_per_shard has been reached
    local_cfg.get("audit_enabled").set_value(true);

    /// With the switch enabled the audit topic should be created
    wait_for_topics(
      {cluster::topic_result(
        model::topic_namespace(
          model::kafka_namespace, model::kafka_audit_logging_topic))})
      .get();

    /// Wait until the run loops are available, otherwise enqueuing events will
    /// pass through
    vlog(logger.info, "Waiting until the audit fibers are up");
    tests::cooperative_spin_wait_with_timeout(10s, [&audit_mgr] {
        return audit_mgr.local().is_effectively_enabled();
    }).get();

    /// Verify auditing can enqueue up until the max configured, and further
    /// calls to enqueue return false, signifying action did not occur.
    auto enqueue_some = [event_size](sa::audit_log_manager& m) {
        bool success = true;
        for (auto i = 0; i < 200; ++i) {
            const bool can_enqueue = m.avaiable_reservation() >= event_size;
            if (m.enqueue_authn_event(sa::test::make_random_authn_options())) {
                success &= can_enqueue;
            } else {
                success &= !can_enqueue;
            }
        }
        return success;
    };
    vlog(logger.info, "Enqueue 200 records per shard");
    const bool success
      = audit_mgr
          .map_reduce0(std::move(enqueue_some), true, std::logical_and<>())
          .get();

    /// Since different messages related to application lifecycle may be
    /// enqueued during program execution, the test solely asserts that at any
    /// given time "if enough memory reservation does or does not exist, should
    /// the next enqueue work or not". Success is determined if the expectation
    /// matches the observed outcome, on all attempts, across all shards.
    EXPECT_TRUE(success);

    /// Verify auditing doesn't enqueue the non configured types
    EXPECT_TRUE(audit_mgr.local().enqueue_api_activity_event(
      sa::event_type::describe, ss::http::request(), "user", "my_svc"));
    EXPECT_TRUE(audit_mgr.local().enqueue_api_activity_event(
      sa::event_type::admin, ss::http::request(), "user", "my_svc"));
    EXPECT_TRUE(!audit_mgr.local().enqueue_authn_event(
      sa::test::make_random_authn_options()));

    /// Toggle the audit switch a few times
    for (auto i = 0; i < 5; ++i) {
        const bool val = i % 2 != 0;
        vlog(logger.info, "Toggling audit_enabled() to {}", val);
        local_cfg.get("audit_enabled").set_value(val);
        tests::cooperative_spin_wait_with_timeout(10s, [&audit_mgr, val] {
            return audit_mgr.local().is_effectively_enabled() == val;
        }).get();
    }
    EXPECT_TRUE(!config::shard_local_cfg().audit_enabled());

    /// Ensure with auditing disabled that there is no backpressure applied
    /// All enqueues should passthrough with success
    const size_t number_events
      = sa::test::pending_audit_events(audit_mgr.local()).get();
    const bool enqueued = audit_mgr
                            .map_reduce0(
                              [](sa::audit_log_manager& m) {
                                  return m.enqueue_authn_event(
                                    sa::test::make_random_authn_options());
                              },
                              true,
                              std::logical_and<>())
                            .get();

    EXPECT_TRUE(enqueued);
    EXPECT_EQ(
      sa::test::pending_audit_events(audit_mgr.local()).get(), number_events);

    /// Verify that eventually, all messages are drained
    local_cfg.get("audit_enabled").set_value(true);
    /// Lower the fiber loop interval from 60s (set high so that messages
    /// wouldn't be sent quicker then they could be enqueued) to a smaller
    /// interval so test can end quick as records are written and purged
    /// from each shards audit fibers queue.
    local_cfg.get("audit_queue_drain_interval_ms")
      .set_value(std::chrono::milliseconds(10));
    vlog(logger.info, "Waiting for all records to drain");
    tests::cooperative_spin_wait_with_timeout(30s, [&audit_mgr] {
        return sa::test::pending_audit_events(audit_mgr.local())
          .then([](size_t pending) { return pending == 0; });
    }).get();
}

INSTANTIATE_TEST_SUITE_P(
  audit_log_fixture_test, audit_log_fixture, testing::Bool());
