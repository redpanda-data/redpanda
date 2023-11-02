/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/types.h"
#include "kafka/types.h"
#include "redpanda/tests/fixture.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "test_utils/fixture.h"

#include <seastar/util/log.hh>

namespace sa = security::audit;

sa::application_lifecycle make_random_audit_event() {
    auto make_random_product = []() {
        return sa::product{
          .name = random_generators::gen_alphanum_string(10),
          .vendor_name = random_generators::gen_alphanum_string(10),
          .version = random_generators::gen_alphanum_string(10)};
    };

    auto now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};

    return {
      sa::application_lifecycle::activity_id(random_generators::get_int(0, 4)),
      make_random_product(),
      sa::severity_id(random_generators::get_int(0, 6)),
      now};
}

ss::future<size_t> pending_audit_events(sa::audit_log_manager& m) {
    return m.container().map_reduce0(
      [](const sa::audit_log_manager& m) { return m.pending_events(); },
      size_t(0),
      std::plus<>());
}

FIXTURE_TEST(test_audit_init_phase, redpanda_thread_fixture) {
    /// Initialize auditing configurations
    ss::smp::invoke_on_all([] {
        std::vector<ss::sstring> enabled_types{"management", "consume"};
        config::shard_local_cfg().get("audit_enabled").set_value(false);
        config::shard_local_cfg()
          .get("audit_log_replication_factor")
          .set_value(int16_t(1));
        config::shard_local_cfg()
          .get("audit_max_queue_elements_per_shard")
          .set_value(size_t(5));
        config::shard_local_cfg()
          .get("audit_queue_drain_interval_ms")
          .set_value(std::chrono::milliseconds(60000));
        config::shard_local_cfg()
          .get("audit_enabled_event_types")
          .set_value(enabled_types);
    }).get();

    ss::global_logger_registry().set_logger_level(
      "auditing", ss::log_level::trace);

    wait_for_controller_leadership().get0();
    auto& audit_mgr = app.audit_mgr;

    /// with auditing disabled, calls to enqueue should be no-ops
    audit_mgr
      .invoke_on_all([](sa::audit_log_manager& m) {
          for (auto i = 0; i < 20; ++i) {
              BOOST_ASSERT(m.enqueue_audit_event(
                sa::event_type::management, make_random_audit_event()));
          }
      })
      .get0();

    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get0(), size_t(0));

    /// with auditing enabled, the system should block when the threshold of
    /// 5 records has been surpassed
    ss::smp::invoke_on_all([] {
        config::shard_local_cfg().get("audit_enabled").set_value(true);
    }).get();

    /// With the switch enabled the audit topic should be created
    wait_for_topics(
      {cluster::topic_result(model::topic_namespace(
        model::kafka_namespace, model::kafka_audit_logging_topic))})
      .get();

    /// Verify auditing can enqueue up until the max configured, and further
    /// calls to enqueue return false, signifying action did not occur.
    bool success = audit_mgr
                     .map_reduce0(
                       [](sa::audit_log_manager& m) {
                           bool success = true;
                           for (auto i = 0; i < 20; ++i) {
                               /// Should always return true, auditing is
                               /// disabled
                               bool enqueued = m.enqueue_audit_event(
                                 sa::event_type::management,
                                 make_random_audit_event());
                               // There is already a message in there from
                               // the topic creation from the audit log manager
                               if (i >= 4) {
                                   /// Assert that when the max is reached data
                                   /// cannot be entered into the system
                                   enqueued = !enqueued;
                               }
                               success = success && enqueued;
                           }
                           return success;
                       },
                       true,
                       std::logical_and<>())
                     .get0();

    BOOST_CHECK(success);
    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get0(),
      size_t(5 * ss::smp::count));

    /// Verify auditing doesn't enqueue the non configured types
    BOOST_CHECK(audit_mgr.local().enqueue_audit_event(
      sa::event_type::authenticate, make_random_audit_event()));
    BOOST_CHECK(audit_mgr.local().enqueue_audit_event(
      sa::event_type::describe, make_random_audit_event()));
    BOOST_CHECK(!audit_mgr.local().enqueue_audit_event(
      sa::event_type::management, make_random_audit_event()));

    /// Toggle the audit switch a few times
    for (auto i = 0; i < 5; ++i) {
        const bool val = i % 2 != 0;
        ss::smp::invoke_on_all([val] {
            config::shard_local_cfg().get("audit_enabled").set_value(val);
        }).get0();
        audit_mgr
          .invoke_on(
            0,
            [val](sa::audit_log_manager& m) {
                return tests::cooperative_spin_wait_with_timeout(
                  10s, [&m, val] { return m.is_client_enabled() == val; });
            })
          .get0();
    }
    /// Ensure in the off state even with buffers filled, no data is lost
    BOOST_CHECK(!config::shard_local_cfg().audit_enabled());
    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get0(),
      size_t(5 * ss::smp::count));

    const bool enqueued = audit_mgr
                            .map_reduce0(
                              [](sa::audit_log_manager& m) {
                                  return m.enqueue_audit_event(
                                    sa::event_type::management,
                                    make_random_audit_event());
                              },
                              true,
                              std::logical_and<>())
                            .get0();

    BOOST_CHECK(enqueued);
    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get0(),
      size_t(5 * ss::smp::count));

    BOOST_TEST_MESSAGE("End of test");
}
