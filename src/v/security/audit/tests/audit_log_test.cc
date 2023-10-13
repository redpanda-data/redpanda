// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "kafka/types.h"
#include "redpanda/tests/fixture.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "test_utils/fixture.h"

namespace sa = security::audit;

bool enqueue_random_audit_event(sa::audit_log_manager& m, sa::event_type type) {
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

    return m.enqueue_audit_event<sa::application_lifecycle>(
      type,
      sa::application_lifecycle::activity_id(random_generators::get_int(0, 4)),
      make_random_product(),
      sa::severity_id(random_generators::get_int(0, 6)),
      now);
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

    wait_for_controller_leadership().get0();
    auto& audit_mgr = app.audit_mgr;

    /// with auditing disabled, calls to enqueue should be no-ops
    audit_mgr
      .invoke_on_all([](sa::audit_log_manager& m) {
          for (auto i = 0; i < 20; ++i) {
              BOOST_ASSERT(
                enqueue_random_audit_event(m, sa::event_type::management));
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
                               bool enqueued = enqueue_random_audit_event(
                                 m, sa::event_type::management);
                               if (i >= 5) {
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
    BOOST_CHECK(enqueue_random_audit_event(
      audit_mgr.local(), sa::event_type::authenticate));
    BOOST_CHECK(
      enqueue_random_audit_event(audit_mgr.local(), sa::event_type::describe));
    BOOST_CHECK(!enqueue_random_audit_event(
      audit_mgr.local(), sa::event_type::management));

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
                                  return enqueue_random_audit_event(
                                    m, sa::event_type::management);
                              },
                              true,
                              std::logical_and<>())
                            .get0();

    BOOST_CHECK(enqueued);
    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get0(),
      size_t(5 * ss::smp::count));
}
