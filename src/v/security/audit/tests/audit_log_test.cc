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
#include "kafka/client/test/fixture.h"
#include "redpanda/tests/fixture.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "test_utils/fixture.h"

#include <seastar/util/log.hh>

namespace sa = security::audit;

sa::user make_random_user() {
    return sa::user{
      .domain = random_generators::gen_alphanum_string(10),
      .name = random_generators::gen_alphanum_string(10),
      .type_id = random_generators::random_choice(
        {sa::user::type::user,
         sa::user::type::admin,
         sa::user::type::system,
         sa::user::type::other,
         sa::user::type::unknown}),
      .uid = random_generators::gen_alphanum_string(10),
    };
}

sa::authentication_event_options make_random_authn_options() {
    return sa::authentication_event_options{
      .server_addr = net::unresolved_address(
        "1.2.3.4", random_generators::get_int(9999)),
      .client_addr = net::unresolved_address(
        "1.2.3.4", random_generators::get_int(9999)),
      .is_cleartext = sa::authentication::used_cleartext(
        random_generators::get_int(0, 1)),
      .user = make_random_user(),
      .error_reason = std::make_optional(
        random_generators::gen_alphanum_string(10))};
}

ss::future<size_t> pending_audit_events(sa::audit_log_manager& m) {
    return m.container().map_reduce0(
      [](const sa::audit_log_manager& m) { return m.pending_events(); },
      size_t(0),
      std::plus<>());
}

ss::future<> set_auditing_config_options(size_t event_size) {
    return ss::smp::invoke_on_all([event_size] {
        std::vector<ss::sstring> enabled_types{"authenticate"};
        config::shard_local_cfg().get("audit_enabled").set_value(false);
        config::shard_local_cfg()
          .get("audit_log_replication_factor")
          .set_value(std::make_optional(int16_t(1)));
        config::shard_local_cfg()
          .get("audit_queue_max_buffer_size_per_shard")
          .set_value(event_size * 100);
        config::shard_local_cfg()
          .get("audit_queue_drain_interval_ms")
          .set_value(std::chrono::milliseconds(60000));
        config::shard_local_cfg()
          .get("audit_enabled_event_types")
          .set_value(enabled_types);
    });
}

FIXTURE_TEST(test_audit_init_phase, kafka_client_fixture) {
    /// Knowing the size of one event allows to set a predetermined maximum
    /// shard allowance for auditing that way backpressure is applied when
    /// anticipated
    const size_t event_size = sa::authentication::construct(
                                make_random_authn_options())
                                .estimated_size();
    info("Single event size bytes: {}", event_size);

    ss::global_logger_registry().set_logger_level(
      "auditing", ss::log_level::trace);

    set_auditing_config_options(event_size).get();
    enable_sasl_and_restart("username");

    wait_for_controller_leadership().get();
    auto& audit_mgr = app.audit_mgr;

    /// with auditing disabled, calls to enqueue should be no-ops
    const auto n_events = pending_audit_events(audit_mgr.local()).get();
    audit_mgr
      .invoke_on_all([](sa::audit_log_manager& m) {
          for (auto i = 0; i < 20; ++i) {
              BOOST_ASSERT(m.enqueue_authn_event(make_random_authn_options()));
          }
      })
      .get();

    BOOST_CHECK_EQUAL(pending_audit_events(audit_mgr.local()).get(), n_events);

    /// with auditing enabled, the system should block when the threshold of
    /// audit_queue_max_buffer_size_per_shard has been reached
    ss::smp::invoke_on_all([] {
        config::shard_local_cfg().get("audit_enabled").set_value(true);
    }).get();

    /// With the switch enabled the audit topic should be created
    wait_for_topics(
      {cluster::topic_result(model::topic_namespace(
        model::kafka_namespace, model::kafka_audit_logging_topic))})
      .get();

    /// Wait until the run loops are available, otherwise enqueuing events will
    /// pass through
    info("Waiting until the audit fibers are up");
    tests::cooperative_spin_wait_with_timeout(10s, [&audit_mgr] {
        return audit_mgr.local().is_effectively_enabled();
    }).get();

    /// Verify auditing can enqueue up until the max configured, and further
    /// calls to enqueue return false, signifying action did not occur.
    auto enqueue_some = [event_size](sa::audit_log_manager& m) {
        bool success = true;
        for (auto i = 0; i < 200; ++i) {
            const bool can_enqueue = m.avaiable_reservation() >= event_size;
            if (m.enqueue_authn_event(make_random_authn_options())) {
                success &= can_enqueue;
            } else {
                success &= !can_enqueue;
            }
        }
        return success;
    };
    info("Enqueue 200 records per shard");
    const bool success
      = audit_mgr
          .map_reduce0(std::move(enqueue_some), true, std::logical_and<>())
          .get();

    /// Since different messages related to application lifecycle may be
    /// enqueued during program execution, the test solely asserts that at any
    /// given time "if enough memory reservation does or does not exist, should
    /// the next enqueue work or not". Success is determined if the expectation
    /// matches the observed outcome, on all attempts, across all shards.
    BOOST_CHECK(success);

    /// Verify auditing doesn't enqueue the non configured types
    BOOST_CHECK(audit_mgr.local().enqueue_api_activity_event(
      sa::event_type::describe, ss::http::request(), "user", "my_svc"));
    BOOST_CHECK(audit_mgr.local().enqueue_api_activity_event(
      sa::event_type::admin, ss::http::request(), "user", "my_svc"));
    BOOST_CHECK(
      !audit_mgr.local().enqueue_authn_event(make_random_authn_options()));

    /// Toggle the audit switch a few times
    for (auto i = 0; i < 5; ++i) {
        const bool val = i % 2 != 0;
        info("Toggling audit_enabled() to {}", val);
        ss::smp::invoke_on_all([val] {
            config::shard_local_cfg().get("audit_enabled").set_value(val);
        }).get();
        tests::cooperative_spin_wait_with_timeout(10s, [&audit_mgr, val] {
            return audit_mgr.local().is_effectively_enabled() == val;
        }).get();
    }
    BOOST_CHECK(!config::shard_local_cfg().audit_enabled());

    /// Ensure with auditing disabled that there is no backpressure applied
    /// All enqueues should passthrough with success
    const size_t number_events = pending_audit_events(audit_mgr.local()).get();
    const bool enqueued = audit_mgr
                            .map_reduce0(
                              [](sa::audit_log_manager& m) {
                                  return m.enqueue_authn_event(
                                    make_random_authn_options());
                              },
                              true,
                              std::logical_and<>())
                            .get();

    BOOST_CHECK(enqueued);
    BOOST_CHECK_EQUAL(
      pending_audit_events(audit_mgr.local()).get(), number_events);

    /// Verify that eventually, all messages are drained
    ss::smp::invoke_on_all([] {
        config::shard_local_cfg().get("audit_enabled").set_value(true);
        /// Lower the fiber loop interval from 60s (set high so that messages
        /// wouldn't be sent quicker then they could be enqueued) to a smaller
        /// interval so test can end quick as records are written and purged
        /// from each shards audit fibers queue.
        config::shard_local_cfg()
          .get("audit_queue_drain_interval_ms")
          .set_value(std::chrono::milliseconds(10));
    }).get();
    info("Waiting for all records to drain");
    tests::cooperative_spin_wait_with_timeout(30s, [&audit_mgr] {
        return pending_audit_events(audit_mgr.local()).then([](size_t pending) {
            return pending == 0;
        });
    }).get();

    BOOST_TEST_MESSAGE("End of test");
}
