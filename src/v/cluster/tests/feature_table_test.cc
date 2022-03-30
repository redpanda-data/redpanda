/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/feature_table.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;
using namespace cluster;
using action_t = feature_update_action::action_t;

class setenv_helper {
public:
    setenv_helper() { setenv("__REDPANDA_TEST_FEATURES", "TRUE", 1); }
    ~setenv_helper() { unsetenv("__REDPANDA_TEST_FEATURES"); }
};

namespace cluster {
class feature_table_fixture {
public:
    feature_table_fixture() {}

    ~feature_table_fixture() { as.request_abort(); }

    setenv_helper setenv_hack;
    feature_table ft;
    ss::abort_source as;
    void set_active_version(cluster_version v) { ft.set_active_version(v); }
    void apply_action(const feature_update_action& fua) {
        ft.apply_action(fua);
    }

    /**
     * Combine constructing action and applying it to table, for
     * convenience.
     */
    void execute_action(
      std::string_view feature_name, feature_update_action::action_t a) {
        apply_action(feature_update_action{
          .feature_name = ss::sstring(feature_name), .action = a});
    }
};
} // namespace cluster

static constexpr std::string_view mock_feature{"__test_alpha"};

/**
 * Check that the test feature is not visible in the table if we
 * do not activate it with an environment variable.
 */
SEASTAR_THREAD_TEST_CASE(feature_table_test_hook_off) {
    feature_table ft;
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    BOOST_REQUIRE(!found);
}

SEASTAR_THREAD_TEST_CASE(feature_table_strings) {
    BOOST_REQUIRE_EQUAL(to_string_view(feature::test_alpha), mock_feature);
    BOOST_REQUIRE_EQUAL(
      to_string_view(feature::consumer_offsets), "consumer_offsets");
    BOOST_REQUIRE_EQUAL(
      to_string_view(feature::central_config), "central_config");
    BOOST_REQUIRE_EQUAL(
      to_string_view(feature::maintenance_mode), "maintenance_mode");
}

/**
 * Check that the test feature shows up when environment variable
 * is set (via the fixture, in this case)
 */
FIXTURE_TEST(feature_table_test_hook_on, feature_table_fixture) {
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    BOOST_REQUIRE(found);
}

/**
 * Exercise the lifecycle of a synthetic test feature
 */
FIXTURE_TEST(feature_table_basic, feature_table_fixture) {
    set_active_version(cluster_version{2});
    BOOST_REQUIRE_EQUAL(ft.get_active_version(), cluster_version{2});

    // Check we can get the list of all states and that our
    // test feature is in it.
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    BOOST_REQUIRE(found);

    BOOST_REQUIRE(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::unavailable);

    // The dummy test features requires version 2001.  The feature
    // should go available, but not any further: the feature table
    // relies on external stimulus to actually activate features.
    set_active_version(cluster_version{2001});

    BOOST_REQUIRE(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::available);
    BOOST_REQUIRE(!ft.is_active(feature::test_alpha));
    BOOST_REQUIRE(!ft.is_preparing(feature::test_alpha));

    auto f_active = ft.await_feature(feature::test_alpha, as);
    auto f_preparing = ft.await_feature_preparing(feature::test_alpha, as);
    BOOST_REQUIRE(!f_active.available());
    BOOST_REQUIRE(!f_preparing.available());

    // Disable the feature while it's only 'available'
    execute_action(mock_feature, action_t::deactivate);
    BOOST_REQUIRE(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::disabled_clean);

    // Construct an action to enable the feature
    execute_action(mock_feature, action_t::activate);
    BOOST_REQUIRE(ft.is_active(feature::test_alpha));

    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_active.available());
    BOOST_REQUIRE(f_preparing.available());
    f_active.get();
    f_preparing.get();

    // Waiting on already-active should be immediate
    auto f_active_immediate = ft.await_feature(feature::test_alpha, as);
    BOOST_REQUIRE(f_active_immediate.available());
    f_active_immediate.get();

    // Disable the feature after it has been activated
    execute_action(mock_feature, action_t::deactivate);
    BOOST_REQUIRE(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::disabled_active);

    // Waiting on active while disabled should block
    auto f_reactivated = ft.await_feature(feature::test_alpha, as);
    BOOST_REQUIRE(!f_reactivated.available());
    execute_action(mock_feature, action_t::activate);
    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_reactivated.available());
    f_reactivated.get();
}

/**
 * Exercise a feature that goes through a preparing stage
 */
FIXTURE_TEST(feature_table_preparing, feature_table_fixture) {
    set_active_version(cluster_version{1});
    BOOST_REQUIRE_EQUAL(ft.get_active_version(), cluster_version{1});
    BOOST_REQUIRE(
      ft.get_state(feature::consumer_offsets).get_state()
      == feature_state::state::unavailable);

    auto f_active = ft.await_feature(feature::consumer_offsets, as);
    auto f_preparing = ft.await_feature_preparing(
      feature::consumer_offsets, as);
    BOOST_REQUIRE(!f_preparing.available());

    // consumer_offsets is an auto-activating feature, as soon
    // as the cluster is upgraded it should go into preparing mode
    set_active_version(cluster_version{2});
    BOOST_REQUIRE_EQUAL(ft.get_active_version(), cluster_version{2});
    BOOST_REQUIRE(
      ft.get_state(feature::consumer_offsets).get_state()
      == feature_state::state::available);
    BOOST_REQUIRE(!ft.is_preparing(feature::consumer_offsets));
    BOOST_REQUIRE(!ft.is_active(feature::consumer_offsets));

    execute_action("consumer_offsets", action_t::activate);
    BOOST_REQUIRE(ft.is_preparing(feature::consumer_offsets));
    BOOST_REQUIRE(!ft.is_active(feature::consumer_offsets));

    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_preparing.available());
    f_preparing.get();

    // While in preparing mode, it is still valid to deactivate+activate
    // the feature.
    execute_action("consumer_offsets", action_t::deactivate);
    BOOST_REQUIRE(!ft.is_preparing(feature::consumer_offsets));
    BOOST_REQUIRE(
      ft.get_state(feature::consumer_offsets).get_state()
      == feature_state::state::disabled_preparing);

    // Re-activating the feature should revert it to preparing
    execute_action("consumer_offsets", action_t::activate);
    BOOST_REQUIRE(ft.is_preparing(feature::consumer_offsets));

    // Finally, completing preparing should make the feature active
    execute_action("consumer_offsets", action_t::complete_preparing);
    BOOST_REQUIRE(ft.is_active(feature::consumer_offsets));
    BOOST_REQUIRE(!ft.is_preparing(feature::consumer_offsets));

    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_active.available());
    f_active.get();
}
