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

#include "cluster/feature_update_action.h"
#include "features/feature_table.h"
#include "features/feature_table_snapshot.h"
#include "security/license.h"
#include "test_utils/fixture.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;
using namespace cluster;
using namespace features;
using action_t = feature_update_action::action_t;

class setenv_helper {
public:
    setenv_helper() { setenv("__REDPANDA_TEST_FEATURES", "TRUE", 1); }
    ~setenv_helper() { unsetenv("__REDPANDA_TEST_FEATURES"); }
};

namespace features {
class feature_table_fixture {
public:
    feature_table_fixture() {}

    ~feature_table_fixture() { as.request_abort(); }

    setenv_helper setenv_hack;
    feature_table ft;
    ss::abort_source as;
    void set_active_version(cluster_version v) { ft.set_active_version(v); }
    void bootstrap_active_version(cluster_version v) {
        ft.bootstrap_active_version(v);
    }
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
} // namespace features

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
      to_string_view(feature::rpc_v2_by_default), "rpc_v2_by_default");
    BOOST_REQUIRE_EQUAL(to_string_view(feature::kafka_gssapi), "kafka_gssapi");
    BOOST_REQUIRE_EQUAL(
      to_string_view(feature::node_isolation), "node_isolation");
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
      ft.get_state(feature::cloud_retention).get_state()
      == feature_state::state::unavailable);

    auto f_active = ft.await_feature(feature::cloud_retention, as);
    auto f_preparing = ft.await_feature_preparing(feature::cloud_retention, as);
    BOOST_REQUIRE(!f_preparing.available());

    // cloud_retention is an auto-activating feature, as soon
    // as the cluster is upgraded it should go into preparing mode
    set_active_version(cluster_version{7});
    BOOST_REQUIRE_EQUAL(ft.get_active_version(), cluster_version{7});
    BOOST_REQUIRE(
      ft.get_state(feature::cloud_retention).get_state()
      == feature_state::state::available);
    BOOST_REQUIRE(!ft.is_preparing(feature::cloud_retention));
    BOOST_REQUIRE(!ft.is_active(feature::cloud_retention));

    execute_action("cloud_retention", action_t::activate);
    BOOST_REQUIRE(ft.is_preparing(feature::cloud_retention));
    BOOST_REQUIRE(!ft.is_active(feature::cloud_retention));

    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_preparing.available());
    f_preparing.get();

    // While in preparing mode, it is still valid to deactivate+activate
    // the feature.
    execute_action("cloud_retention", action_t::deactivate);
    BOOST_REQUIRE(!ft.is_preparing(feature::cloud_retention));
    BOOST_REQUIRE(
      ft.get_state(feature::cloud_retention).get_state()
      == feature_state::state::disabled_preparing);

    // Re-activating the feature should revert it to preparing
    execute_action("cloud_retention", action_t::activate);
    BOOST_REQUIRE(ft.is_preparing(feature::cloud_retention));

    // Finally, completing preparing should make the feature active
    execute_action("cloud_retention", action_t::complete_preparing);
    BOOST_REQUIRE(ft.is_active(feature::cloud_retention));
    BOOST_REQUIRE(!ft.is_preparing(feature::cloud_retention));

    ss::sleep(10ms).get();
    BOOST_REQUIRE(f_active.available());
    f_active.get();
}

FIXTURE_TEST(feature_table_then, feature_table_fixture) {
    set_active_version(cluster_version{1});
    bool activated = false;

    // Wait for the feature and then activate. We should fire off its 'then'
    // function.
    auto wait_cloud_then_set = ft.await_feature_then(
      feature::cloud_retention, [&] { activated = true; });
    BOOST_REQUIRE(!activated);
    set_active_version(cluster_version{7});
    execute_action("cloud_retention", action_t::activate);
    execute_action("cloud_retention", action_t::complete_preparing);
    BOOST_REQUIRE_EQUAL(ft.get_active_version(), cluster_version{7});
    ss::sleep(10ms).get();
    BOOST_REQUIRE(wait_cloud_then_set.available());
    wait_cloud_then_set.get();
    BOOST_REQUIRE(activated);

    // Now try again but abort. 'then' shouldn't complete.
    activated = false;
    auto wait_alpha_then_set = ft.await_feature_then(
      feature::test_alpha, [&] { activated = true; });
    BOOST_REQUIRE(!activated);
    ft.abort_for_tests();
    ss::sleep(10ms).get();
    BOOST_REQUIRE(wait_alpha_then_set.available());
    BOOST_REQUIRE(!activated);
}

FIXTURE_TEST(feature_uniqueness, feature_table_fixture) {
    for (const auto& schema : feature_schema) {
        feature current_feature = schema.bits;
        for (const auto& other : feature_schema) {
            BOOST_REQUIRE(
              (static_cast<uint64_t>(other.bits)
               & static_cast<uint64_t>(current_feature))
                == 0
              || other.bits == current_feature);
        }
    }
}

/**
 * Validate that the bootstrap method not only updates the cluster version,
 * but also activates elegible features.
 */
FIXTURE_TEST(feature_table_bootstrap, feature_table_fixture) {
    bootstrap_active_version(cluster_version{2001});

    // A non-auto-activating feature should remain in available state:
    // explicit_only features always require explicit activation, even
    // if the cluster was bootstrapped in a version where the feature
    // is available.
    BOOST_REQUIRE(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::available);
    BOOST_REQUIRE(!ft.is_active(feature::test_alpha));

    // An auto-activating feature fast-forwards to the active state
    BOOST_REQUIRE(
      ft.get_state(feature::test_bravo).get_state()
      == feature_state::state::active);
    BOOST_REQUIRE(ft.is_active(feature::test_bravo));

    // A feature that has a preparing state skips it when bootstrapping
    // straight into the version where the feature is available.
    BOOST_REQUIRE(
      ft.get_state(feature::cloud_retention).spec.prepare_rule
      == feature_spec::prepare_policy::requires_migration);
    BOOST_REQUIRE(
      ft.get_state(feature::cloud_retention).get_state()
      == feature_state::state::active);
    BOOST_REQUIRE(ft.is_active(feature::cloud_retention));
}

// Test that applying an old snapshot doesn't disable features that we
// auto-enabled when fast-forwarding to the earliest_logical_version.
FIXTURE_TEST(feature_table_old_snapshot, feature_table_fixture) {
    bootstrap_active_version(
      features::feature_table::get_earliest_logical_version());

    features::feature_table_snapshot snapshot;
    snapshot.version = features::feature_table::get_earliest_logical_version();
    snapshot.states = {
      features::feature_state_snapshot{
        .name = "serde_raft_0",
        .state = feature_state::state::available,
      },
      features::feature_state_snapshot{
        .name = "__test_alpha",
        .state = feature_state::state::active,
      },
    };

    snapshot.apply(ft);

    // Fast-forwarded feature should still be active.
    BOOST_CHECK(
      ft.get_state(feature::serde_raft_0).get_state()
      == feature_state::state::active);
    // A feature with explicit available_policy should be activated by the
    // snapshot.
    BOOST_CHECK(
      ft.get_state(feature::test_alpha).get_state()
      == feature_state::state::active);
}

SEASTAR_THREAD_TEST_CASE(feature_table_probe_expiry_metric_test) {
    using ft = features::feature_table;
    const char* sample_valid_license = std::getenv("REDPANDA_SAMPLE_LICENSE");
    if (sample_valid_license == nullptr) {
        const char* is_on_ci = std::getenv("CI");
        BOOST_TEST_REQUIRE(
          !is_on_ci,
          "Expecting the REDPANDA_SAMPLE_LICENSE env var in the CI "
          "enviornment");
        return;
    }
    const ss::sstring license_str{sample_valid_license};
    const auto license = security::make_license(license_str);

    auto expiry = security::license::clock::time_point{4813252273s};

    BOOST_CHECK_EQUAL(ft::calculate_expiry_metric(license, expiry - 1s), 1);
    BOOST_CHECK_EQUAL(ft::calculate_expiry_metric(license, expiry), 0);
    BOOST_CHECK_EQUAL(ft::calculate_expiry_metric(license, expiry + 1s), 0);
    BOOST_CHECK_EQUAL(ft::calculate_expiry_metric(std::nullopt), -1);
}

SEASTAR_THREAD_TEST_CASE(is_major_version_upgrade_test) {
    BOOST_CHECK(!is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_1_1)));
    BOOST_CHECK(!is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_1_5)));
    BOOST_CHECK(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_2_1)));
    BOOST_CHECK(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_5),
      to_cluster_version(release_version::v22_2_1)));
    BOOST_CHECK(!is_major_version_upgrade(
      to_cluster_version(release_version::v22_3_1),
      to_cluster_version(release_version::v22_1_1)));
    BOOST_CHECK(is_major_version_upgrade(
      cluster::cluster_version{2},
      to_cluster_version(release_version::v22_3_1)));
    BOOST_CHECK(is_major_version_upgrade(
      cluster::cluster_version{-1},
      to_cluster_version(release_version::v22_3_1)));
    BOOST_CHECK(is_major_version_upgrade(
      to_cluster_version(release_version::v24_3_1),
      cluster::cluster_version{15}));
}
