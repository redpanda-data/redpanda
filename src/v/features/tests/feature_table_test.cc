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
#include "model/timestamp.h"
#include "security/license.h"
#include "security/tests/license_utils.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

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

class feature_table_fixture : public testing::Test {
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
        apply_action(
          feature_update_action{
            .feature_name = ss::sstring(feature_name), .action = a});
    }
};

class FeatureTableTest : public testing::Test {};

} // namespace features

static constexpr std::string_view mock_feature{"__test_alpha"};

/**
 * Check that the test feature is not visible in the table if we
 * do not activate it with an environment variable.
 */
TEST_F(FeatureTableTest, feature_table_test_hook_off) {
    feature_table ft;
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    ASSERT_FALSE(found);
}

TEST_F(FeatureTableTest, feature_table_strings) {
    ASSERT_EQ(to_string_view(feature::test_alpha), mock_feature);
    ASSERT_EQ(to_string_view(feature::audit_logging), "audit_logging");
}

/**
 * Check that the test feature shows up when environment variable
 * is set (via the fixture, in this case)
 */
TEST_F(feature_table_fixture, feature_table_test_hook_on) {
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
}

/**
 * Exercise the lifecycle of a synthetic test feature
 */
TEST_F(feature_table_fixture, feature_table_basic) {
    set_active_version(cluster_version{2});
    ASSERT_EQ(ft.get_active_version(), cluster_version{2});

    // Check we can get the list of all states and that our
    // test feature is in it.
    bool found{false};
    for (const auto& s : ft.get_feature_state()) {
        if (s.spec.name == mock_feature) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);

    ASSERT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::unavailable);

    // The dummy test features requires version TEST_VERSION. The feature
    // should go available, but not any further: the feature table
    // relies on external stimulus to actually activate features.
    set_active_version(TEST_VERSION);

    ASSERT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::available);
    ASSERT_FALSE(ft.is_active(feature::test_alpha));
    ASSERT_FALSE(ft.is_preparing(feature::test_alpha));

    auto f_active = ft.await_feature(feature::test_alpha, as);
    auto f_preparing = ft.await_feature_preparing(feature::test_alpha, as);
    ASSERT_FALSE(f_active.available());
    ASSERT_FALSE(f_preparing.available());

    // Disable the feature while it's only 'available'
    execute_action(mock_feature, action_t::deactivate);
    ASSERT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::disabled_clean);

    // Construct an action to enable the feature
    execute_action(mock_feature, action_t::activate);
    ASSERT_TRUE(ft.is_active(feature::test_alpha));

    ss::sleep(10ms).get();
    ASSERT_TRUE(f_active.available());
    ASSERT_TRUE(f_preparing.available());
    f_active.get();
    f_preparing.get();

    // Waiting on already-active should be immediate
    auto f_active_immediate = ft.await_feature(feature::test_alpha, as);
    ASSERT_TRUE(f_active_immediate.available());
    f_active_immediate.get();

    // Disable the feature after it has been activated
    execute_action(mock_feature, action_t::deactivate);
    ASSERT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::disabled_active);

    // Waiting on active while disabled should block
    auto f_reactivated = ft.await_feature(feature::test_alpha, as);
    ASSERT_FALSE(f_reactivated.available());
    execute_action(mock_feature, action_t::activate);
    ss::sleep(10ms).get();
    ASSERT_TRUE(f_reactivated.available());
    f_reactivated.get();
}

/**
 * Exercise a feature that goes through a preparing stage
 */
TEST_F(feature_table_fixture, feature_table_preparing) {
    set_active_version(cluster_version{1});
    ASSERT_EQ(ft.get_active_version(), cluster_version{1});
    ASSERT_EQ(
      ft.get_state(feature::cloud_retention).get_state(),
      feature_state::state::unavailable);

    auto f_active = ft.await_feature(feature::cloud_retention, as);
    auto f_preparing = ft.await_feature_preparing(feature::cloud_retention, as);
    ASSERT_FALSE(f_preparing.available());

    // cloud_retention is an auto-activating feature, as soon
    // as the cluster is upgraded it should go into preparing mode
    set_active_version(cluster_version{7});
    ASSERT_EQ(ft.get_active_version(), cluster_version{7});
    ASSERT_EQ(
      ft.get_state(feature::cloud_retention).get_state(),
      feature_state::state::available);
    ASSERT_FALSE(ft.is_preparing(feature::cloud_retention));
    ASSERT_FALSE(ft.is_active(feature::cloud_retention));

    execute_action("cloud_retention", action_t::activate);
    ASSERT_TRUE(ft.is_preparing(feature::cloud_retention));
    ASSERT_FALSE(ft.is_active(feature::cloud_retention));

    ss::sleep(10ms).get();
    ASSERT_TRUE(f_preparing.available());
    f_preparing.get();

    // While in preparing mode, it is still valid to deactivate+activate
    // the feature.
    execute_action("cloud_retention", action_t::deactivate);
    ASSERT_FALSE(ft.is_preparing(feature::cloud_retention));
    ASSERT_EQ(
      ft.get_state(feature::cloud_retention).get_state(),
      feature_state::state::disabled_preparing);

    // Re-activating the feature should revert it to preparing
    execute_action("cloud_retention", action_t::activate);
    ASSERT_TRUE(ft.is_preparing(feature::cloud_retention));

    // Finally, completing preparing should make the feature active
    execute_action("cloud_retention", action_t::complete_preparing);
    ASSERT_TRUE(ft.is_active(feature::cloud_retention));
    ASSERT_FALSE(ft.is_preparing(feature::cloud_retention));

    ss::sleep(10ms).get();
    ASSERT_TRUE(f_active.available());
    f_active.get();
}

TEST_F(feature_table_fixture, feature_table_then) {
    set_active_version(cluster_version{1});
    bool activated = false;

    // Wait for the feature and then activate. We should fire off its 'then'
    // function.
    auto wait_cloud_then_set = ft.await_feature_then(
      feature::cloud_retention, [&] { activated = true; });
    ASSERT_FALSE(activated);
    set_active_version(cluster_version{7});
    execute_action("cloud_retention", action_t::activate);
    execute_action("cloud_retention", action_t::complete_preparing);
    ASSERT_EQ(ft.get_active_version(), cluster_version{7});
    ss::sleep(10ms).get();
    ASSERT_TRUE(wait_cloud_then_set.available());
    wait_cloud_then_set.get();
    ASSERT_TRUE(activated);

    // Now try again but abort. 'then' shouldn't complete.
    activated = false;
    auto wait_alpha_then_set = ft.await_feature_then(
      feature::test_alpha, [&] { activated = true; });
    ASSERT_FALSE(activated);
    ft.abort_for_tests();
    ss::sleep(10ms).get();
    ASSERT_TRUE(wait_alpha_then_set.available());
    ASSERT_FALSE(activated);
}

TEST_F(feature_table_fixture, feature_uniqueness) {
    for (const auto& schema : feature_schema) {
        feature current_feature = schema.bits;
        for (const auto& other : feature_schema) {
            ASSERT_TRUE(
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
TEST_F(feature_table_fixture, feature_table_bootstrap) {
    bootstrap_active_version(TEST_VERSION);

    // A non-auto-activating feature should remain in available state:
    // explicit_only features always require explicit activation, even
    // if the cluster was bootstrapped in a version where the feature
    // is available.
    ASSERT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::available);
    ASSERT_FALSE(ft.is_active(feature::test_alpha));

    // An auto-activating feature fast-forwards to the active state
    ASSERT_EQ(
      ft.get_state(feature::test_bravo).get_state(),
      feature_state::state::active);
    ASSERT_TRUE(ft.is_active(feature::test_bravo));

    // A feature that has a preparing state skips it when bootstrapping
    // straight into the version where the feature is available.
    ASSERT_EQ(
      ft.get_state(feature::cloud_retention).spec.prepare_rule,
      feature_spec::prepare_policy::requires_migration);
    ASSERT_EQ(
      ft.get_state(feature::cloud_retention).get_state(),
      feature_state::state::active);
    ASSERT_TRUE(ft.is_active(feature::cloud_retention));
}

// Test that applying an old snapshot doesn't disable features that we
// auto-enabled when fast-forwarding to the earliest_logical_version.
TEST_F(feature_table_fixture, feature_table_old_snapshot) {
    bootstrap_active_version(
      features::feature_table::get_earliest_logical_version());

    features::feature_table_snapshot snapshot;
    snapshot.version = features::feature_table::get_earliest_logical_version();
    snapshot.states = {
      features::feature_state_snapshot{
        .name = "audit_logging",
        .state = feature_state::state::available,
      },
      features::feature_state_snapshot{
        .name = "__test_alpha",
        .state = feature_state::state::active,
      },
    };

    snapshot.apply(ft);

    // Fast-forwarded feature should still be active.
    EXPECT_EQ(
      ft.get_state(feature::audit_logging).get_state(),
      feature_state::state::active);
    // A feature with explicit available_policy should be activated by the
    // snapshot.
    EXPECT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::active);
}

// Test that applying an old snapshot disables features that we only enabled in
// this version.
TEST_F(feature_table_fixture, feature_table_old_snapshot_missing) {
    bootstrap_active_version(TEST_VERSION);

    features::feature_table_snapshot snapshot;
    snapshot.version = cluster::cluster_version{ft.get_active_version() - 1};
    snapshot.states = {};
    snapshot.apply(ft);

    // A feature with explicit available_policy should be activated by the
    // snapshot.
    EXPECT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::unavailable);
}

// Test that applying a snapshot of the same version with a missing feature
// enables it, as we assume it was retired in the next version.
TEST_F(feature_table_fixture, feature_table_new_snapshot_missing) {
    bootstrap_active_version(TEST_VERSION);

    features::feature_table_snapshot snapshot;
    snapshot.version = cluster::cluster_version{ft.get_active_version()};
    snapshot.states = {};
    snapshot.apply(ft);

    // A feature with explicit available_policy should be activated by the
    // snapshot.
    EXPECT_EQ(
      ft.get_state(feature::test_alpha).get_state(),
      feature_state::state::active);
}

TEST_F(feature_table_fixture, feature_table_trial_license_test) {
    const auto opt_license = security::testing::get_test_license();
    if (!opt_license) {
        GTEST_SKIP() << security::testing::skip_no_license_msg;
        return;
    }
    auto& license = *opt_license;

    auto expired_license = license;
    expired_license.expiry = 0s;

    EXPECT_EQ(ft.get_license().has_value(), false);
    EXPECT_EQ(ft.should_sanction(), false);

    ft.set_builtin_trial_license(model::timestamp::now());
    EXPECT_EQ(ft.get_license().has_value(), true);
    EXPECT_EQ(ft.get_license()->is_expired(), false);
    EXPECT_EQ(ft.should_sanction(), false);

    ft.set_license(expired_license);
    EXPECT_EQ(ft.get_license().has_value(), true);
    EXPECT_EQ(ft.get_license()->is_expired(), true);
    EXPECT_EQ(ft.should_sanction(), true);

    ft.set_license(license);
    EXPECT_EQ(ft.get_license().has_value(), true);
    EXPECT_EQ(ft.get_license()->is_expired(), false);
    EXPECT_EQ(ft.should_sanction(), false);

    ft.revoke_license();
    EXPECT_EQ(ft.get_license().has_value(), false);
    EXPECT_EQ(ft.should_sanction(), true);
}

TEST_F(FeatureTableTest, feature_table_probe_expiry_metric_test) {
    using ft = features::feature_table;

    const auto opt_license = security::testing::get_test_license();
    if (!opt_license) {
        GTEST_SKIP() << security::testing::skip_no_license_msg;
        return;
    }
    auto& license = *opt_license;

    auto expiry = security::license::clock::time_point{4813252273s};

    EXPECT_EQ(ft::calculate_expiry_metric(license, expiry - 1s), 1);
    EXPECT_EQ(ft::calculate_expiry_metric(license, expiry), 0);
    EXPECT_EQ(ft::calculate_expiry_metric(license, expiry + 1s), 0);
    EXPECT_EQ(ft::calculate_expiry_metric(std::nullopt), -1);
}

TEST_F(FeatureTableTest, is_major_version_upgrade_test) {
    EXPECT_FALSE(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_1_1)));
    EXPECT_FALSE(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_1_5)));
    EXPECT_TRUE(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_1),
      to_cluster_version(release_version::v22_2_1)));
    EXPECT_TRUE(is_major_version_upgrade(
      to_cluster_version(release_version::v22_1_5),
      to_cluster_version(release_version::v22_2_1)));
    EXPECT_FALSE(is_major_version_upgrade(
      to_cluster_version(release_version::v22_3_1),
      to_cluster_version(release_version::v22_1_1)));
    EXPECT_TRUE(is_major_version_upgrade(
      cluster::cluster_version{2},
      to_cluster_version(release_version::v22_3_1)));
    EXPECT_TRUE(is_major_version_upgrade(
      cluster::cluster_version{-1},
      to_cluster_version(release_version::v22_3_1)));
    EXPECT_TRUE(is_major_version_upgrade(
      to_cluster_version(release_version::MAX),
      cluster::cluster_version{
        static_cast<std::underlying_type_t<release_version>>(
          release_version::MAX)
        + 1}));
}
