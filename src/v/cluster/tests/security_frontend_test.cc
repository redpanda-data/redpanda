/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/commands.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/security_frontend.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "container/zip.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "security/role.h"
#include "security/role_store.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <absl/algorithm/container.h>
#include <boost/range/irange.hpp>

#include <algorithm>

namespace {
using role_name = security::role_name;
using role_member = security::role_member;
using role_member_type = security::role_member_type;
using role = security::role;
using role_store = security::role_store;
using security_frontend = cluster::security_frontend;

constexpr size_t N_ROLES = 3;
constexpr size_t N_MEMBERS_PER_ROLE = 10;

static std::vector<role_name> make_random_names(size_t N) {
    std::vector<role_name> names;
    std::generate_n(std::back_inserter(names), N, [] {
        return role_name{random_generators::gen_alphanum_string(32)};
    });
    return names;
}

static std::vector<role> make_random_roles(size_t N, size_t N_MEM) {
    std::vector<role> roles;
    std::generate_n(std::back_inserter(roles), N, [N = N_MEM] {
        role::container_type mems;
        std::generate_n(std::inserter(mems, mems.begin()), N, [] {
            return role_member{
              role_member_type::user,
              random_generators::gen_alphanum_string(32)};
        });
        return role{std::move(mems)};
    });
    return roles;
}

static const std::vector<role_name> orig_names = make_random_names(N_ROLES);
static const std::vector<role> orig_roles = make_random_roles(
  N_ROLES, N_MEMBERS_PER_ROLE);
static const std::vector<role> updated_roles = make_random_roles(
  N_ROLES, N_MEMBERS_PER_ROLE);

} // namespace

FIXTURE_TEST(test_role_management, cluster_test_fixture) {
    model::node_id n_1(0);
    model::node_id n_2(1);

    BOOST_REQUIRE_EQUAL(orig_names.size(), N_ROLES);
    BOOST_REQUIRE_EQUAL(orig_roles.size(), N_ROLES);

    auto app_0 = create_node_application(n_1);
    create_node_application(n_2);

    wait_for_controller_leadership(n_1).get();

    // Wait for cluster to reach stable state
    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return get_local_cache(model::node_id(0)).node_count() == 2
               && get_local_cache(model::node_id(1)).node_count() == 2;
    }).get();

    using security_frontend_request
      = std::function<ss::future<std::error_code>(security_frontend & app)>;

    auto try_command =
      [app_0](security_frontend_request&& fn) -> cluster::errc {
        absl::flat_hash_set<std::error_code> break_codes{
          cluster::errc::success,
          cluster::errc::feature_disabled,
          cluster::errc::role_exists,
          cluster::errc::role_does_not_exist};
        break_codes.insert(cluster::errc::success);
        bool success = false;
        cluster::errc result{cluster::errc::success};
        while (!success) {
            auto ec
              = fn(app_0->controller->get_security_frontend().local()).get();
            success = break_codes.contains(ec);
            result = static_cast<cluster::errc>(ec.value());
        }
        return result;
    };

    auto create_role = [&try_command](const role_name& n, const role& r) {
        return try_command([&n, &r](security_frontend& fe) {
            return fe.create_role(n, r, model::timeout_clock::now() + 10s);
        });
    };

    auto update_role = [&try_command](const role_name& n, const role& r) {
        return try_command([&n, &r](security_frontend& fe) {
            return fe.update_role(n, r, model::timeout_clock::now() + 10s);
        });
    };

    auto delete_role = [&try_command](const role_name& n) {
        return try_command([&n](security_frontend& fe) {
            return fe.delete_role(n, model::timeout_clock::now() + 10s);
        });
    };

    // Activate features and await RBAC
    app_0->controller->get_feature_table().local().testing_activate_all();
    app_0->controller->get_feature_table()
      .local()
      .await_feature(features::feature::role_based_access_control)
      .get();

    const auto& store = app_0->controller->get_role_store().local();

    // We can create roles...
    for (const auto& it : container::zip(orig_names, orig_roles)) {
        BOOST_CHECK_EQUAL(
          create_role(get<0>(it), get<1>(it)), cluster::errc::success);
    }

    for (const auto& expect : container::zip(orig_names, orig_roles)) {
        auto got = store.get(get<0>(expect));
        BOOST_REQUIRE(got.has_value());
        BOOST_CHECK_EQUAL(got.value(), get<1>(expect));
    }

    // ...And change their contents...
    for (const auto& it : container::zip(orig_names, updated_roles)) {
        BOOST_CHECK_EQUAL(
          update_role(get<0>(it), get<1>(it)), cluster::errc::success);
    }

    for (const auto& expect : container::zip(orig_names, updated_roles)) {
        auto got = store.get(get<0>(expect));
        BOOST_REQUIRE(got.has_value());
        BOOST_CHECK_EQUAL(got.value(), get<1>(expect));
    }

    // ...And remove them from the system...
    for (const auto& name : orig_names) {
        BOOST_CHECK_EQUAL(delete_role(name), cluster::errc::success);
    }

    for (const auto& name : orig_names) {
        BOOST_CHECK(!store.get(name).has_value());
    }

    // Store should be empty at this point

    const auto& some_name = orig_names[0];
    const auto& some_role = orig_roles.back();

    // Updating a nonexistent role should fail and have no effect
    BOOST_CHECK_EQUAL(
      update_role(some_name, some_role), cluster::errc::role_does_not_exist);
    BOOST_CHECK(!store.get(some_name).has_value());

    // Same for deleting a nonexistent role
    BOOST_CHECK_EQUAL(
      delete_role(some_name), cluster::errc::role_does_not_exist);

    // Create role should fail if the role already exists
    BOOST_CHECK_EQUAL(
      create_role(some_name, some_role), cluster::errc::success);
    BOOST_CHECK_EQUAL(
      create_role(some_name, some_role), cluster::errc::role_exists);

    // Disable RBAC feature and confirm errors

    {
        bool success = false;
        while (!success) {
            auto ec
              = app_0->controller->get_feature_manager()
                  .local()
                  .write_action(cluster::feature_update_action{
                    .feature_name = ss::sstring("role_based_access_control"),
                    .action
                    = cluster::feature_update_action::action_t::deactivate})
                  .get();
            success = ec == cluster::errc::success;
        }
    }

    BOOST_CHECK_EQUAL(
      create_role(role_name{"foo"}, role{}), cluster::errc::feature_disabled);
    BOOST_CHECK_EQUAL(
      update_role(role_name{"foo"}, role{}), cluster::errc::feature_disabled);
    BOOST_CHECK_EQUAL(
      delete_role(role_name{"foo"}), cluster::errc::feature_disabled);
}
