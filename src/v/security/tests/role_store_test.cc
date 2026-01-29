/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/container/node_hash_set.h"
#include "random/generators.h"
#include "security/role.h"
#include "security/role_store.h"

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <vector>

namespace security {

namespace {

absl::node_hash_set<role_name>
make_role_name_set(const role_store::roles_range& range) {
    return {range.begin(), range.end()};
}

} // namespace

// This simple role store fixture prepopulates a role store with a single role
// that contains a single member.
struct simple_role_store_fixture {
    simple_role_store_fixture()
      : test_role_member{role_member_type::user, "test_user"}
      , test_role{{test_role_member}}
      , test_role_name("test_role") {
        BOOST_REQUIRE(store.put(test_role_name, test_role));
        BOOST_REQUIRE(store.size() == 1);
    }

    role_member test_role_member;
    role test_role;
    role_name test_role_name;
    role_store store;
};

// Verify that the role in the fixture was correctly inserted into the store.
BOOST_FIXTURE_TEST_CASE(role_store_put_role_test, simple_role_store_fixture) {
    BOOST_CHECK_MESSAGE(
      store.contains(test_role_name), "Role should exist in store");

    auto result = store.get(test_role_name);
    BOOST_CHECK_MESSAGE(
      result.has_value(), "Role should be retrievable from store");
    BOOST_CHECK_EQUAL(result.value(), test_role);

    // Verify that the member is associated with the role
    auto role_names = make_role_name_set(
      store.roles_for_member(test_role_member));
    BOOST_CHECK_MESSAGE(
      role_names.contains(test_role_name), "Member should belong to the role");
}

// Verify that a role can be removed from the store.
BOOST_FIXTURE_TEST_CASE(
  role_store_remove_role_test, simple_role_store_fixture) {
    bool removed = store.remove(test_role_name);
    BOOST_CHECK_MESSAGE(removed, "Failed to remove role from store");
    BOOST_CHECK_MESSAGE(
      !store.contains(test_role_name),
      "Role should no longer exist after removal");
    BOOST_CHECK_MESSAGE(
      !store.get(test_role_name).has_value(),
      "Getting removed role should return no value");
    BOOST_CHECK_MESSAGE(
      !store.remove(test_role_name),
      "Removing non-existent role should return false");

    auto role_names = make_role_name_set(
      store.roles_for_member(test_role_member));
    BOOST_CHECK_MESSAGE(
      !role_names.contains(test_role_name),
      "Member should no longer belong to the removed role");
}

// Verify that a role can be retrieved from the store.
BOOST_FIXTURE_TEST_CASE(role_store_get_role_test, simple_role_store_fixture) {
    BOOST_CHECK_MESSAGE(
      store.contains(test_role_name), "Role should exist in store");
    auto result = store.get(test_role_name);
    BOOST_CHECK_MESSAGE(
      result.has_value(), "Role should be retrievable from store");
    BOOST_CHECK_EQUAL(result.value(), test_role);
}

// Verify that a role can be updated in the store by first removing it and then
// inserting the updated role with the same name.
BOOST_FIXTURE_TEST_CASE(
  role_store_update_flow_test, simple_role_store_fixture) {
    const role_member new_member{role_member_type::user, "new_member"};
    const role updated_role{{new_member}};
    BOOST_REQUIRE_NE(test_role, updated_role);

    BOOST_REQUIRE(store.remove(test_role_name));
    BOOST_REQUIRE(store.put(test_role_name, updated_role));

    BOOST_CHECK_MESSAGE(
      store.get(test_role_name).has_value(),
      "Updated role should be retrievable from store");
    BOOST_CHECK_EQUAL(store.get(test_role_name).value(), updated_role);

    {
        auto role_names = make_role_name_set(
          store.roles_for_member(test_role_member));
        BOOST_CHECK_MESSAGE(
          !role_names.contains(test_role_name),
          "Old member should no longer be associated with the updated role");
    }
    {
        auto role_names = make_role_name_set(
          store.roles_for_member(new_member));
        BOOST_CHECK_MESSAGE(
          role_names.contains(test_role_name),
          "New member should be associated with the updated role");
    }
}

// Verify that the role store only inserts new roles and will not overwrite
// existing ones. A role must be explicitly removed from the store by name
// before a new role with the same name can be inserted.
BOOST_FIXTURE_TEST_CASE(
  role_store_no_overwrite_test, simple_role_store_fixture) {
    const role_member new_member{role_member_type::user, "new_member"};
    const role updated_role{{new_member}};
    BOOST_REQUIRE_NE(test_role, updated_role);

    BOOST_CHECK_MESSAGE(
      !store.put(test_role_name, updated_role),
      "Inserting a role with an existing name should fail");

    auto result = store.get(test_role_name);
    BOOST_CHECK_MESSAGE(
      result.has_value(),
      "Original role should still be retrievable from store");
    BOOST_CHECK_EQUAL(result.value(), test_role);

    {
        auto role_names = make_role_name_set(
          store.roles_for_member(test_role_member));
        BOOST_CHECK_MESSAGE(
          role_names.contains(test_role_name),
          "Old member should still be associated with the role");
    }
    {
        auto role_names = make_role_name_set(
          store.roles_for_member(new_member));
        BOOST_CHECK_MESSAGE(
          !role_names.contains(test_role_name),
          "New member should not be associated with the role");
    }
}

// Verify that empty roles (roles with no members) can be stored and retrieved.
BOOST_FIXTURE_TEST_CASE(role_store_empty_role_test, simple_role_store_fixture) {
    const role empty_role;
    const role_name empty_role_name("empty_role");
    BOOST_REQUIRE_MESSAGE(
      empty_role.members().empty(), "Role should have no members");

    bool inserted = store.put(empty_role_name, empty_role);
    BOOST_CHECK_MESSAGE(inserted, "Failed to put empty role in store");
    auto r = store.get(empty_role_name);
    BOOST_CHECK_MESSAGE(
      r.has_value(), "Empty role should be retrievable from store");
    BOOST_CHECK_EQUAL(r.value(), empty_role);
}

// Verify that the role store can be cleared of all roles.
BOOST_FIXTURE_TEST_CASE(role_store_clear_test, simple_role_store_fixture) {
    BOOST_REQUIRE_EQUAL(store.size(), 1);
    store.clear();
    BOOST_CHECK_EQUAL(store.size(), 0);
}

// Verify that all operations on an empty role_store behave correctly:
// queries return empty/nullopt, and removal operations return false.
BOOST_AUTO_TEST_CASE(role_store_empty_store) {
    role_store store;
    const role_name n{"foo"};
    const role_member m{role_member_type::user, "bar"};
    BOOST_REQUIRE_EQUAL(store.size(), 0);

    BOOST_CHECK(!store.get(n).has_value());
    BOOST_CHECK(store.roles_for_member(m).empty());
    BOOST_CHECK(store.range([](const auto&) { return true; }).empty());
    BOOST_CHECK(!store.remove(n));
    BOOST_CHECK(!store.contains(n));

    auto range = store.roles_for_member(m);
    BOOST_CHECK_MESSAGE(
      range.empty(),
      "No roles should be associated with any member in an empty store");
}

// Shared fixture for role_store range query tests.
// Prepopulates a role store with two roles (role0 and role1), each containing
// the same two members (member0 and member1).
struct role_store_range_fixture {
    role_store_range_fixture()
      : mem0{role_member_type::user, "member0"}
      , mem1{role_member_type::user, "member1"}
      , role0_name("role0")
      , role1_name("role1")
      , other_role_name("other_role") {
        const role role_with_both{{mem0, mem1}};

        BOOST_REQUIRE(store.put(role0_name, role_with_both));
        BOOST_REQUIRE(store.put(role1_name, role_with_both));
        BOOST_REQUIRE(store.put(other_role_name, role_with_both));
        BOOST_REQUIRE_EQUAL(store.size(), 3);
    }

    role_store store;
    role_member mem0;
    role_member mem1;
    role_name role0_name;
    role_name role1_name;
    role_name other_role_name;
};

// Verify that a range query with a predicate that always returns true
// return all roles in the store.
BOOST_FIXTURE_TEST_CASE(range_all, role_store_range_fixture) {
    auto pred = [](const auto&) { return true; };

    auto result = store.range(pred);
    absl::node_hash_set<role_name> got{result.begin(), result.end()};

    BOOST_CHECK_EQUAL(got.size(), 3);
    BOOST_CHECK(got.contains(role0_name));
    BOOST_CHECK(got.contains(role1_name));
    BOOST_CHECK(got.contains(other_role_name));
}

// Verify that a range query with a predicate that always returns false
// returns no roles.
BOOST_FIXTURE_TEST_CASE(range_none, role_store_range_fixture) {
    auto pred = [](const auto&) { return false; };

    auto result = store.range(pred);
    BOOST_CHECK_MESSAGE(
      result.empty(), "Expected no roles to match the false predicate");
}

// Verify that range queries can filter roles by name prefix.
BOOST_FIXTURE_TEST_CASE(
  range_filters_by_name_prefix, role_store_range_fixture) {
    auto pred = [](const auto& e) {
        return role_store::name_prefix_filter(e, "rol");
    };

    auto result = store.range(pred);
    absl::node_hash_set<role_name> got{result.begin(), result.end()};

    BOOST_CHECK_EQUAL(got.size(), 2);
    BOOST_CHECK(got.contains(role0_name));
    BOOST_CHECK(got.contains(role1_name));
    BOOST_CHECK_MESSAGE(
      !got.contains(other_role_name),
      "other_role should not match the 'rol' prefix");
}

// Verify that an empty name prefix matches all roles in the store.
BOOST_FIXTURE_TEST_CASE(
  range_with_empty_prefix_returns_all, role_store_range_fixture) {
    auto pred = [](const auto& e) {
        return role_store::name_prefix_filter(e, "");
    };

    auto result = store.range(pred);
    absl::node_hash_set<role_name> got{result.begin(), result.end()};

    BOOST_CHECK_EQUAL(got.size(), 3);
    BOOST_CHECK(got.contains(role0_name));
    BOOST_CHECK(got.contains(role1_name));
    BOOST_CHECK(got.contains(other_role_name));
}

// Verify that a name prefix that matches no roles returns no results.
BOOST_FIXTURE_TEST_CASE(
  range_with_different_prefix_returns_none, role_store_range_fixture) {
    auto pred = [](const auto& e) {
        return role_store::name_prefix_filter(e, "different_prefix");
    };

    auto result = store.range(pred);
    BOOST_CHECK_MESSAGE(
      result.empty(), "Expected no roles to match the different prefix filter");
}

// Verify that a range query can filter by member presence and returns all
// roles containing that member.
BOOST_FIXTURE_TEST_CASE(range_filters_by_member, role_store_range_fixture) {
    auto result = store.range(
      [this](const auto& e) { return role_store::has_member(e, mem0); });
    absl::node_hash_set<role_name> got{result.begin(), result.end()};

    BOOST_CHECK_EQUAL(got.size(), 3);
    BOOST_CHECK(got.contains(role0_name));
    BOOST_CHECK(got.contains(role1_name));
    BOOST_CHECK(got.contains(other_role_name));
}

// Verify that a range query filtering by a member that doesn't belong to any
// roles returns no results.
BOOST_FIXTURE_TEST_CASE(
  range_filters_by_nonexistent_member, role_store_range_fixture) {
    role_member nonexistent_member{role_member_type::user, "nonexistent"};

    auto result = store.range([&nonexistent_member](const auto& e) {
        return role_store::has_member(e, nonexistent_member);
    });
    BOOST_CHECK_MESSAGE(
      result.empty(), "Expected no roles to contain the nonexistent member");
}

// Verify that range queries can filter by both member presence and name prefix.
// All 3 roles contain mem1, but only other_role matches the name prefix filter.
BOOST_FIXTURE_TEST_CASE(
  range_filters_by_member_and_prefix, role_store_range_fixture) {
    auto pred = [this](const auto& e) {
        return role_store::has_member(e, mem1)
               && role_store::name_prefix_filter(e, "other");
    };

    auto result = store.range(pred);
    absl::node_hash_set<role_name> got{result.begin(), result.end()};

    BOOST_CHECK_EQUAL(got.size(), 1);
    BOOST_CHECK_MESSAGE(
      got.contains(other_role_name),
      "Expected to find other_role_name in the filtered results");
}

BOOST_AUTO_TEST_CASE(role_store_big_store) {
    role_member stable_member{role_member_type::user, "stable"};
    role_name stable_name{"stable"};

    constexpr size_t N_MEMBERS = 1024ul << 3u;
    constexpr size_t N_ROLES = 1024ul;

    const std::vector<role_member> members_data = [extra = stable_member]() {
        std::vector<role_member> mems{extra};
        mems.reserve(N_MEMBERS);
        std::ranges::for_each(boost::irange(0ul, N_MEMBERS), [&mems](auto) {
            mems.emplace_back(
              role_member_type::user,
              random_generators::gen_alphanum_string(32));
        });
        return mems;
    }();

    const std::vector<role_name> role_names_data = [extra = stable_name]() {
        std::vector<role_name> roles{extra};
        roles.reserve(N_ROLES);
        std::ranges::for_each(boost::irange(0ul, N_ROLES), [&roles](auto) {
            roles.emplace_back(random_generators::gen_alphanum_string(32));
        });
        return roles;
    }();

    const role_store store =
      [&stable_member, &members_data, &role_names_data]() {
          role_store store;
          for (auto n : role_names_data) {
              role::container_type role_mems;
              for (const auto& m : members_data) {
                  if (random_generators::get_int(0, 1) || m == stable_member) {
                      role_mems.insert(m);
                  }
              }
              store.put(std::move(n), role_mems);
          }
          return store;
      }();

    auto r = store.get(stable_name);
    BOOST_REQUIRE(r.has_value());
    BOOST_CHECK(r.value().members().contains(stable_member));

    auto rls = store.roles_for_member(stable_member);
    absl::node_hash_set<std::string_view> membership{rls.begin(), rls.end()};
    BOOST_CHECK(!membership.empty());
    BOOST_CHECK(membership.contains(stable_name()));
}

} // namespace security
