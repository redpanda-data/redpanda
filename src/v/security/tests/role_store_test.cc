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

#include <boost/algorithm/string.hpp>
#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

#include <vector>

namespace security {

BOOST_AUTO_TEST_CASE(role_test) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::user, "member1"};

    std::vector<role_member> mems{mem0, mem1, mem0};
    role::container_type members{mems.begin(), mems.end()};
    const role rol{std::move(members)};

    for (const auto& m : mems) {
        BOOST_CHECK(rol.members().contains(m));
        BOOST_CHECK_EQUAL(rol.members().count(m), 1);
    }

    auto rep = fmt::format("{}", rol);
    BOOST_CHECK(rep.find("{User}:{member0}") != std::string::npos);
    BOOST_CHECK(rep.find("{User}:{member1}") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(role_store_test) {
    const role_member mem0{role_member_type::user, "m0"};
    const role_member mem1{role_member_type::user, "m1"};

    std::vector<role_member> mems{mem0, mem1};
    const role role0{{mem0, mem1}};
    const role role1{{mem1}};

    auto role0_copy = role0;
    auto role1_copy = role1;

    BOOST_CHECK_NE(role0, role1);
    BOOST_CHECK_NE(role0_copy, role1_copy);

    const role_name copied("copied");
    const role_name moved("moved");

    role_store store;
    BOOST_CHECK(store.put(copied, role0));
    store.put(moved, std::move(role0_copy));

    BOOST_REQUIRE(store.get(copied).has_value());
    BOOST_CHECK_EQUAL(store.get(copied).value(), role0);

    BOOST_REQUIRE(store.get(moved).has_value());
    BOOST_CHECK_EQUAL(store.get(moved).value(), role0);

    // update roles
    BOOST_CHECK(store.remove(copied));
    BOOST_CHECK(store.remove(moved));
    BOOST_CHECK(store.put(copied, role1));
    store.put(moved, std::move(role1_copy));

    BOOST_REQUIRE(store.get(copied).has_value());
    BOOST_CHECK_EQUAL(store.get(copied).value(), role1);

    BOOST_REQUIRE(store.get(moved).has_value());
    BOOST_CHECK_EQUAL(store.get(moved).value(), role1);

    // remove a role
    BOOST_CHECK(store.contains(copied));
    BOOST_CHECK(store.remove(copied));
    BOOST_CHECK(!store.remove(copied));
    BOOST_CHECK(!store.contains(copied));
    BOOST_REQUIRE(!store.get(copied).has_value());
    BOOST_CHECK(store.contains(moved));
    store.clear();
    BOOST_CHECK(!store.contains(moved));
}

BOOST_AUTO_TEST_CASE(role_store_no_update) {
    const role_member m{role_member_type::user, "m0"};
    const role r{{m}};
    const role_name n{"r"};

    role_store store;

    {
        BOOST_CHECK(store.put(n, r));
        auto o = store.get(n);
        BOOST_REQUIRE(o.has_value());
        BOOST_CHECK(o.value() == r);
    }

    {
        BOOST_CHECK(!store.put(n, role{}));
        auto o = store.get(n);
        BOOST_REQUIRE(o.has_value());
        BOOST_CHECK(o.value() == r);
    }

    BOOST_CHECK(store.remove(n));

    {
        BOOST_CHECK(store.put(n, role{}));
        auto o = store.get(n);
        BOOST_REQUIRE(o.has_value());
        BOOST_CHECK(o.value() == role{});
    }
}

BOOST_AUTO_TEST_CASE(role_store_empty_role_test) {
    role_store store;
    const role r0;
    const role_name r0_name("r0");
    BOOST_CHECK(r0.members().empty());
    BOOST_CHECK(store.put(r0_name, r0));
    BOOST_CHECK(!store.put(r0_name, r0));
    auto r = store.get(r0_name);
    BOOST_REQUIRE(r.has_value());
    BOOST_CHECK_EQUAL(r.value(), r0);
}

BOOST_AUTO_TEST_CASE(role_store_empty_store) {
    role_store store;
    const role_name n{"foo"};
    const role_member m{role_member_type::user, "bar"};

    BOOST_CHECK(!store.get(n).has_value());
    BOOST_CHECK(store.roles_for_member(m).empty());
    BOOST_CHECK(store
                  .range([&m](const auto& e) {
                      return role_store::has_member(e, m)
                             || role_store::name_prefix_filter(e, "f");
                  })
                  .empty());
    BOOST_CHECK(!store.remove(n));
    BOOST_CHECK(!store.contains(n));
}

BOOST_AUTO_TEST_CASE(role_store_range_test) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::user, "member1"};

    std::vector<role_member> mems{mem0, mem1};
    const role role0{{mem0, mem1}};
    const role role1{{mem0, mem1}};

    BOOST_CHECK_EQUAL(role0, role1);

    const role_name r0_name("role0");
    const role_name r1_name("role1");

    role_store store;
    store.put(r0_name, role0);
    store.put(r1_name, role1);

    auto prefix_pred = [](std::string_view pfx) {
        return [pfx](const auto& e) {
            return role_store::name_prefix_filter(e, pfx);
        };
    };

    auto member_prefix_pred =
      [](const role_member& mem, std::string_view filter) {
          return [&mem, filter](const auto& e) {
              return role_store::has_member(e, mem)
                     && role_store::name_prefix_filter(e, filter);
          };
      };

    auto check_range_query =
      [](auto& rng, absl::node_hash_set<role_name> expected) -> bool {
        absl::node_hash_set<role_name> got{rng.begin(), rng.end()};
        return got == expected;
    };

    {
        auto res = store.range(prefix_pred("rol"));
        BOOST_CHECK(check_range_query(res, {r0_name, r1_name}));
    }

    {
        auto res = store.range(prefix_pred(""));
        BOOST_CHECK(check_range_query(res, {r0_name, r1_name}));
    }

    {
        // both roles contain mem1 but we also filter on the name
        auto res = store.range(member_prefix_pred(mem1, r0_name()));
        BOOST_CHECK(check_range_query(res, {r0_name}));
    }
}

BOOST_AUTO_TEST_CASE(role_store_roles_for_member) {
    const role_member mem0{role_member_type::user, "m0"};
    const role_member mem1{role_member_type::user, "m1"};
    const role_member mem2{role_member_type::user, "m2"};

    const role role0{{mem0, mem1, mem2}};
    const role role1{{mem1, mem2}};
    const role role2{{mem2}};

    const role_name r0_name("role0");
    const role_name r1_name("role1");
    const role_name r2_name("role2");

    BOOST_CHECK_NE(role0, role1);
    BOOST_CHECK_NE(role0, role2);
    BOOST_CHECK_NE(role1, role2);

    role_store store;
    BOOST_CHECK(store.put(r0_name, role0));
    BOOST_CHECK(store.put(r1_name, role1));
    BOOST_CHECK(store.put(r2_name, role2));

    auto check_membership =
      [](const auto& rng, absl::node_hash_set<role_name> expected) {
          absl::node_hash_set<role_name> got{rng.begin(), rng.end()};
          return got == expected;
      };

    {
        auto itr = store.roles_for_member(mem0);
        BOOST_CHECK(check_membership(itr, {r0_name}));
    }

    {
        auto itr = store.roles_for_member(mem1);
        BOOST_CHECK(check_membership(itr, {r0_name, r1_name}));
    }

    {
        auto itr = store.roles_for_member(mem2);
        BOOST_CHECK(check_membership(itr, {r0_name, r1_name, r2_name}));
    }

    store.remove(r0_name);

    {
        auto itr = store.roles_for_member(mem0);
        BOOST_CHECK(check_membership(itr, {}));
    }

    {
        auto itr = store.roles_for_member(mem1);
        BOOST_CHECK(check_membership(itr, {r1_name}));
    }

    {
        auto itr = store.roles_for_member(mem2);
        BOOST_CHECK(check_membership(itr, {r1_name, r2_name}));
    }

    store.clear();
    for (const auto& m : {mem0, mem1, mem2}) {
        auto itr = store.roles_for_member(m);
        BOOST_CHECK(itr.empty());
    };
}

BOOST_AUTO_TEST_CASE(role_store_big_store) {
    role_member stable_member{role_member_type::user, "stable"};
    role_name stable_name{"stable"};

    constexpr size_t N_MEMBERS = 1024ul << 3u;
    constexpr size_t N_ROLES = 1024ul;

    const std::vector<role_member> members_data = [extra = stable_member]() {
        std::vector<role_member> mems{extra};
        mems.reserve(N_MEMBERS);
        absl::c_for_each(boost::irange(0ul, N_MEMBERS), [&mems](auto) {
            mems.emplace_back(
              role_member_type::user,
              random_generators::gen_alphanum_string(32));
        });
        return mems;
    }();

    const std::vector<role_name> role_names_data = [extra = stable_name]() {
        std::vector<role_name> roles{extra};
        roles.reserve(N_ROLES);
        absl::c_for_each(boost::irange(0ul, N_ROLES), [&roles](auto) {
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
              store.put(std::move(n), std::move(role_mems));
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
