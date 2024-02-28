/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "security/role.h"
#include "security/role_store.h"

#include <boost/algorithm/string.hpp>
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
        BOOST_REQUIRE(rol.members().contains(m));
        BOOST_REQUIRE_EQUAL(rol.members().count(m), 1);
    }

    auto rep = fmt::format("{}", rol);
    BOOST_REQUIRE(rep.find("{user}:{member0}") != std::string::npos);
    BOOST_REQUIRE(rep.find("{user}:{member1}") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(role_store_test) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::user, "member1"};

    std::vector<role_member> mems{mem0, mem1};
    const role role0{{mems.begin(), mems.end()}};
    const role role1{{mems.begin(), mems.begin() + 1}};

    auto role0_copy = role0;
    auto role1_copy = role1;

    BOOST_REQUIRE_NE(role0, role1);
    BOOST_REQUIRE_NE(role0_copy, role1_copy);

    const role_name copied("copied");
    const role_name moved("moved");

    role_store store;
    store.put(copied, role0);
    store.put(moved, std::move(role0_copy));

    BOOST_REQUIRE(store.get<role>(copied).has_value());
    BOOST_REQUIRE_EQUAL(store.get<role>(copied).value(), role0);

    BOOST_REQUIRE(store.get<role>(moved).has_value());
    BOOST_REQUIRE_EQUAL(store.get<role>(moved).value(), role0);

    // update roles
    store.put(copied, role1);
    store.put(moved, std::move(role1_copy));

    BOOST_REQUIRE(store.get<role>(copied).has_value());
    BOOST_REQUIRE_EQUAL(store.get<role>(copied).value(), role1);

    BOOST_REQUIRE(store.get<role>(moved).has_value());
    BOOST_REQUIRE_EQUAL(store.get<role>(moved).value(), role1);

    // remove a role
    BOOST_REQUIRE(store.contains(copied));
    BOOST_REQUIRE(store.remove(copied));
    BOOST_REQUIRE(!store.remove(copied));
    BOOST_REQUIRE(!store.contains(copied));
    BOOST_REQUIRE(!store.get<role>(copied).has_value());
    BOOST_REQUIRE(store.contains(moved));
    store.clear();
    BOOST_REQUIRE(!store.contains(moved));
}

BOOST_AUTO_TEST_CASE(role_store_range_test) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::user, "member1"};

    std::vector<role_member> mems{mem0, mem1};
    const role role0{{mems.begin(), mems.end()}};
    const role role1{{mems.begin(), mems.begin() + 1}};

    BOOST_REQUIRE_NE(role0, role1);

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

    {
        auto res = store.range(prefix_pred("rol"));
        BOOST_REQUIRE_EQUAL(std::distance(res.begin(), res.end()), 2);
    }

    {
        auto res = store.range(prefix_pred(""));
        BOOST_REQUIRE_EQUAL(std::distance(res.begin(), res.end()), 2);
    }

    auto member_pred = [](const role_member& mem) {
        return [&mem](const auto& e) { return role_store::has_member(e, mem); };
    };

    {
        // only one role contains mem1
        auto res = store.range(member_pred(mem1));
        BOOST_REQUIRE_EQUAL(std::distance(res.begin(), res.end()), 1);
    }
}

BOOST_AUTO_TEST_CASE(role_store_member_query_test) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::user, "member1"};
    const role_member mem2{role_member_type::user, "member2"};

    std::vector<role_member> mems{mem0, mem1, mem2};
    const role role0{{mems.begin(), mems.end()}};
    const role role1{{mems.begin() + 1, mems.end()}};
    const role role2{{mems.begin() + 2, mems.end()}};

    const role_name r0_name("role0");
    const role_name r1_name("role1");
    const role_name r2_name("role2");

    BOOST_REQUIRE_NE(role0, role1);
    BOOST_REQUIRE_NE(role0, role2);
    BOOST_REQUIRE_NE(role1, role2);

    role_store store;
    store.put(r0_name, role0);
    store.put(r1_name, role1);
    store.put(r2_name, role2);

    {
        auto mem0_roles = store.get_member_roles(mem0);
        BOOST_REQUIRE(mem0_roles.has_value());
        BOOST_REQUIRE(mem0_roles.value()->contains(r0_name));
        BOOST_REQUIRE(!mem0_roles.value()->contains(r1_name));
        BOOST_REQUIRE(!mem0_roles.value()->contains(r2_name));
    }

    {
        auto mem1_roles = store.get_member_roles(mem1);
        BOOST_REQUIRE(mem1_roles.has_value());
        BOOST_REQUIRE(mem1_roles.value()->contains(r0_name));
        BOOST_REQUIRE(mem1_roles.value()->contains(r1_name));
        BOOST_REQUIRE(!mem1_roles.value()->contains(r2_name));
    }

    {
        auto mem2_roles = store.get_member_roles(mem2);
        BOOST_REQUIRE(mem2_roles.has_value());
        BOOST_REQUIRE(mem2_roles.value()->contains(r0_name));
        BOOST_REQUIRE(mem2_roles.value()->contains(r1_name));
        BOOST_REQUIRE(mem2_roles.value()->contains(r2_name));
    }

    store.remove(r0_name);

    {
        auto mem0_roles = store.get_member_roles(mem0);
        BOOST_REQUIRE(mem0_roles.has_value());
        BOOST_REQUIRE(!mem0_roles.value()->contains(r0_name));
        BOOST_REQUIRE(!mem0_roles.value()->contains(r1_name));
        BOOST_REQUIRE(!mem0_roles.value()->contains(r2_name));
    }

    {
        auto mem1_roles = store.get_member_roles(mem1);
        BOOST_REQUIRE(mem1_roles.has_value());
        BOOST_REQUIRE(!mem1_roles.value()->contains(r0_name));
        BOOST_REQUIRE(mem1_roles.value()->contains(r1_name));
        BOOST_REQUIRE(!mem1_roles.value()->contains(r2_name));
    }

    {
        auto mem2_roles = store.get_member_roles(mem2);
        BOOST_REQUIRE(mem2_roles.has_value());
        BOOST_REQUIRE(!mem2_roles.value()->contains(r0_name));
        BOOST_REQUIRE(mem2_roles.value()->contains(r1_name));
        BOOST_REQUIRE(mem2_roles.value()->contains(r2_name));
    }

    store.clear();
    absl::c_for_each(mems, [&store](const auto& m) {
        BOOST_REQUIRE(!store.get_member_roles(m).has_value());
    });
}

} // namespace security
