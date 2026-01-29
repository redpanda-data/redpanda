/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/role.h"

#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace security {

BOOST_AUTO_TEST_CASE(role_members_are_deduplicated) {
    const role_member mem{role_member_type::user, "member0"};

    // Add same member three times
    const role rol{{mem, mem, mem}};

    // Should only contain member once
    BOOST_CHECK_EQUAL(rol.members().size(), 1);
    BOOST_CHECK(rol.members().contains(mem));
}

BOOST_AUTO_TEST_CASE(role_members_user_format) {
    const role_member mem0{role_member_type::user, "member0"};

    auto rep = fmt::format("{}", mem0);
    BOOST_CHECK_EQUAL(rep, "{User}:{member0}");
}

BOOST_AUTO_TEST_CASE(role_members_group_format) {
    const role_member mem0{role_member_type::group, "group0"};

    auto rep = fmt::format("{}", mem0);
    BOOST_CHECK_EQUAL(rep, "{Group}:{group0}");
}

BOOST_AUTO_TEST_CASE(role_format_includes_all_members) {
    const role_member mem0{role_member_type::user, "member0"};
    const role_member mem1{role_member_type::group, "group0"};

    const role rol{{mem0, mem1}};

    auto rep = fmt::format("{}", rol);
    BOOST_CHECK(rep.find("{User}:{member0}") != std::string::npos);
    BOOST_CHECK(rep.find("{Group}:{group0}") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(role_can_be_empty) {
    const role rol{};

    BOOST_CHECK_EQUAL(rol.members().size(), 0);
    BOOST_CHECK(rol.members().empty());
}

BOOST_AUTO_TEST_CASE(role_to_principal_creates_role_principal) {
    auto principal = role::to_principal("test_role");

    BOOST_CHECK_EQUAL(principal.type(), principal_type::role);
    BOOST_CHECK_EQUAL(principal.name_view(), "test_role");
}

BOOST_AUTO_TEST_CASE(role_to_principal_view_creates_role_principal_view) {
    auto principal_view = role::to_principal_view("test_role");

    BOOST_CHECK_EQUAL(principal_view.type(), principal_type::role);
    BOOST_CHECK_EQUAL(principal_view.name_view(), "test_role");
}

// Test that role_member can be created from a user principal type
BOOST_AUTO_TEST_CASE(role_member_from_user_principal) {
    acl_principal user_principal{principal_type::user, "test_user"};
    auto member = role_member::from_principal(user_principal);
    BOOST_CHECK_EQUAL(member.type(), role_member_type::user);
    BOOST_CHECK_EQUAL(member.name(), "test_user");
}

// Test that role_member_view can be created from a user principal type
BOOST_AUTO_TEST_CASE(role_member_view_from_user_principal) {
    acl_principal user_principal{principal_type::user, "test_user"};
    auto member_view = role_member_view::from_principal(user_principal);
    BOOST_CHECK_EQUAL(member_view.type(), role_member_type::user);
    BOOST_CHECK_EQUAL(member_view.name(), "test_user");
}

// Test that role_member can be created from a group principal type
BOOST_AUTO_TEST_CASE(role_member_from_group_principal) {
    acl_principal group_principal{principal_type::group, "test_group"};
    auto member = role_member::from_principal(group_principal);
    BOOST_CHECK_EQUAL(member.type(), role_member_type::group);
    BOOST_CHECK_EQUAL(member.name(), "test_group");
}

// Test that role_member_view can be created from a group principal type
BOOST_AUTO_TEST_CASE(role_member_view_from_group_principal) {
    acl_principal group_principal{principal_type::group, "test_group"};
    auto member_view = role_member_view::from_principal(group_principal);
    BOOST_CHECK_EQUAL(member_view.type(), role_member_type::group);
    BOOST_CHECK_EQUAL(member_view.name(), "test_group");
}

} // namespace security
