// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "security/request_auth.h"

#include <seastar/http/exception.hh>

#include <boost/test/unit_test.hpp>

namespace {

request_auth_result make_authenticated_user(
  request_auth_result::superuser is_superuser
  = request_auth_result::superuser::no) {
    return request_auth_result{
      security::credential_user{"user"},
      security::credential_password{"password"},
      "SCRAM-SHA-256",
      is_superuser,
      {}};
}

request_auth_result
make_anonymous(request_auth_result::authenticated is_authenticated) {
    return request_auth_result{
      is_authenticated,
      request_auth_result::superuser::no,
      request_auth_result::auth_required::yes};
}

} // namespace

// A handler can fail before reaching its authorization check, in which case
// the result is destroyed unchecked. In a coroutine frame teardown that
// happens inside a noexcept task entry point, so the destructor must never
// throw; enforcement of "the check must run" lives with the caller instead.
BOOST_AUTO_TEST_CASE(unchecked_destroy_does_not_throw) {
    BOOST_CHECK_NO_THROW({ auto result = make_authenticated_user(); });
}

BOOST_AUTO_TEST_CASE(is_checked_reflects_authorization_checks) {
    {
        auto result = make_authenticated_user();
        BOOST_CHECK(!result.is_checked());
        result.pass();
        BOOST_CHECK(result.is_checked());
    }
    {
        auto result = make_authenticated_user();
        BOOST_CHECK_NO_THROW(result.require_authenticated());
        BOOST_CHECK(result.is_checked());
    }
    {
        auto result = make_authenticated_user(
          request_auth_result::superuser::yes);
        BOOST_CHECK_NO_THROW(result.require_superuser());
        BOOST_CHECK(result.is_checked());
    }
}

// A denied check throws, but still counts as performed: callers that treat
// "handler failed" as an acceptable outcome rely on denial being an
// exception, never an unchecked success.
BOOST_AUTO_TEST_CASE(denied_checks_throw_and_still_mark_checked) {
    {
        auto result = make_anonymous(request_auth_result::authenticated::no);
        BOOST_CHECK_THROW(
          result.require_authenticated(), ss::httpd::base_exception);
        BOOST_CHECK(result.is_checked());
    }
    {
        auto result = make_authenticated_user();
        BOOST_CHECK_THROW(
          result.require_superuser(), ss::httpd::base_exception);
        BOOST_CHECK(result.is_checked());
    }
}

// The moved-from husk counts as checked so it neither logs nor enforces,
// while the moved-to object carries the real state.
BOOST_AUTO_TEST_CASE(move_transfers_checked_state_to_destination) {
    auto source = make_authenticated_user();
    auto destination = std::move(source);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK(source.is_checked());
    BOOST_CHECK(!destination.is_checked());
    destination.pass();
}
