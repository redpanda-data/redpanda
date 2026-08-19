/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/auth.h"
#include "security/request_auth.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <memory>
#include <stdexcept>

namespace ppsr = pandaproxy::schema_registry;

namespace {

ss::lw_shared_ptr<request_auth_result> make_deferred_auth_result() {
    return ss::make_lw_shared<request_auth_result>(
      security::credential_user{"user"},
      security::credential_password{"password"},
      "SCRAM-SHA-256",
      request_auth_result::superuser::no);
}

ppsr::server::reply_t make_reply() {
    ppsr::server::reply_t rp;
    rp.rep = std::make_unique<ss::http::reply>();
    return rp;
}

ss::future<ppsr::server::reply_t> ready_reply(ppsr::server::reply_t rp) {
    return ss::make_ready_future<ppsr::server::reply_t>(std::move(rp));
}

ss::future<ppsr::server::reply_t> failed_reply() {
    return ss::make_exception_future<ppsr::server::reply_t>(
      std::runtime_error{"handler failed"});
}

} // namespace

SEASTAR_THREAD_TEST_CASE(checked_reply_passes_through) {
    auto auth_result = make_deferred_auth_result();
    auth_result->pass();
    auto rp = make_reply();
    const auto* marker = rp.rep.get();

    auto result = ppsr::enforce_deferred_authz(
                    ready_reply(std::move(rp)), auth_result, "get_config")
                    .get();

    BOOST_CHECK_EQUAL(result.rep.get(), marker);
}

// A handler that produces a reply without having performed its deferred
// authorization check must not have that reply returned to the client.
SEASTAR_THREAD_TEST_CASE(unchecked_reply_is_replaced_with_a_500) {
    auto auth_result = make_deferred_auth_result();

    auto fut = ppsr::enforce_deferred_authz(
      ready_reply(make_reply()), auth_result, "get_config");

    BOOST_CHECK_THROW(fut.get(), ss::httpd::server_error_exception);
    BOOST_CHECK(auth_result->is_checked());
}

// A handler that fails before its deferred authorization check is an
// acceptable outcome: the client receives the original error, which must
// not be masked by enforcement.
SEASTAR_THREAD_TEST_CASE(failed_handler_keeps_its_original_exception) {
    auto auth_result = make_deferred_auth_result();

    auto fut = ppsr::enforce_deferred_authz(
      failed_reply(), auth_result, "get_config");

    BOOST_CHECK_THROW(fut.get(), std::runtime_error);
    BOOST_CHECK(auth_result->is_checked());
}

// No authentication result means authorization is not deferred for this
// request (e.g. it is disabled); there is nothing to enforce.
SEASTAR_THREAD_TEST_CASE(without_auth_result_reply_passes_through) {
    auto rp = make_reply();
    const auto* marker = rp.rep.get();

    auto result = ppsr::enforce_deferred_authz(
                    ready_reply(std::move(rp)), nullptr, "get_config")
                    .get();

    BOOST_CHECK_EQUAL(result.rep.get(), marker);
}

SEASTAR_THREAD_TEST_CASE(without_auth_result_failure_propagates) {
    auto fut = ppsr::enforce_deferred_authz(
      failed_reply(), nullptr, "get_config");

    BOOST_CHECK_THROW(fut.get(), std::runtime_error);
}
