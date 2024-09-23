// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "random/generators.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential.h"
#include "security/sasl_authentication.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace security {

BOOST_AUTO_TEST_CASE(credential_store_test) {
    const scram_credential cred0(
      bytes::from_string("salty"),
      bytes::from_string("i'm a server key"),
      bytes::from_string("i'm the stored key"),
      123456);

    const scram_credential cred1(
      bytes::from_string("salty2"),
      bytes::from_string("i'm a server key2"),
      bytes::from_string("i'm the stored key2"),
      1234567);

    auto cred0_copy = cred0;
    auto cred1_copy = cred1;

    BOOST_REQUIRE_NE(cred0, cred1);
    BOOST_REQUIRE_NE(cred0_copy, cred1_copy);

    const credential_user copied("copied");
    const credential_user moved("moved");

    // put new credentials
    credential_store store;
    store.put(copied, cred0);
    store.put(moved, std::move(cred0_copy));

    BOOST_REQUIRE(store.get<scram_credential>(moved));
    BOOST_REQUIRE_EQUAL(*store.get<scram_credential>(moved), cred0);

    BOOST_REQUIRE(store.get<scram_credential>(copied));
    BOOST_REQUIRE_EQUAL(*store.get<scram_credential>(copied), cred0);

    // update credentials
    store.put(copied, cred1);
    store.put(moved, std::move(cred1_copy));

    BOOST_REQUIRE(store.get<scram_credential>(moved));
    BOOST_REQUIRE_EQUAL(*store.get<scram_credential>(moved), cred1);

    BOOST_REQUIRE(store.get<scram_credential>(copied));
    BOOST_REQUIRE_EQUAL(*store.get<scram_credential>(copied), cred1);
}

BOOST_AUTO_TEST_CASE(credential_store_test_principal) {
    const scram_credential cred0(
      bytes::from_string("salty"),
      bytes::from_string("i'm a server key"),
      bytes::from_string("i'm the stored key"),
      123456);

    const scram_credential cred1(
      bytes::from_string("salty2"),
      bytes::from_string("i'm a server key2"),
      bytes::from_string("i'm the stored key2"),
      1234567,
      acl_principal{principal_type::ephemeral_user, "ephemeral"});

    const credential_user user0("user0");
    const credential_user user1("user1");

    // put new credentials
    credential_store store;
    store.put(user0, cred0);
    store.put(user1, cred1);

    auto r0 = store.get<scram_credential>(user0);
    auto r1 = store.get<scram_credential>(user1);
    BOOST_REQUIRE_EQUAL(r0->principal().has_value(), false);
    BOOST_REQUIRE_EQUAL(
      r1->principal()->type(), principal_type::ephemeral_user);
    BOOST_REQUIRE_EQUAL(r1->principal()->name(), "ephemeral");
}

} // namespace security
