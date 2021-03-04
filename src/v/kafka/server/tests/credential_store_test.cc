// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "kafka/security/credential_store.h"
#include "random/generators.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

BOOST_AUTO_TEST_CASE(credential_store_test) {
    const kafka::scram_credential cred0(
      bytes("salty"),
      bytes("i'm a server key"),
      bytes("i'm the stored key"),
      123456);

    const kafka::scram_credential cred1(
      bytes("salty2"),
      bytes("i'm a server key2"),
      bytes("i'm the stored key2"),
      1234567);

    auto cred0_copy = cred0;
    auto cred1_copy = cred1;

    BOOST_REQUIRE_NE(cred0, cred1);
    BOOST_REQUIRE_NE(cred0_copy, cred1_copy);

    // put new credentials
    kafka::credential_store store;
    store.put("copied", cred0);
    store.put("moved", std::move(cred0_copy));

    BOOST_REQUIRE(store.get<kafka::scram_credential>("moved"));
    BOOST_REQUIRE_EQUAL(*store.get<kafka::scram_credential>("moved"), cred0);

    BOOST_REQUIRE(store.get<kafka::scram_credential>("copied"));
    BOOST_REQUIRE_EQUAL(*store.get<kafka::scram_credential>("copied"), cred0);

    // update credentials
    store.put("copied", cred1);
    store.put("moved", std::move(cred1_copy));

    BOOST_REQUIRE(store.get<kafka::scram_credential>("moved"));
    BOOST_REQUIRE_EQUAL(*store.get<kafka::scram_credential>("moved"), cred1);

    BOOST_REQUIRE(store.get<kafka::scram_credential>("copied"));
    BOOST_REQUIRE_EQUAL(*store.get<kafka::scram_credential>("copied"), cred1);
}
