// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "random/generators.h"
#include "security/acl.h"
#include "security/ephemeral_credential.h"
#include "security/ephemeral_credential_store.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "test_utils/randoms.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace security {

SEASTAR_THREAD_TEST_CASE(test_ephemeral_credential_store_insert) {
    ephemeral_credential_store store{};

    const auto scram_cred{tests::random_credential()};

    const acl_principal principal{
      principal_type::ephemeral_user, "ephemeral_user"};

    const ephemeral_credential ephemeral_cred_0{
      principal,
      credential_user{"user_0"},
      credential_password{"password_0"},
      scram_sha512_authenticator::name};

    const ephemeral_credential ephemeral_cred_1{
      principal,
      credential_user{"user_1"},
      credential_password{"password_1"},
      scram_sha512_authenticator::name};

    // insert
    {
        auto it = store.insert_or_assign(ephemeral_cred_0);
        BOOST_REQUIRE(store.has(it));
        BOOST_REQUIRE_EQUAL(*it, ephemeral_cred_0);
    }

    // assign
    {
        auto it = store.insert_or_assign(ephemeral_cred_1);
        BOOST_REQUIRE(store.has(it));
        BOOST_REQUIRE_EQUAL(*it, ephemeral_cred_1);
    }
}

} // namespace security
