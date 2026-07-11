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

#include "config/configuration.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/types.h"
#include "ssx/sformat.h"

#include <seastar/testing/perf_tests.hh>

// Measures security::validate_scram_credential, the password check that
// request_authenticator::do_authenticate runs on every HTTP Basic auth
// request (pandaproxy, schema registry, admin API). The PBKDF2-style
// salted-password derivation (scram_algorithm::hi, min_iterations = 4096
// HMAC rounds) is what pegged pandaproxy CPU in INC-2882. A wrong password
// costs the same as a correct one: the mismatch is only detected after the
// full derivation.
//
// The plain cases run with the credential cache at its default (disabled)
// and pay the full derivation on every call. The _cached group enables
// scram_credential_cache_enabled: its correct/wrong cases reuse one
// credential per case, so repeat calls are served from the cache, while
// its unique-password case never repeats and always pays the full
// derivation.

namespace {

const security::credential_password& password() {
    static const security::credential_password p{"benchpassword"};
    return p;
}

const security::credential_password& wrong_password() {
    static const security::credential_password p{"wrongpassword"};
    return p;
}

const security::scram_credential& sha256_credential() {
    static const auto cred = security::scram_sha256::make_credentials(
      password()(), security::scram_sha256::min_iterations);
    return cred;
}

const security::scram_credential& sha512_credential() {
    static const auto cred = security::scram_sha512::make_credentials(
      password()(), security::scram_sha512::min_iterations);
    return cred;
}

} // namespace

PERF_TEST(validate_scram_credential, sha256_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST(validate_scram_credential, sha256_wrong_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), wrong_password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST(validate_scram_credential, sha512_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha512_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

// Never-repeating passwords: every call pays the full salted-password
// derivation, with no opportunity for memoization.
PERF_TEST(validate_scram_credential, sha256_unique_passwords) {
    static size_t counter = 0;
    security::credential_password unique{
      ssx::sformat("benchpassword-{}", counter++)};
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), unique);
    perf_tests::do_not_optimize(mechanism);
}

namespace {

// Enables the credential cache for the _cached group. The fixture is
// constructed before each run and destroyed after it, so the config
// toggles outside the measured region.
struct validate_scram_credential_cached {
    validate_scram_credential_cached() {
        config::shard_local_cfg().scram_credential_cache_enabled.set_value(
          true);
    }
    validate_scram_credential_cached(const validate_scram_credential_cached&)
      = delete;
    validate_scram_credential_cached&
    operator=(const validate_scram_credential_cached&) = delete;
    validate_scram_credential_cached(validate_scram_credential_cached&&)
      = delete;
    validate_scram_credential_cached&
    operator=(validate_scram_credential_cached&&) = delete;
    ~validate_scram_credential_cached() {
        config::shard_local_cfg().scram_credential_cache_enabled.reset();
    }
};

} // namespace

PERF_TEST_F(validate_scram_credential_cached, sha256_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST_F(validate_scram_credential_cached, sha256_wrong_password) {
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), wrong_password());
    perf_tests::do_not_optimize(mechanism);
}

PERF_TEST_F(validate_scram_credential_cached, sha512_correct_password) {
    auto mechanism = security::validate_scram_credential(
      sha512_credential(), password());
    perf_tests::do_not_optimize(mechanism);
}

// No hit is possible, so every call pays the full derivation plus the
// cache lookup and insertion overhead.
PERF_TEST_F(validate_scram_credential_cached, sha256_unique_passwords) {
    static size_t counter = 0;
    security::credential_password unique{
      ssx::sformat("benchpassword-{}", counter++)};
    auto mechanism = security::validate_scram_credential(
      sha256_credential(), unique);
    perf_tests::do_not_optimize(mechanism);
}
