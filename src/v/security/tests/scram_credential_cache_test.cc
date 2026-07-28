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
#define BOOST_TEST_MODULE security

#include "bytes/bytes.h"
#include "config/configuration.h"
#include "config/mock_property.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential_cache.h"
#include "security/types.h"
#include "test_utils/random_bytes.h"

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

namespace security {

namespace {

const credential_password& password() {
    static const credential_password p{"cache-test-password"};
    return p;
}

constexpr int iterations = 4096;

} // namespace

BOOST_AUTO_TEST_CASE(cache_miss_then_hit) {
    scram_credential_cache cache(16);
    auto salt = tests::random_bytes(16);
    auto stored_key = tests::random_bytes(32);

    BOOST_REQUIRE(
      cache.get(scram_algorithm_t::sha256, password(), salt, iterations)
      == nullptr);

    cache.put(
      scram_algorithm_t::sha256, password(), salt, iterations, stored_key);

    auto hit = cache.get(
      scram_algorithm_t::sha256, password(), salt, iterations);
    BOOST_REQUIRE(hit != nullptr);
    BOOST_REQUIRE(hit->data == stored_key);

    auto stats = cache.stats();
    BOOST_REQUIRE_EQUAL(stats.hits, 1);
    BOOST_REQUIRE_EQUAL(stats.misses, 1);
}

BOOST_AUTO_TEST_CASE(cache_key_includes_all_inputs) {
    scram_credential_cache cache(16);
    auto salt = tests::random_bytes(16);
    auto stored_key = tests::random_bytes(32);

    cache.put(
      scram_algorithm_t::sha256, password(), salt, iterations, stored_key);

    // Same inputs: hit.
    BOOST_REQUIRE(
      cache.get(scram_algorithm_t::sha256, password(), salt, iterations)
      != nullptr);
    // Different password: miss.
    BOOST_REQUIRE(
      cache.get(
        scram_algorithm_t::sha256,
        credential_password{"other-password"},
        salt,
        iterations)
      == nullptr);
    // Different salt (i.e. the credential was updated): miss.
    auto other_salt = tests::random_bytes(16);
    BOOST_REQUIRE(
      cache.get(scram_algorithm_t::sha256, password(), other_salt, iterations)
      == nullptr);
    // Different iteration count: miss.
    BOOST_REQUIRE(
      cache.get(scram_algorithm_t::sha256, password(), salt, iterations * 2)
      == nullptr);
    // Different mechanism: miss.
    BOOST_REQUIRE(
      cache.get(scram_algorithm_t::sha512, password(), salt, iterations)
      == nullptr);
}

BOOST_AUTO_TEST_CASE(cache_is_bounded) {
    constexpr size_t capacity = 16;
    scram_credential_cache cache(capacity);
    auto salt = tests::random_bytes(16);

    for (size_t i = 0; i < capacity * 10; ++i) {
        cache.put(
          scram_algorithm_t::sha256,
          credential_password{fmt::format("password-{}", i)},
          salt,
          iterations,
          tests::random_bytes(32));
    }

    // A one-hit-wonder inserted early must have been evicted.
    BOOST_REQUIRE(
      cache.get(
        scram_algorithm_t::sha256,
        credential_password{"password-0"},
        salt,
        iterations)
      == nullptr);
    // The index may retain evicted (ghost) entries, but remains bounded well
    // below the number of insertions.
    BOOST_REQUIRE_LT(cache.stats().size, capacity * 5);
}

BOOST_AUTO_TEST_CASE(cache_tiny_capacity_does_not_crash) {
    // The S3-FIFO backing asserts cache_size > small_size; capacities below
    // that minimum must be floored rather than trip the assertion.
    for (size_t capacity : {size_t{0}, size_t{1}, size_t{2}}) {
        scram_credential_cache cache(capacity);
        auto salt = tests::random_bytes(16);
        auto stored_key = tests::random_bytes(32);
        cache.put(
          scram_algorithm_t::sha256, password(), salt, iterations, stored_key);
        BOOST_REQUIRE(
          cache.get(scram_algorithm_t::sha256, password(), salt, iterations)
          != nullptr);
    }
}

BOOST_AUTO_TEST_CASE(cached_validate_scram_credential) {
    scram_credential_cache cache(16);
    auto cred = scram_sha256::make_credentials(
      password()(), scram_sha256::min_iterations);

    auto mech = detail::validate_scram_credential(cred, password(), &cache);
    BOOST_REQUIRE(mech.has_value());
    BOOST_REQUIRE_EQUAL(*mech, scram_sha256_authenticator::name);
    BOOST_REQUIRE_EQUAL(cache.stats().hits, 0);

    // Repeat validation is served from the cache with the same result.
    mech = detail::validate_scram_credential(cred, password(), &cache);
    BOOST_REQUIRE(mech.has_value());
    BOOST_REQUIRE_EQUAL(*mech, scram_sha256_authenticator::name);
    BOOST_REQUIRE_EQUAL(cache.stats().hits, 1);

    // A wrong password fails, and keeps failing once its derivation is
    // cached.
    const credential_password wrong{"wrong-password"};
    BOOST_REQUIRE(
      !detail::validate_scram_credential(cred, wrong, &cache).has_value());
    BOOST_REQUIRE(
      !detail::validate_scram_credential(cred, wrong, &cache).has_value());
}

BOOST_AUTO_TEST_CASE(cached_validate_scram_credential_sha512) {
    scram_credential_cache cache(16);
    auto cred = scram_sha512::make_credentials(
      password()(), scram_sha512::min_iterations);

    for (int i = 0; i < 2; ++i) {
        auto mech = detail::validate_scram_credential(cred, password(), &cache);
        BOOST_REQUIRE(mech.has_value());
        BOOST_REQUIRE_EQUAL(*mech, scram_sha512_authenticator::name);
    }
    BOOST_REQUIRE_EQUAL(cache.stats().hits, 1);
}

BOOST_AUTO_TEST_CASE(cached_validate_scram_credential_password_change) {
    scram_credential_cache cache(16);
    auto cred = scram_sha256::make_credentials(
      password()(), scram_sha256::min_iterations);

    // Warm the cache with the old credential.
    BOOST_REQUIRE(detail::validate_scram_credential(cred, password(), &cache));
    BOOST_REQUIRE(detail::validate_scram_credential(cred, password(), &cache));

    // The user's password is changed: the new credential gets a fresh salt.
    const credential_password new_password{"brand-new-password"};
    auto new_cred = scram_sha256::make_credentials(
      new_password(), scram_sha256::min_iterations);

    // The stale cache entries do not interfere in either direction.
    BOOST_REQUIRE(
      detail::validate_scram_credential(new_cred, new_password, &cache)
        .has_value());
    BOOST_REQUIRE(
      !detail::validate_scram_credential(new_cred, password(), &cache)
         .has_value());
}

BOOST_AUTO_TEST_CASE(holder_disabled_returns_no_cache) {
    config::mock_property<bool> enabled(false);
    scram_credential_cache_holder holder(enabled.bind(), 16);
    BOOST_REQUIRE(holder.get() == nullptr);
}

BOOST_AUTO_TEST_CASE(holder_flushes_on_disable) {
    config::mock_property<bool> enabled(true);
    scram_credential_cache_holder holder(enabled.bind(), 16);
    auto salt = tests::random_bytes(16);
    auto stored_key = tests::random_bytes(32);

    auto* cache = holder.get();
    BOOST_REQUIRE(cache != nullptr);
    cache->put(
      scram_algorithm_t::sha256, password(), salt, iterations, stored_key);
    BOOST_REQUIRE(
      cache->get(scram_algorithm_t::sha256, password(), salt, iterations)
      != nullptr);

    // Disabling flushes: the cache is destroyed eagerly, without waiting for
    // the next authentication.
    enabled.update(false);
    BOOST_REQUIRE(holder.get() == nullptr);

    // Re-enabling builds a fresh cache: the old entry is gone.
    enabled.update(true);
    auto* fresh = holder.get();
    BOOST_REQUIRE(fresh != nullptr);
    BOOST_REQUIRE(
      fresh->get(scram_algorithm_t::sha256, password(), salt, iterations)
      == nullptr);
    BOOST_REQUIRE_EQUAL(fresh->stats().size, 0);
}

BOOST_AUTO_TEST_CASE(public_validate_survives_config_toggling) {
    auto& enabled = config::shard_local_cfg().scram_credential_cache_enabled;
    auto cred = scram_sha256::make_credentials(
      password()(), scram_sha256::min_iterations);

    enabled.set_value(true);
    BOOST_REQUIRE(validate_scram_credential(cred, password()).has_value());
    BOOST_REQUIRE(validate_scram_credential(cred, password()).has_value());

    // Disable mid-stream: validation falls back to plain derivation.
    enabled.set_value(false);
    BOOST_REQUIRE(validate_scram_credential(cred, password()).has_value());

    // Re-enable: caching resumes with a fresh cache.
    enabled.set_value(true);
    BOOST_REQUIRE(validate_scram_credential(cred, password()).has_value());
    const credential_password wrong{"wrong-password"};
    BOOST_REQUIRE(!validate_scram_credential(cred, wrong).has_value());

    enabled.reset();
}

} // namespace security
