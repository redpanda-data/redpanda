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
#pragma once

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "config/property.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/shared_ptr.hh>

#include <array>
#include <cstddef>
#include <optional>

namespace security {

/// \brief Per-shard memoization of SCRAM password derivations.
///
/// Validating a plaintext password against a stored SCRAM credential
/// rederives the credential's stored key (RFC 5802 `Hi`, at least 4096 HMAC
/// rounds). HTTP Basic auth and SASL/PLAIN pay that cost on every
/// authentication, which is expensive enough to saturate a core at a few
/// thousand authentications per second. This cache memoizes the derivation:
/// (algorithm, password, salt, iterations) -> stored key.
///
/// Entries are keyed by an HMAC (with a random per-instance key) of the
/// inputs so plaintext passwords are not retained. The value is the stored
/// key derived from the presented password: for a correct password this is
/// the credential's stored key, already persisted in the credential store;
/// an incorrect password is cached too (so repeated bad credentials stay
/// cheap), and its derived value reveals nothing about the stored credential.
/// Either way a memory disclosure yields no credential secret beyond what the
/// store already holds. The memoized mapping is purely functional, so entries
/// never go stale: updating a credential generates a fresh salt, which
/// changes the cache key.
///
class scram_credential_cache {
public:
    static constexpr size_t default_capacity = 1024;

    struct stats_t {
        size_t hits;
        size_t misses;
        size_t size;
    };

    /// Secret bytes, securely erased on destruction: eviction, flush, and
    /// shutdown all scrub the cache's secrets through this one destructor.
    struct zeroizing_bytes {
        explicit zeroizing_bytes(bytes b) noexcept
          : data(std::move(b)) {}
        zeroizing_bytes(const zeroizing_bytes&) = delete;
        zeroizing_bytes& operator=(const zeroizing_bytes&) = delete;
        zeroizing_bytes(zeroizing_bytes&&) = delete;
        zeroizing_bytes& operator=(zeroizing_bytes&&) = delete;
        ~zeroizing_bytes();

        bytes data;
    };

    explicit scram_credential_cache(size_t capacity);

    /// Returns the memoized stored key for the derivation inputs, or null on
    /// a miss.
    ss::shared_ptr<const zeroizing_bytes> get(
      scram_algorithm_t mech,
      const credential_password& password,
      bytes_view salt,
      int iterations);

    /// Memoizes the stored key derived from the given inputs.
    void put(
      scram_algorithm_t mech,
      const credential_password& password,
      bytes_view salt,
      int iterations,
      bytes stored_key);

    stats_t stats() const;

private:
    static constexpr size_t digest_size = 32;

    struct key_t {
        std::array<char, digest_size> digest;

        bool operator==(const key_t&) const = default;

        template<typename H>
        friend H AbslHashValue(H h, const key_t& k) {
            return H::combine(std::move(h), k.digest);
        }
    };

    key_t make_key(
      scram_algorithm_t mech,
      const credential_password& password,
      bytes_view salt,
      int iterations) const;

    zeroizing_bytes _digest_key;
    utils::chunked_kv_cache<key_t, zeroizing_bytes> _cache;
};

/// \brief Ties a shard's scram_credential_cache to its enable config.
///
/// get() returns the cache while enabled (constructing it on first use) and
/// nullptr while disabled. Disabling flushes eagerly: the config watch
/// destroys the cache, securely erasing every memoized derivation, without
/// waiting for the next authentication on the shard. Re-enabling builds a
/// fresh cache with a fresh digest key.
class scram_credential_cache_holder {
public:
    scram_credential_cache_holder(
      config::binding<bool> enabled, size_t capacity);

    scram_credential_cache* get();

private:
    config::binding<bool> _enabled;
    size_t _capacity;
    std::optional<scram_credential_cache> _cache;
};

} // namespace security
