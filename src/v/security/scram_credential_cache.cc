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
#include "security/scram_credential_cache.h"

#include "base/vlog.h"
#include "bytes/random.h"
#include "crypto/crypto.h"
#include "hashing/secure.h"
#include "security/logger.h"

#include <seastar/core/shared_ptr.hh>

#include <algorithm>
#include <bit>

namespace security {

namespace {
// The S3-FIFO probationary queue is conventionally ~10% of the cache.
constexpr size_t small_queue_ratio = 10;
} // namespace

scram_credential_cache::zeroizing_bytes::~zeroizing_bytes() {
    crypto::secure_erase({data.data(), data.size()});
}

scram_credential_cache::scram_credential_cache(size_t capacity)
  : _digest_key(random_generators::get_crypto_bytes(digest_size))
  , _cache([capacity]() -> decltype(_cache)::config {
      auto small_size = std::max<size_t>(1, capacity / small_queue_ratio);
      // s3_fifo::cache requires cache_size > small_size; derive cache_size
      // from small_size so the invariant holds even for tiny capacities.
      return {
        .cache_size = std::max(capacity, small_size + 1),
        .small_size = small_size};
  }()) {}

scram_credential_cache::key_t scram_credential_cache::make_key(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations) const {
    // The digest is keyed with a random per-instance secret so that cached
    // keys are not plain password hashes: without the secret, a digest
    // cannot be matched against password guesses. HMAC rather than
    // sha256(secret ‖ inputs) because direct hashing has a length-extension
    // weakness; HMAC is the standard way to key a hash with a secret.
    hmac_sha256 mac(_digest_key.data);
    // The packing must keep field boundaries unambiguous: password and
    // salt are both variable-length fields, so the password length is
    // written first; without it, (password "ab", salt "c") and (password
    // "a", salt "bc") would pack to the same bytes and collide.
    mac.update(std::array<char, 1>{static_cast<char>(mech)});
    mac.update(
      std::bit_cast<std::array<char, sizeof(uint32_t)>>(
        static_cast<uint32_t>(iterations)));
    mac.update(
      std::bit_cast<std::array<char, sizeof(uint32_t)>>(
        static_cast<uint32_t>(password().size())));
    mac.update(std::string_view{password()});
    mac.update(salt);
    return {.digest = mac.reset()};
}

ss::shared_ptr<const scram_credential_cache::zeroizing_bytes>
scram_credential_cache::get(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations) {
    auto val = _cache.get_value(make_key(mech, password, salt, iterations));
    if (!val) {
        return nullptr;
    }
    return std::move(*val);
}

void scram_credential_cache::put(
  scram_algorithm_t mech,
  const credential_password& password,
  bytes_view salt,
  int iterations,
  bytes stored_key) {
    _cache.try_insert(
      make_key(mech, password, salt, iterations),
      ss::make_shared<zeroizing_bytes>(std::move(stored_key)));
}

scram_credential_cache::stats_t scram_credential_cache::stats() const {
    auto s = _cache.stat();
    return {
      .hits = s.hit_count,
      .misses = s.access_count - s.hit_count,
      .size = s.index_size};
}

scram_credential_cache_holder::scram_credential_cache_holder(
  config::binding<bool> enabled, size_t capacity)
  : _enabled(std::move(enabled))
  , _capacity(capacity) {
    _enabled.watch([this] {
        if (!_enabled() && _cache) {
            vlog(seclog.info, "SCRAM credential cache disabled, flushing");
            _cache.reset();
        }
    });
}

scram_credential_cache* scram_credential_cache_holder::get() {
    if (!_enabled()) {
        return nullptr;
    }
    if (!_cache) {
        // Constructed here rather than in the watch so that a throwing
        // construction surfaces on the authentication path (which simply
        // retries next call) instead of failing a config update.
        _cache.emplace(_capacity);
        vlog(
          seclog.debug,
          "SCRAM credential cache constructed, capacity {}",
          _capacity);
    }
    return &*_cache;
}

} // namespace security
