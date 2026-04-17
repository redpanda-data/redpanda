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

#include "encryption/dek_manager.h"

#include "base/vassert.h"
#include "crypto/crypto.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

namespace encryption {

namespace {

size_t dek_key_size(dek_algorithm algo) {
    switch (algo) {
    case dek_algorithm::aes128_gcm:
        return 16;
    case dek_algorithm::aes256_gcm:
    case dek_algorithm::aes256_siv:
        return 32;
    }
    vassert(false, "unknown dek_algorithm: {}", static_cast<int>(algo));
    return 0;
}

bool is_expired(
  const dek_state& dek, std::optional<std::chrono::seconds> expiry) {
    if (!expiry.has_value()) {
        return false;
    }
    auto expiry_ms = expiry->count() * 1000;
    auto deadline = dek.created_at.value() + expiry_ms;
    return deadline < model::timestamp::now().value();
}

} // namespace

dek_manager::dek_manager(kms_provider& kms)
  : _kms(kms) {}

ss::future<dek_state> dek_manager::generate_dek(
  ss::sstring kek_name,
  ss::sstring kms_type,
  ss::sstring kms_key_id,
  dek_algorithm algo,
  uint32_t version) {
    auto key_size = dek_key_size(algo);
    auto plaintext = crypto::generate_random(
      key_size, crypto::use_private_rng::yes);
    auto encrypted = co_await _kms.wrap_dek(kms_key_id, plaintext);
    co_return dek_state{
      .plaintext_dek = std::move(plaintext),
      .encrypted_dek = std::move(encrypted),
      .algorithm = algo,
      .kek_name = std::move(kek_name),
      .kms_type = std::move(kms_type),
      .kms_key_id = std::move(kms_key_id),
      .version = version,
      .created_at = model::timestamp::now(),
      .expiry = std::nullopt,
    };
}

ss::future<dek_state> dek_manager::get_or_create_dek(
  ss::sstring subject,
  ss::sstring kek_name,
  ss::sstring kms_type,
  ss::sstring kms_key_id,
  dek_algorithm algo,
  std::optional<std::chrono::seconds> expiry) {
    auto key = cache_key{subject, kek_name};
    auto it = _active_deks.find(key);

    if (it != _active_deks.end() && !is_expired(it->second, expiry)) {
        co_return it->second;
    }

    // Determine version: first DEK or rotation
    uint32_t version = 1;
    if (it != _active_deks.end()) {
        // Expired -- rotate with incremented version
        version = it->second.version + 1;
    }

    auto dek = co_await generate_dek(
      ss::sstring(kek_name),
      std::move(kms_type),
      std::move(kms_key_id),
      algo,
      version);

    if (expiry.has_value()) {
        auto expiry_ts = model::timestamp(
          dek.created_at.value() + expiry->count() * 1000);
        dek.expiry = expiry_ts;
    }

    auto [ins, _] = _active_deks.insert_or_assign(
      std::move(key), std::move(dek));
    co_return ins->second;
}

} // namespace encryption
