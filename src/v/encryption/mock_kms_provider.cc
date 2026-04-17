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

#include "encryption/mock_kms_provider.h"

#include "crypto/exceptions.h"
#include "crypto/ssl_utils.h"

#include <seastar/core/future.hh>

#include <openssl/evp.h>

#include <array>

namespace encryption {

namespace {

// Fixed 256-bit key used by the mock KMS for AES key wrap.
// Deterministic: every mock_kms_provider instance uses this key.
constexpr std::array<uint8_t, 32> fixed_wrapping_key = {
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
  0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
  0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
};

using evp_cipher_ctx_ptr
  = crypto::internal::handle<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free>;

bytes aes_key_wrap(bytes_view wrapping_key, bytes_view plaintext) {
    // AES key wrap requires plaintext to be a multiple of 8 bytes
    // and at least 16 bytes
    if (plaintext.size() < 16 || plaintext.size() % 8 != 0) {
        throw crypto::exception(
          "AES key wrap: plaintext must be >= 16 bytes and a multiple of 8");
    }

    evp_cipher_ctx_ptr ctx(EVP_CIPHER_CTX_new());
    if (!ctx) {
        throw crypto::internal::ossl_error("Failed to create EVP_CIPHER_CTX");
    }

    // EVP_aes_256_wrap requires the wrap flag to be set before init
    EVP_CIPHER_CTX_set_flags(ctx.get(), EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);

    if (
      1
      != EVP_EncryptInit_ex(
        ctx.get(), EVP_aes_256_wrap(), nullptr, wrapping_key.data(), nullptr)) {
        throw crypto::internal::ossl_error(
          "AES key wrap: EVP_EncryptInit_ex failed");
    }

    // Wrapped output is plaintext_len + 8 (for the integrity check value)
    const auto out_len = plaintext.size() + 8;
    bytes wrapped(bytes::initialized_later(), out_len);

    int len = 0;
    if (
      1
      != EVP_EncryptUpdate(
        ctx.get(),
        wrapped.data(),
        &len,
        plaintext.data(),
        static_cast<int>(plaintext.size()))) {
        throw crypto::internal::ossl_error(
          "AES key wrap: EVP_EncryptUpdate failed");
    }

    int final_len = 0;
    if (1 != EVP_EncryptFinal_ex(ctx.get(), wrapped.data() + len, &final_len)) {
        throw crypto::internal::ossl_error(
          "AES key wrap: EVP_EncryptFinal_ex failed");
    }

    wrapped.resize(len + final_len);
    return wrapped;
}

bytes aes_key_unwrap(bytes_view wrapping_key, bytes_view ciphertext) {
    // Wrapped key must be at least 24 bytes (16 plaintext + 8 ICV)
    // and a multiple of 8
    if (ciphertext.size() < 24 || ciphertext.size() % 8 != 0) {
        throw crypto::exception(
          "AES key unwrap: ciphertext must be >= 24 bytes and a multiple of 8");
    }

    evp_cipher_ctx_ptr ctx(EVP_CIPHER_CTX_new());
    if (!ctx) {
        throw crypto::internal::ossl_error("Failed to create EVP_CIPHER_CTX");
    }

    EVP_CIPHER_CTX_set_flags(ctx.get(), EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);

    if (
      1
      != EVP_DecryptInit_ex(
        ctx.get(), EVP_aes_256_wrap(), nullptr, wrapping_key.data(), nullptr)) {
        throw crypto::internal::ossl_error(
          "AES key unwrap: EVP_DecryptInit_ex failed");
    }

    // Unwrapped output is ciphertext_len - 8
    const auto out_len = ciphertext.size() - 8;
    bytes unwrapped(bytes::initialized_later(), out_len);

    int len = 0;
    if (
      1
      != EVP_DecryptUpdate(
        ctx.get(),
        unwrapped.data(),
        &len,
        ciphertext.data(),
        static_cast<int>(ciphertext.size()))) {
        throw crypto::internal::ossl_error(
          "AES key unwrap: integrity check failed");
    }

    int final_len = 0;
    if (
      1 != EVP_DecryptFinal_ex(ctx.get(), unwrapped.data() + len, &final_len)) {
        throw crypto::internal::ossl_error(
          "AES key unwrap: EVP_DecryptFinal_ex failed");
    }

    unwrapped.resize(len + final_len);
    return unwrapped;
}

} // namespace

mock_kms_provider::mock_kms_provider() = default;

ss::future<bytes>
mock_kms_provider::wrap_dek(ss::sstring /*kms_key_id*/, bytes plaintext_dek) {
    bytes_view key(fixed_wrapping_key.data(), fixed_wrapping_key.size());
    return ss::make_ready_future<bytes>(aes_key_wrap(key, plaintext_dek));
}

ss::future<bytes>
mock_kms_provider::unwrap_dek(ss::sstring /*kms_key_id*/, bytes encrypted_dek) {
    bytes_view key(fixed_wrapping_key.data(), fixed_wrapping_key.size());
    return ss::make_ready_future<bytes>(aes_key_unwrap(key, encrypted_dek));
}

} // namespace encryption
