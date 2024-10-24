/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "key.h"

#include "crypto/crypto.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"
#include "internal.h"
#include "ssl_utils.h"
#include "thirdparty/openssl/bio.h"
#include "thirdparty/openssl/bn.h"
#include "thirdparty/openssl/evp.h"
#include "thirdparty/openssl/param_build.h"
#include "thirdparty/openssl/pem.h"

#include <absl/container/flat_hash_set.h>

namespace crypto {

namespace internal {
EVP_PKEY_ptr load_public_pem_key(bytes_view key) {
    BIO_ptr key_bio(BIO_new_mem_buf(key.data(), static_cast<int>(key.size())));
    EVP_PKEY_ptr pkey(
      PEM_read_bio_PUBKEY(key_bio.get(), nullptr, nullptr, nullptr));
    if (!pkey) {
        throw ossl_error("Failed to load PEM public key");
    }

    return pkey;
}

EVP_PKEY_ptr load_private_pem_key(bytes_view key) {
    BIO_ptr key_bio(BIO_new_mem_buf(key.data(), static_cast<int>(key.size())));
    EVP_PKEY_ptr pkey(
      PEM_read_bio_PrivateKey(key_bio.get(), nullptr, nullptr, nullptr));
    if (!pkey) {
        throw ossl_error("Failed to load PEM private key");
    }

    return pkey;
}

} // namespace internal

key::~key() noexcept = default;
key::key(key&&) noexcept = default;
key& key::operator=(key&&) noexcept = default;

static_assert(
  std::is_nothrow_move_constructible_v<key>,
  "key should be nothrow move constructible");
static_assert(
  std::is_nothrow_move_assignable_v<key>,
  "key should be nothrow move assignable");

key::impl::~impl() noexcept = default;

key::impl::impl(
  _private, internal::EVP_PKEY_ptr pkey, is_private_key_t is_private_key)
  : _pkey(std::move(pkey))
  , _is_private_key(is_private_key) {
    static const absl::flat_hash_set<int> supported_key_types{EVP_PKEY_RSA};

    auto key_type = EVP_PKEY_get_base_id(_pkey.get());

    if (!supported_key_types.contains(key_type)) {
        throw exception(fmt::format("Unsupported key type {}", key_type));
    }
}

key_type key::impl::get_key_type() const {
    auto key_type = EVP_PKEY_get_base_id(_pkey.get());

    // NOLINTNEXTLINE: For when we add more supported key types
    switch (key_type) {
    case EVP_PKEY_RSA:
        return key_type::RSA;
    }

    vassert(false, "Unsupported key type {}", key_type);
}

key_type key::get_type() const { return _impl->get_key_type(); }
is_private_key_t key::is_private_key() const { return _impl->is_private_key(); }

std::unique_ptr<key::impl>
key::impl::load_pem_key(bytes_view key, is_private_key_t is_private_key) {
    auto key_ptr = is_private_key == is_private_key_t::yes
                     ? internal::load_private_pem_key(key)
                     : internal::load_public_pem_key(key);

    return std::make_unique<key::impl>(
      key::impl::_private{}, std::move(key_ptr), is_private_key);
}

std::unique_ptr<key::impl>
key::impl::load_rsa_public_key(bytes_view n, bytes_view e) {
    auto n_bn = internal::BN_ptr(
      BN_bin2bn(n.data(), static_cast<int>(n.size()), nullptr));
    if (!n_bn) {
        throw internal::ossl_error("Failed to load exponent bignum value");
    }
    auto e_bn = internal::BN_ptr(
      BN_bin2bn(e.data(), static_cast<int>(e.size()), nullptr));
    if (!e_bn) {
        throw internal::ossl_error(
          "Failed to load public exponent bignum value");
    }
    auto param_bld = internal::OSSL_PARAM_BLD_ptr(OSSL_PARAM_BLD_new());
    if (!param_bld) {
        throw internal::ossl_error(
          "Failed to create param builder for loading RSA public key");
    }
    if (
      !OSSL_PARAM_BLD_push_BN(param_bld.get(), "n", n_bn.get())
      || !OSSL_PARAM_BLD_push_BN(param_bld.get(), "e", e_bn.get())) {
        throw internal::ossl_error("Failed to set E or N to parameters");
    }

    auto params = internal::OSSL_PARAM_ptr(
      OSSL_PARAM_BLD_to_param(param_bld.get()));
    if (!params) {
        throw internal::ossl_error("Failed to great parameters from builder");
    }

    auto ctx = internal::EVP_PKEY_CTX_ptr(
      EVP_PKEY_CTX_new_from_name(nullptr, "RSA", nullptr));
    if (!ctx) {
        throw internal::ossl_error("Failed to create RSA PKEY context");
    }

    if (EVP_PKEY_fromdata_init(ctx.get()) != 1) {
        throw internal::ossl_error("Failed to initialize EVP PKEY from data");
    }

    EVP_PKEY* pkey = nullptr;

    if (
      1
      != EVP_PKEY_fromdata(
        ctx.get(), &pkey, EVP_PKEY_PUBLIC_KEY, params.get())) {
        throw internal::ossl_error(
          "Failed to load RSA public key from parameters");
    }

    auto pkey_ptr = internal::EVP_PKEY_ptr(pkey);
    {
        // For some reason, even though the original PKEY_CTX was used to load
        // the key, it's not 'saved' within that ctx so we need to create a
        // _new_ one to verify that the key was good
        auto verify_ctx = internal::EVP_PKEY_CTX_ptr(
          EVP_PKEY_CTX_new_from_pkey(nullptr, pkey_ptr.get(), nullptr));
        if (1 != EVP_PKEY_public_check(verify_ctx.get())) {
            throw internal::ossl_error("Failed RSA public key validation");
        }
    }

    return std::make_unique<key::impl>(
      _private{}, std::move(pkey_ptr), is_private_key_t::no);
}

key::key(std::unique_ptr<impl>&& impl)
  : _impl(std::move(impl)) {}

key key::load_key(
  bytes_view key_buffer, format_type fmt, is_private_key_t is_private_key) {
    auto key_ptr = fmt == format_type::PEM
                     ? key::impl::load_pem_key(key_buffer, is_private_key)
                     : nullptr;

    return key{std::move(key_ptr)};
}

key key::load_key(
  std::string_view key_buffer,
  format_type fmt,
  is_private_key_t is_private_key) {
    return key::load_key(
      internal::string_view_to_bytes_view(key_buffer), fmt, is_private_key);
}

key key::load_rsa_public_key(bytes_view n, bytes_view e) {
    return key{key::impl::load_rsa_public_key(n, e)};
}

key key::load_rsa_public_key(std::string_view n, std::string_view e) {
    return key::load_rsa_public_key(
      internal::string_view_to_bytes_view(n),
      internal::string_view_to_bytes_view(e));
}
} // namespace crypto
