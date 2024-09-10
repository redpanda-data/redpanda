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

#include "crypto/crypto.h"
#include "internal.h"
#include "ssl_utils.h"
#include "thirdparty/openssl/core_names.h"
#include "thirdparty/openssl/evp.h"
#include "thirdparty/openssl/params.h"

namespace crypto {
namespace {
static const auto sha256_params = []() {
    std::array<OSSL_PARAM, 2> params{
      OSSL_PARAM_construct_utf8_string(
        OSSL_MAC_PARAM_DIGEST, const_cast<char*>("SHA256"), 0),
      OSSL_PARAM_construct_end()};
    return params;
}();

static const auto sha512_params = []() {
    std::array<OSSL_PARAM, 2> params{
      OSSL_PARAM_construct_utf8_string(
        OSSL_MAC_PARAM_DIGEST, const_cast<char*>("SHA512"), 0),
      OSSL_PARAM_construct_end()};
    return params;
}();

static const std::array<OSSL_PARAM, 2>& get_param(digest_type type) {
    switch (type) {
    case digest_type::SHA256:
        return sha256_params;
    case digest_type::SHA512:
        return sha512_params;
    default:
        vassert(false, "Cannot create an HMAC for digest type {}", type);
    };
}
} // namespace
class hmac_ctx::impl {
public:
    impl(digest_type type, bytes_view key) {
        _mac_ctx = internal::EVP_MAC_CTX_ptr(
          EVP_MAC_CTX_new(internal::get_mac()));
        if (
          1
          != EVP_MAC_init(
            _mac_ctx.get(), key.data(), key.size(), get_param(type).data())) {
            throw internal::ossl_error(
              fmt::format("Failed to initialize HMAC-{}", type));
        }
    }

    void update(bytes_view msg) {
        if (1 != EVP_MAC_update(_mac_ctx.get(), msg.data(), msg.size())) {
            throw internal::ossl_error("Failed to update MAC operation");
        }
    }

    bytes finish() {
        bytes sig(bytes::initialized_later(), size());
        finish_no_check(sig);
        return sig;
    }

    bytes_span<> finish(bytes_span<> sig) {
        auto len = sig.size();
        if (len != size()) {
            throw exception(fmt::format(
              "Invalid signature buffer length: {} != {}", len, size()));
        }
        return finish_no_check(sig);
    }

    bytes reset() {
        bytes sig(bytes::initialized_later(), size());
        reset(sig);
        return sig;
    }

    bytes_span<> reset(bytes_span<> sig) {
        auto len = sig.size();
        if (len != size()) {
            throw exception(fmt::format(
              "Invalid signature buffer length: {} != {}", len, size()));
        }
        finish_no_check(sig);
        if (1 != EVP_MAC_init(_mac_ctx.get(), nullptr, 0, nullptr)) {
            throw internal::ossl_error("Failed to re-initialize HMAC");
        }

        return sig;
    }

    size_t size() const { return EVP_MAC_CTX_get_mac_size(_mac_ctx.get()); }
    static size_t size(digest_type type) { return digest_ctx::size(type); }

private:
    internal::EVP_MAC_CTX_ptr _mac_ctx;

    static const char* get_digest_str(digest_type type) {
        switch (type) {
        case digest_type::MD5:
            return "MD5";
        case digest_type::SHA256:
            return "SHA256";
        case digest_type::SHA512:
            return "SHA512";
        }
    }

    bytes_span<> finish_no_check(bytes_span<> sig) {
        size_t outl = sig.size();
        if (1 != EVP_MAC_final(_mac_ctx.get(), sig.data(), &outl, sig.size())) {
            throw internal::ossl_error("Failed to finalize MAC operation");
        }
        return sig;
    }
};

hmac_ctx::hmac_ctx(digest_type type, bytes_view key)
  : _impl(std::make_unique<impl>(type, key)) {}

hmac_ctx::hmac_ctx(digest_type type, std::string_view key)
  : _impl(
      std::make_unique<impl>(type, internal::string_view_to_bytes_view(key))) {}

hmac_ctx::~hmac_ctx() noexcept = default;
hmac_ctx::hmac_ctx(hmac_ctx&&) noexcept = default;
hmac_ctx& hmac_ctx::operator=(hmac_ctx&&) noexcept = default;

static_assert(
  std::is_nothrow_move_constructible_v<hmac_ctx>,
  "hmac_ctx should be nothrow move constructible");
static_assert(
  std::is_nothrow_move_assignable_v<hmac_ctx>,
  "hmac_ctx should be nothrow move assignable");

size_t hmac_ctx::size() const { return _impl->size(); }
size_t hmac_ctx::size(digest_type type) { return impl::size(type); }

hmac_ctx& hmac_ctx::update(bytes_view msg) {
    _impl->update(msg);
    return *this;
}

hmac_ctx& hmac_ctx::update(std::string_view msg) {
    return update(internal::string_view_to_bytes_view(msg));
}

bytes hmac_ctx::final() && { return _impl->finish(); }
bytes_span<> hmac_ctx::final(bytes_span<> signature) && {
    return _impl->finish(signature);
}

std::span<char> hmac_ctx::final(std::span<char> signature) && {
    _impl->finish(internal::char_span_to_bytes_span(signature));
    return signature;
}

bytes hmac_ctx::reset() { return _impl->reset(); };
bytes_span<> hmac_ctx::reset(bytes_span<> sig) { return _impl->reset(sig); }
std::span<char> hmac_ctx::reset(std::span<char> sig) {
    _impl->reset(internal::char_span_to_bytes_span(sig));
    return sig;
}

// NOLINTNEXTLINE
bytes hmac(digest_type type, bytes_view key, bytes_view msg) {
    hmac_ctx ctx(type, key);
    ctx.update(msg);
    return std::move(ctx).final();
}

// NOLINTNEXTLINE
bytes hmac(digest_type type, std::string_view key, std::string_view msg) {
    hmac_ctx ctx(type, key);
    ctx.update(msg);
    return std::move(ctx).final();
}
} // namespace crypto
