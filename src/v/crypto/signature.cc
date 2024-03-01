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
#include "key.h"
#include "ssl_utils.h"

namespace crypto {
verify_ctx::~verify_ctx() noexcept = default;

class verify_ctx::impl {
public:
    impl(digest_type type, const key& key)
      : _ctx(EVP_MD_CTX_new()) {
        if (!_ctx) {
            throw internal::ossl_error("Failed to create EVP_MD_CTX");
        }

        if (
          1
          != EVP_DigestVerifyInit(
            _ctx.get(),
            nullptr,
            internal::get_md(type),
            nullptr,
            key._impl->get_pkey())) {
            throw internal::ossl_error(
              "Failed to initialize verification operation");
        }
    }

    void update(bytes_view msg) {
        if (1 != EVP_DigestVerifyUpdate(_ctx.get(), msg.data(), msg.size())) {
            throw internal::ossl_error("Failed to update verify operation");
        }
    }

    bool final(bytes_view sig) {
        auto verify_result = EVP_DigestVerifyFinal(
          _ctx.get(), sig.data(), sig.size());
        if (verify_result == 1) {
            return true;
        } else if (verify_result == 0) {
            return false;
        } else {
            throw internal::ossl_error(
              "Failed to validate signature due to error");
        }
    }

private:
    internal::EVP_MD_CTX_ptr _ctx;
};

verify_ctx::verify_ctx(digest_type type, const key& key)
  : _impl(std::make_unique<impl>(type, key)) {}

verify_ctx& verify_ctx::update(bytes_view msg) {
    _impl->update(msg);
    return *this;
}

verify_ctx& verify_ctx::update(std::string_view msg) {
    return update(internal::string_view_to_bytes_view(msg));
}

bool verify_ctx::final(bytes_view sig) && { return _impl->final(sig); }

bool verify_ctx::final(std::string_view sig) && {
    return _impl->final(internal::string_view_to_bytes_view(sig));
}

bool verify_signature(
  digest_type type, const key& key, bytes_view msg, bytes_view sig) { // NOLINT
    verify_ctx ctx(type, key);
    ctx.update(msg);
    return std::move(ctx).final(sig);
}

bool verify_signature(
  digest_type type,
  const key& key,
  std::string_view msg,
  std::string_view sig) {
    return verify_signature(
      type,
      key,
      internal::string_view_to_bytes_view(msg),
      internal::string_view_to_bytes_view(sig));
}
} // namespace crypto
