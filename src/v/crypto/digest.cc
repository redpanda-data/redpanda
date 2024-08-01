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
#include "thirdparty/openssl/evp.h"

#include <type_traits>

namespace crypto {
class digest_ctx::impl {
public:
    explicit impl(digest_type type)
      : _type(type)
      , _md_ctx(EVP_MD_CTX_new()) {
        if (!_md_ctx) {
            throw internal::ossl_error("Failed to allocate new EVP_MD_CTX");
        }

        if (
          1
          != EVP_DigestInit_ex(
            _md_ctx.get(), internal::get_md(type), nullptr)) {
            throw internal::ossl_error(fmt::format(
              "Failed to initialize digest operation for {}", type));
        }
    }

    void update(bytes_view msg) {
        if (1 != EVP_DigestUpdate(_md_ctx.get(), msg.data(), msg.size())) {
            throw internal::ossl_error("Failed to update digest operation");
        }
    }

    size_t size() const { return size(EVP_MD_CTX_get0_md(_md_ctx.get())); }

    static size_t size(const EVP_MD* md) { return EVP_MD_get_size(md); }

    bytes_span<> finish(bytes_span<> digest) {
        auto len = size();
        if (digest.size() != len) {
            throw exception(fmt::format(
              "Invalid digest buffer length: {} != {}", digest.size(), len));
        }

        return finish_no_check(digest);
    }

    bytes finish() {
        bytes digest(bytes::initialized_later(), size());
        finish(digest);
        return digest;
    }

    bytes reset() {
        bytes digest(bytes::initialized_later(), size());
        reset(digest);
        return digest;
    }

    bytes_span<> reset(bytes_span<> digest) {
        auto len = size();
        if (digest.size() != len) {
            throw exception(fmt::format(
              "Invalid digest buffer length: {} != {}", digest.size(), len));
        }

        finish_no_check(digest);
        if (1 != EVP_DigestInit_ex(_md_ctx.get(), nullptr, nullptr)) {
            throw internal::ossl_error("Failed to reset MD context");
        }

        return digest;
    }

    digest_type get_type() const { return _type; }

private:
    digest_type _type;
    internal::EVP_MD_CTX_ptr _md_ctx;

    bytes_span<> finish_no_check(bytes_span<> digest) {
        unsigned int len = digest.size();
        if (1 != EVP_DigestFinal_ex(_md_ctx.get(), digest.data(), &len)) {
            throw internal::ossl_error("Failed to finalize digest operation");
        }
        return digest;
    }
};

digest_ctx::digest_ctx(digest_type type)
  : _impl(std::make_unique<impl>(type)) {}

digest_ctx::~digest_ctx() noexcept = default;
digest_ctx::digest_ctx(digest_ctx&&) noexcept = default;
digest_ctx& digest_ctx::operator=(digest_ctx&&) noexcept = default;

static_assert(
  std::is_nothrow_move_constructible_v<digest_ctx>,
  "digest_ctx should be nothrow move constructible");
static_assert(
  std::is_nothrow_move_assignable_v<digest_ctx>,
  "digest_ctx should be nothrow move assignable");

size_t digest_ctx::size() const { return _impl->size(); }
size_t digest_ctx::size(digest_type type) {
    return impl::size(internal::get_md(type));
}

digest_ctx& digest_ctx::update(bytes_view msg) {
    _impl->update(msg);
    return *this;
}

digest_ctx& digest_ctx::update(std::string_view msg) {
    return update(internal::string_view_to_bytes_view(msg));
}

bytes digest_ctx::final() && { return _impl->finish(); }
bytes_span<> digest_ctx::final(bytes_span<> digest) && {
    return _impl->finish(digest);
}

std::span<char> digest_ctx::final(std::span<char> digest) && {
    _impl->finish(internal::char_span_to_bytes_span(digest));
    return digest;
}

bytes digest_ctx::reset() { return _impl->reset(); };
bytes_span<> digest_ctx::reset(bytes_span<> digest) {
    return _impl->reset(digest);
}

std::span<char> digest_ctx::reset(std::span<char> digest) {
    _impl->reset(internal::char_span_to_bytes_span(digest));
    return digest;
}

bytes digest(digest_type type, bytes_view msg) {
    digest_ctx ctx(type);
    ctx.update(msg);
    return std::move(ctx).final();
}

bytes digest(digest_type type, std::string_view msg) {
    return digest(type, internal::string_view_to_bytes_view(msg));
}

} // namespace crypto
