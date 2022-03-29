/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "bytes/bytes.h"

#include <gnutls/crypto.h>
#include <gnutls/gnutls.h>

class hmac_exception final : public std::exception {
public:
    explicit hmac_exception(const char* msg)
      : _msg(msg) {}

    const char* what() const noexcept final { return _msg; }

private:
    const char* _msg;
};

class hash_exception final : public std::exception {
public:
    explicit hash_exception(const char* msg)
      : _msg(msg) {}

    const char* what() const noexcept final { return _msg; }

private:
    const char* _msg;
};

namespace internal {

template<gnutls_mac_algorithm_t Algo, size_t DigestSize>
class hmac {
    static_assert(DigestSize > 0, "digest cannot be zero length");

public:
    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    explicit hmac(std::string_view key)
      : hmac(key.data(), key.size()) {}

    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    explicit hmac(bytes_view key)
      : hmac(key.data(), key.size()) {}

    hmac(const hmac&) = delete;
    hmac& operator=(const hmac&) = delete;
    hmac(hmac&&) = delete;
    hmac& operator=(hmac&&) = delete;

    ~hmac() noexcept { gnutls_hmac_deinit(_handle, nullptr); }

    void update(std::string_view data) { update(data.data(), data.size()); }
    void update(bytes_view data) { update(data.data(), data.size()); }

    template<std::size_t Size>
    void update(const std::array<char, Size>& data) {
        update(data.data(), Size);
    }

    /**
     * Return the current output and reset.
     */
    std::array<char, DigestSize> reset() {
        std::array<char, DigestSize> digest;
        gnutls_hmac_output(_handle, digest.data());
        return digest;
    }

private:
    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    hmac(const void* key, size_t size) {
        int ret = gnutls_hmac_init(&_handle, Algo, key, size);
        if (unlikely(ret)) {
            throw hmac_exception(gnutls_strerror(ret));
        }

        ret = gnutls_hmac_get_len(Algo);
        if (unlikely(ret != DigestSize)) {
            throw hmac_exception("invalid digest length");
        }
    }

    void update(const void* data, size_t size) {
        int ret = gnutls_hmac(_handle, data, size);
        if (unlikely(ret)) {
            throw hmac_exception(gnutls_strerror(ret));
        }
    }

    gnutls_hmac_hd_t _handle;
};

template<gnutls_digest_algorithm_t Algo, size_t DigestSize>
class hash {
    static_assert(DigestSize > 0, "digest cannot be zero length");

public:
    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    hash() {
        int ret = gnutls_hash_init(&_handle, Algo);
        if (unlikely(ret)) {
            throw hash_exception(gnutls_strerror(ret));
        }

        ret = gnutls_hash_get_len(Algo);
        if (unlikely(ret != DigestSize)) {
            throw hash_exception("invalid digest length");
        }
    }

    hash(const hash&) = delete;
    hash& operator=(const hash&) = delete;
    hash(hash&&) = delete;
    hash& operator=(hash&&) = delete;

    ~hash() noexcept { gnutls_hash_deinit(_handle, nullptr); }

    void update(std::string_view data) { update(data.data(), data.size()); }
    void update(bytes_view data) { update(data.data(), data.size()); }

    /**
     * Return the current output and reset.
     */
    std::array<char, DigestSize> reset() {
        std::array<char, DigestSize> digest;
        gnutls_hash_output(_handle, digest.data());
        return digest;
    }

private:
    void update(const void* data, size_t size) {
        int ret = gnutls_hash(_handle, data, size);
        if (unlikely(ret)) {
            throw hash_exception(gnutls_strerror(ret));
        }
    }

    gnutls_hash_hd_t _handle;
};

} // namespace internal

using hmac_sha256 = internal::hmac<GNUTLS_MAC_SHA256, 32>; // NOLINT
using hmac_sha512 = internal::hmac<GNUTLS_MAC_SHA512, 64>; // NOLINT

using hash_sha256 = internal::hash<GNUTLS_DIG_SHA256, 32>; // NOLINT
using hash_sha512 = internal::hash<GNUTLS_DIG_SHA512, 64>; // NOLINT
