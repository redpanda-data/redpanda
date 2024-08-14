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
#include "crypto/crypto.h"

namespace internal {

/**
 * Creates a context that can be used to perform an HMAC operation
 *
 * @tparam Algo The algorithm type
 * @tparam DigestSize The expected size of the digest
 * @note To maintainers, do not create a new exception class that inherits from
 * `crypto::exception`.  There are other exceptions within the crypto library
 * that inherit from `crypto::exception` that are hidden from users.
 */
template<crypto::digest_type Algo, size_t DigestSize>
class hmac {
    static_assert(DigestSize > 0, "digest cannot be zero length");

public:
    /**
     * Construct a new hmac object
     *
     * @param key The key to use in the HMAC operation
     * @throws crypto::exception If DigestSize is invalid or internal error with
     * crypto lib
     */
    explicit hmac(std::string_view key)
      : _ctx(Algo, key) {
        validate_hmac_len();
    }

    explicit hmac(bytes_view key)
      : _ctx(Algo, key) {
        validate_hmac_len();
    }

    hmac(const hmac&) = delete;
    hmac& operator=(const hmac&) = delete;
    hmac(hmac&&) = delete;
    hmac& operator=(hmac&&) = delete;

    ~hmac() noexcept = default;

    /**
     * Updates the HMAC operation with the supplied data
     *
     * @throws crypto::exception If there is an internal error with crypto lib
     */
    void update(std::string_view data) { _ctx.update(data); }
    void update(bytes_view data) { _ctx.update(data); }

    template<std::size_t Size>
    void update(const std::array<char, Size>& data) {
        _ctx.update({data.data(), data.size()});
    }

    /**
     * Return the signature and resets the context so it can be used again
     * @throws crypto::exception If there is an internal error with crypto lib
     */
    std::array<char, DigestSize> reset() {
        std::array<char, DigestSize> digest;
        _ctx.reset(digest);
        return digest;
    }

private:
    void validate_hmac_len() {
        auto len = _ctx.size();
        if (unlikely(len != DigestSize)) {
            throw crypto::exception("invalid digest length");
        }
    }

    crypto::hmac_ctx _ctx;
};

/**
 * Creates a context that can be used to perform digest operations
 *
 * @tparam Algo The algorithm to perform
 * @tparam DigestSize The expected digest size
 * @note To maintainers, do not create a new exception class that inherits from
 * `crypto::exception`.  There are other exceptions within the crypto library
 * that inherit from `crypto::exception` that are hidden from users.
 */
template<crypto::digest_type Algo, size_t DigestSize>
class hash {
    static_assert(DigestSize > 0, "digest cannot be zero length");

public:
    static constexpr auto digest_size = DigestSize;
    using digest_type = std::array<char, DigestSize>;

    /**
     * Construct a new hash object
     *
     * @throws crypto::exception If DigestSize is not correct or if there is an
     * internal error with crypto lib
     */
    hash()
      : _ctx(Algo) {
        auto len = _ctx.size();

        if (unlikely(len != DigestSize)) {
            throw crypto::exception("invalid digest length");
        }
    }

    hash(const hash&) = delete;
    hash& operator=(const hash&) = delete;
    hash(hash&&) = delete;
    hash& operator=(hash&&) = delete;

    ~hash() noexcept = default;

    /**
     * Updates the hash operation with the provided message
     *
     * @throws crypto::exception IF there is an internal error with crypto lib
     */
    void update(std::string_view data) { _ctx.update(data); }
    void update(bytes_view data) { _ctx.update(data); }

    /**
     * Return the digest of the data and resets the context so it can be used
     * again
     * @throws crypto::exception On internal error with crypto lib
     */
    digest_type reset() {
        std::array<char, DigestSize> digest;
        _ctx.reset(digest);
        return digest;
    }

private:
    crypto::digest_ctx _ctx;
};

} // namespace internal

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
using hmac_sha256 = internal::hmac<crypto::digest_type::SHA256, 32>;
using hmac_sha512 = internal::hmac<crypto::digest_type::SHA512, 64>;
using hash_sha256 = internal::hash<crypto::digest_type::SHA256, 32>;
using hash_sha512 = internal::hash<crypto::digest_type::SHA512, 64>;
using hash_md5 = internal::hash<crypto::digest_type::MD5, 16>;
// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
