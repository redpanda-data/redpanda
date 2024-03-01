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

#pragma once

#include "bytes/bytes.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"

namespace crypto {
///////////////////////////////////////////////////////////////////////////////
/// Digest operations
///////////////////////////////////////////////////////////////////////////////

/**
 * Context structure used to perform digest operations
 */
struct digest_ctx final {
public:
    /**
     * Constructs new digest context with specified type
     * @throws crypto::exception For implementation error
     */
    explicit digest_ctx(digest_type type);
    ~digest_ctx() noexcept;
    digest_ctx(const digest_ctx&) = delete;
    digest_ctx& operator=(const digest_ctx&) = delete;
    digest_ctx(digest_ctx&&) = default;
    digest_ctx& operator=(digest_ctx&&) = default;

    /**
     * @return size_t Length of digest
     */
    size_t len() const;

    static size_t len(digest_type type);

    /**
     * Updates digest operation with message
     *
     * @return digest_ctx& Itself for update chaining
     * @throws crypto::exception On internal error
     */
    digest_ctx& update(bytes_view msg);
    digest_ctx& update(std::string_view msg);

    /**
     * Finalizes digest operation and returns the digest
     *
     * @return bytes The digest
     * @throws crypto::exception On internal error
     */
    bytes final() &&;

    /**
     * Finalizes digest operation and returns the digest in the provided buffer
     *
     * @param digest The buffer to place the digest into.  It must be exactly
     * the size of the digest
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p digest is an invalid
     * size
     */
    bytes_span<> final(bytes_span<> digest) &&;
    std::span<char> final(std::span<char> digest) &&;

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

/**
 * Performs one-shot digest operation
 *
 * @param type The type to produce
 * @param msg The message to digest
 * @return bytes The resulting digest
 * @throws crypto::exception On internal error
 */
bytes digest(digest_type type, bytes_view msg);
bytes digest(digest_type type, std::string_view msg);

///////////////////////////////////////////////////////////////////////////////
/// MAC operations
///////////////////////////////////////////////////////////////////////////////

/**
 * Context structure used to perform HMAC operations
 */
struct hmac_ctx final {
public:
    /**
     * Construct a new hmac ctx object
     *
     * @param type The type of HMAC to generate
     * @param key The key to use
     * @throws crypto::exception On internal error
     */
    hmac_ctx(digest_type type, bytes_view key);
    hmac_ctx(digest_type type, std::string_view key);
    ~hmac_ctx() noexcept;
    hmac_ctx(const hmac_ctx&) = delete;
    hmac_ctx& operator=(const hmac_ctx&) = delete;
    hmac_ctx(hmac_ctx&&) = default;
    hmac_ctx& operator=(hmac_ctx&&) = default;

    /**
     * @return size_t Length of the resulting HMAC signature
     */
    size_t len() const;
    static size_t len(digest_type type);

    /**
     * Updates HMAC operation
     *
     * @return hmac_ctx& Itself for update chaining
     * @throws crypto::exception On internal error
     */
    hmac_ctx& update(bytes_view msg);
    hmac_ctx& update(std::string_view msg);

    /**
     * Finalies HMAC operation and returns signature
     *
     * @return bytes The signature
     * crypto::exception On internal error
     */
    bytes final() &&;

    /**
     * Finalizes digest operation and returns the signature in the provided
     * buffer
     *
     * @param signature The buffer to place the signature into.  It must be
     * exactly the size of the signature
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p signature is an
     * invalid size
     */
    bytes_span<> final(bytes_span<> signature) &&;
    std::span<char> final(std::span<char> signature) &&;

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

/**
 * Performs one-shot digest operation
 *
 * @param type The type of digest to create
 * @param key The key to use
 * @param msg The message
 * @return bytes The signature
 * @throws crypto::exception On internal error
 */
bytes hmac(digest_type type, bytes_view key, bytes_view msg);
bytes hmac(digest_type type, std::string_view key, std::string_view msg);
} // namespace crypto
