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

#include <memory>

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
    digest_ctx(digest_ctx&&) noexcept;
    digest_ctx& operator=(digest_ctx&&) noexcept;

    /**
     * @return size_t Size of digest
     */
    size_t size() const;

    static size_t size(digest_type type);

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

    /**
     * Finishes digest operation and resets context so it can be reused
     *
     * @return bytes The digest
     * @throws crypto::exception On internal error
     */
    bytes reset();

    /**
     * Finishes digest operation and resets context so it can be used again
     *
     * @param digest The buffer to place the digest into.  It must be exactly
     * the size of the digest
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p digest is an invalid
     * size
     */
    bytes_span<> reset(bytes_span<> digest);
    std::span<char> reset(std::span<char> digest);

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
} // namespace crypto
