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
    hmac_ctx(hmac_ctx&&) noexcept;
    hmac_ctx& operator=(hmac_ctx&&) noexcept;

    /**
     * @return size_t Size of the resulting HMAC signature
     */
    size_t size() const;
    static size_t size(digest_type type);

    /**
     * Updates HMAC operation
     *
     * @return hmac_ctx& Itself for update chaining
     * @throws crypto::exception On internal error
     */
    hmac_ctx& update(bytes_view msg);
    hmac_ctx& update(std::string_view msg);

    /**
     * Finalizes HMAC operation and returns signature
     *
     * @return bytes The signature
     * @throws crypto::exception On internal error
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

    /**
     * Finalizes HMAC operation and returns signature and resets context so it
     * can be used again
     *
     * @return bytes The signature
     * @throws crypto::exception On internal error
     */
    bytes reset();

    /**
     * Finalizes digest operation and returns the signature in the provided
     * buffer and resets the context so it can be used again
     *
     * @param signature The buffer to place the signature into.  It must be
     * exactly the size of the signature
     * @return The provided buffer
     * @throws crypto::exception On internal error or if @p signature is an
     * invalid size
     */
    bytes_span<> reset(bytes_span<> signature);
    std::span<char> reset(std::span<char> signature);

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

///////////////////////////////////////////////////////////////////////////////
/// Asymmetric key operations
///////////////////////////////////////////////////////////////////////////////
class verify_ctx;
/**
 * Structure that holds the key implementation
 */
struct key {
public:
    /**
     * Attempts to load a key from a buffer
     *
     * @param key The key buffer
     * @param fmt The format of the buffer
     * @param is_private_key Whether or not the key is a private key
     * @return key The loaded key
     * @throws crypto::exception If unable to load the key contained in @p key
     * or if @p key contains an unsupported key
     */
    static key
    load_key(bytes_view key, format_type fmt, is_private_key_t is_private_key);
    static key load_key(
      std::string_view key, format_type fmt, is_private_key_t is_private_key);
    /**
     * Loads an RSA public key from its parts
     *
     * @param n The modulus
     * @param e The public exponent
     * @return key The loaded key
     * @throws crypto::exception If there is an error loading the key
     */
    static key load_rsa_public_key(bytes_view n, bytes_view e);
    static key load_rsa_public_key(std::string_view n, std::string_view e);
    ~key() noexcept;
    key(const key&) = delete;
    key& operator=(const key&) = delete;
    key(key&&) noexcept;
    key& operator=(key&&) noexcept;

    key_type get_type() const;
    is_private_key_t is_private_key() const;

private:
    class impl;
    explicit key(std::unique_ptr<impl>&& impl);
    std::unique_ptr<impl> _impl;
    friend class verify_ctx;
};

/**
 * Class used to hold signature verification context
 */
class verify_ctx final {
public:
    /**
     * Constructs new verify context
     *
     * @param type The type of signature to verify
     * @param key The key type
     * @throws crypto::exception On error
     */
    verify_ctx(digest_type type, const key& key);
    ~verify_ctx() noexcept;
    verify_ctx(const verify_ctx&) = delete;
    verify_ctx& operator=(const verify_ctx&) = delete;
    verify_ctx(verify_ctx&&) noexcept;
    verify_ctx& operator=(verify_ctx&&) noexcept;

    /**
     * Updates the verify context
     *
     * @return verify_ctx& Itself for chaining
     * @throws crypto::exception On internal error
     */
    verify_ctx& update(bytes_view msg);
    verify_ctx& update(std::string_view msg);

    /**
     * Finishes verification and returns whether or not the signature has been
     * verified
     *
     * @throws crypto::exception On internal error
     */
    bool final(bytes_view sig) &&;
    bool final(std::string_view sig) &&;

    /**
     * Finishes verification, returns whether or not the signature has been
     * verified and resets the context so it can be used again
     *
     * @throws crypto::exception On internal error
     */
    bool reset(bytes_view sig);
    bool reset(std::string_view sig);

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

/**
 * Verifies the signature
 *
 * @param type The type of digest used to construct the signature
 * @param key The key to use
 * @param msg The message to verify
 * @param sig The signature to verify
 * @return true If @p sig is a valid signature for @p msg
 * @return false Otherwise
 * @throws crypto::exception On internal error
 */
bool verify_signature(
  digest_type type, const key& key, bytes_view msg, bytes_view sig);
bool verify_signature(
  digest_type type, const key& key, std::string_view msg, std::string_view sig);

///////////////////////////////////////////////////////////////////////////////
/// DRBG operation
///////////////////////////////////////////////////////////////////////////////

using use_private_rng = ss::bool_class<struct use_private_rng_tag>;

/**
 * Used to generate some random data and stick it into a provided buffer
 *
 * @param buf The buffer to place random data into
 * @param private_rng Whether or not to use the 'private' DRBG
 * @note For private vs public, please see:
 * https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html
 * @return bytes_span<> The buffer
 * @throws crypto::exception On internal error
 */
bytes_span<> generate_random(
  bytes_span<> buf, use_private_rng private_rng = use_private_rng::no);

/**
 * Generates the requested number of random bytes
 *
 * @param len The amount of data to generate
 * @param private_rng Whether or not to use the 'private' DRBG
 * @note For private vs public, please see:
 * https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html
 * @return bytes The buffer of random data
 * @throws crypto::exception On internal error
 */
bytes generate_random(
  size_t len, use_private_rng private_rng = use_private_rng::no);

/**
 * Secure RNG structure that can be used with stdlib RNG utilities
 *
 * @tparam UsePrivate Whether or not to use the private RNG
 * @tparam UIntType The return type
 */
template<bool UsePrivate, std::unsigned_integral UIntType>
struct secure_rng {
public:
    using result_type = UIntType;

    static constexpr result_type min() {
        return std::numeric_limits<result_type>::min();
    }
    static constexpr result_type max() {
        return std::numeric_limits<result_type>::max();
    }

    result_type operator()();
};

using secure_private_rng = secure_rng<true, uint32_t>;
using secure_public_rng = secure_rng<false, uint32_t>;
} // namespace crypto
