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
#include "crypto/crypto.h"
#include "crypto/types.h"
#include "ssl_utils.h"

namespace crypto {
class key::impl {
private:
    struct _private {};

public:
    impl(
      _private p, internal::EVP_PKEY_ptr pkey, is_private_key_t is_private_key);
    ~impl() noexcept;
    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) = default;
    impl& operator=(impl&&) = default;
    static std::unique_ptr<impl>
    load_pem_key(bytes_view key, is_private_key_t is_private_key);
    static std::unique_ptr<impl>
    load_der_key(bytes_view key, is_private_key_t is_private_key);
    static std::unique_ptr<impl>
    load_rsa_public_key(bytes_view n, bytes_view e);

    key_type get_key_type() const;
    is_private_key_t is_private_key() const { return _is_private_key; }

    EVP_PKEY* get_pkey() const { return _pkey.get(); }

private:
    internal::EVP_PKEY_ptr _pkey;
    is_private_key_t _is_private_key;
};
} // namespace crypto
