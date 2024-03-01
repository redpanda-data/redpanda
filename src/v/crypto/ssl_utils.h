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

#include "base/seastarx.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"

#include <seastar/core/sstring.hh>

#include <openssl/evp.h>

#include <memory>
#include <stdexcept>

namespace crypto::internal {

inline const EVP_MD* get_md(digest_type type) {
    switch (type) {
    case digest_type::MD5:
        return EVP_md5();
    case digest_type::SHA256:
        return EVP_sha256();
    case digest_type::SHA512:
        return EVP_sha512();
    }
}

template<typename T, void (*fn)(T*)>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};

template<typename T, void (*fn)(T*)>
using handle = std::unique_ptr<T, deleter<T, fn>>;

using EVP_MAC_ptr = handle<EVP_MAC, EVP_MAC_free>;
using EVP_MAC_CTX_ptr = handle<EVP_MAC_CTX, EVP_MAC_CTX_free>;
using EVP_MD_CTX_ptr = handle<EVP_MD_CTX, EVP_MD_CTX_free>;

/// Exception class used to extract the error from OpenSSL
class ossl_error final : public exception {
public:
    ossl_error()
      : exception(build_error()) {}

    explicit ossl_error(const std::string& msg)
      : exception(msg + ": " + build_error()) {}

    static std::string build_error();
};
} // namespace crypto::internal
