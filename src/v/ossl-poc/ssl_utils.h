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

#include "base/seastarx.h"

#include <seastar/core/future.hh>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <memory>

#pragma once

using BIO_ptr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using OSSL_PROVIDER_ptr
  = std::unique_ptr<OSSL_PROVIDER, decltype(&OSSL_PROVIDER_unload)>;
using EVP_CIPHER_ptr = std::unique_ptr<EVP_CIPHER, decltype(&EVP_CIPHER_free)>;
using EVP_CIPHER_CTX_ptr
  = std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>;
using EVP_PKEY_ptr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using SSL_CTX_ptr = std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)>;
using SSL_ptr = std::unique_ptr<SSL, decltype(&SSL_free)>;
using X509_ptr = std::unique_ptr<X509, decltype(&X509_free)>;

/// Used to construct an exception holding OpenSSL error information
class ossl_error : public std::runtime_error {
public:
    ossl_error()
      : std::runtime_error(build_error()) {}

    explicit ossl_error(const ss::sstring& msg)
      : std::runtime_error(msg + ": " + build_error()) {}

private:
    static ss::sstring build_error() {
        ss::sstring msg = "{";
        std::array<char, 256> buf{};
        for (auto code = ERR_get_error(); code != 0; code = ERR_get_error()) {
            ERR_error_string_n(code, buf.data(), buf.size());
            msg += fmt::format("{{{}: {}}}", code, buf.data());
        }
        msg += "}";

        return msg;
    }
};

ss::future<EVP_PKEY_ptr> load_evp_pkey_from_file(const ss::sstring& file_name);
ss::future<X509_ptr> load_x509_from_file(const ss::sstring& file_name);
