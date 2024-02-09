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
#include <openssl/x509_vfy.h>

#include <memory>

#pragma once

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};

template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

inline void free_stack_x509(STACK_OF(X509) * stack) {
    sk_X509_pop_free(stack, X509_free);
}

inline void free_stack_x509_crl(STACK_OF(X509_CRL) * stack) {
    sk_X509_CRL_pop_free(stack, X509_CRL_free);
}

using BIO_ptr = handle<BIO, BIO_free>;
using OSSL_PROVIDER_ptr = handle<OSSL_PROVIDER, OSSL_PROVIDER_unload>;
using EVP_CIPHER_ptr = handle<EVP_CIPHER, EVP_CIPHER_free>;
using EVP_CIPHER_CTX_ptr = handle<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free>;
using EVP_PKEY_ptr = handle<EVP_PKEY, EVP_PKEY_free>;
using SSL_CTX_ptr = handle<SSL_CTX, SSL_CTX_free>;
using SSL_ptr = handle<SSL, SSL_free>;
using X509_ptr = handle<X509, X509_free>;
using X509_CRL_ptr = handle<X509_CRL, X509_CRL_free>;
using X509_CRL_STACK_ptr = handle<STACK_OF(X509_CRL), free_stack_x509_crl>;
using X509_STACK_ptr = handle<STACK_OF(X509), free_stack_x509>;
using X509_STORE_CTX_ptr = handle<X509_STORE_CTX, X509_STORE_CTX_free>;

/// Used to construct an exception holding OpenSSL error information
class ossl_error : public std::runtime_error {
public:
    ossl_error()
      : std::runtime_error(build_error()) {}

    explicit ossl_error(const ss::sstring& msg)
      : std::runtime_error(msg + ": " + build_error()) {}

    explicit ossl_error(X509_STORE_CTX* ctx)
      : std::runtime_error(build_error(ctx)) {}

    ossl_error(const ss::sstring& msg, X509_STORE_CTX* ctx)
      : std::runtime_error(msg + ": " + build_error(ctx)) {}

    static ss::sstring build_error() {
        ss::sstring msg = "{{";
        std::array<char, 256> buf{};
        for (auto code = ERR_get_error(); code != 0; code = ERR_get_error()) {
            ERR_error_string_n(code, buf.data(), buf.size());
            msg += fmt::format("{{{}: {}}}", code, buf.data());
        }
        msg += "}}";

        return msg;
    }

    static ss::sstring build_error(X509_STORE_CTX* ctx) {
        return X509_verify_cert_error_string(X509_STORE_CTX_get_error(ctx));
    }
};

ss::future<EVP_PKEY_ptr>
load_evp_pkey_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx);
ss::future<X509_ptr>
load_x509_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx);
ss::future<X509_STACK_ptr>
load_cert_chain_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx);
ss::future<X509_CRL_ptr>
load_crl_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx);
