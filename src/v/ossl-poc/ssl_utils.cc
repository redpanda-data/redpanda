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

#include "ssl_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <openssl/pem.h>
#include <openssl/x509.h>

namespace {
EVP_PKEY_ptr
load_evp_pkey(const ss::temporary_buffer<char>& buf, OSSL_LIB_CTX*) {
    BIO_ptr key_bio(BIO_new_mem_buf(buf.get(), buf.size()));
    auto pkey_temp = EVP_PKEY_new();
    if (
      nullptr
      == PEM_read_bio_PrivateKey(key_bio.get(), &pkey_temp, nullptr, nullptr)) {
        EVP_PKEY_free(pkey_temp);
        throw ossl_error();
    }
    return EVP_PKEY_ptr(pkey_temp);
}

X509_ptr
load_x509(const ss::temporary_buffer<char>& buf, OSSL_LIB_CTX* libctx) {
    BIO_ptr cert_bio(BIO_new_mem_buf(buf.get(), buf.size()));
    auto x509_temp = X509_new_ex(
      libctx, nullptr); // X509_new_ex(nullptr, nullptr);
    if (
      nullptr
      == PEM_read_bio_X509(cert_bio.get(), &x509_temp, nullptr, nullptr)) {
        X509_free(x509_temp);
        throw ossl_error();
    }

    return X509_ptr(x509_temp);
}

X509_CRL_ptr
load_crl(const ss::temporary_buffer<char>& buf, OSSL_LIB_CTX* libctx) {
    BIO_ptr crl_bio(BIO_new_mem_buf(buf.get(), buf.size()));
    auto crl_temp = X509_CRL_new_ex(libctx, nullptr);
    if (
      nullptr
      == PEM_read_bio_X509_CRL(crl_bio.get(), &crl_temp, nullptr, nullptr)) {
        try {
            throw ossl_error();
        } catch (...) {
            // ignore the error but drain the error pipe
        }
        BIO_seek(crl_bio.get(), 0);
        if (nullptr == d2i_X509_CRL_bio(crl_bio.get(), &crl_temp)) {
            throw ossl_error("Failed to read X509 CRL");
        }
    }

    return X509_CRL_ptr(crl_temp);
}

ss::future<ss::temporary_buffer<char>>
read_file_contents(const ss::sstring& path) {
    auto fd = co_await ss::open_file_dma(path, ss::open_flags::ro);
    auto file_size = co_await fd.size();
    co_return co_await fd.dma_read<char>(0, file_size);
}
} // namespace

ss::future<EVP_PKEY_ptr>
load_evp_pkey_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx) {
    try {
        co_return load_evp_pkey(co_await read_file_contents(file_name), libctx);
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load key from " + file_name + ": " + e.what());
    }
}
ss::future<X509_ptr>
load_x509_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx) {
    try {
        co_return load_x509(co_await read_file_contents(file_name), libctx);
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load X.509 certificate from " + file_name + ": "
          + e.what());
    }
}

ss::future<X509_STACK_ptr>
load_cert_chain_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx) {
    try {
        auto contents = co_await read_file_contents(file_name);
        auto stack = X509_STACK_ptr(sk_X509_new_null());
        BIO_ptr chain_bio(BIO_new_mem_buf(contents.get(), contents.size()));
        while (true) {
            X509* cert = X509_new_ex(libctx, nullptr);
            if (!PEM_read_bio_X509(chain_bio.get(), &cert, nullptr, nullptr)) {
                // Free the unused certificate
                X509_free(cert);
                break;
            }
            sk_X509_push(stack.get(), cert);
        }

        co_return stack;
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load X.509 stack from " + file_name + ": " + e.what());
    }
}

ss::future<X509_CRL_ptr>
load_crl_from_file(const ss::sstring& file_name, OSSL_LIB_CTX* libctx) {
    try {
        co_return load_crl(co_await read_file_contents(file_name), libctx);
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load CRL from " + file_name + ": " + e.what());
    }
}
