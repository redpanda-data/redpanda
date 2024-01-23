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

namespace {
EVP_PKEY_ptr load_evp_pkey(const ss::temporary_buffer<char>& buf) {
    BIO_ptr key_bio(BIO_new_mem_buf(buf.get(), buf.size()), BIO_free);
    auto pkey_temp = EVP_PKEY_new();
    if (
      nullptr
      == PEM_read_bio_PrivateKey(key_bio.get(), &pkey_temp, nullptr, nullptr)) {
        EVP_PKEY_free(pkey_temp);
        throw ossl_error();
    }
    return {pkey_temp, EVP_PKEY_free};
}

X509_ptr load_x509(const ss::temporary_buffer<char>& buf) {
    BIO_ptr cert_bio(BIO_new_mem_buf(buf.get(), buf.size()), BIO_free);
    auto x509_temp = X509_new();
    if (
      nullptr
      == PEM_read_bio_X509(cert_bio.get(), &x509_temp, nullptr, nullptr)) {
        X509_free(x509_temp);
        throw ossl_error();
    }

    return {x509_temp, X509_free};
}

ss::future<ss::temporary_buffer<char>>
read_file_contents(const ss::sstring& path) {
    auto fd = co_await ss::open_file_dma(path, ss::open_flags::ro);
    auto file_size = co_await fd.size();
    co_return co_await fd.dma_read<char>(0, file_size);
}
} // namespace

ss::future<EVP_PKEY_ptr> load_evp_pkey_from_file(const ss::sstring& file_name) {
    try {
        co_return load_evp_pkey(co_await read_file_contents(file_name));
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load key from " + file_name + ": " + e.what());
    }
}
ss::future<X509_ptr> load_x509_from_file(const ss::sstring& file_name) {
    try {
        co_return load_x509(co_await read_file_contents(file_name));
    } catch (ossl_error& e) {
        throw std::runtime_error(
          "Failed to load X.509 certificate from " + file_name + ": "
          + e.what());
    }
}
