/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/format_to.h"
#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/net/tls.hh>
namespace net {

extern const std::string_view tls_v1_2_cipher_suites;
extern const std::string_view tls_v1_3_cipher_suites;
extern const std::string_view tls_v1_3_cipher_suites_strict;

/**
 * Either a path to certificate file or the certificate content itself
 * in PEM format.
 */
using certificate = std::variant<std::filesystem::path, ss::sstring>;
fmt::iterator format_cert(fmt::iterator, const net::certificate&);

/**
 * Path to key/cert files
 */
struct key_cert_path {
    std::filesystem::path key;
    std::filesystem::path cert;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool
    operator==(const key_cert_path&, const key_cert_path&) = default;
};
/**
 * Key/cert pair content in PEM format
 */
struct key_cert {
    ss::sstring key;
    ss::sstring cert;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(const key_cert&, const key_cert&) = default;
};

/**
 * PKCS#12 container with certificate and private key
 */
struct pkcs12 {
    certificate cert;
    ss::sstring password;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(const pkcs12&, const pkcs12&) = default;
};

/**
 * Keystore can be either a path or content of key/cert files or a PKCS#12
 * container
 */
using key_store = std::variant<key_cert, key_cert_path, pkcs12>;
fmt::iterator format_keystore(fmt::iterator, const net::key_store&);

/**
 * TLS credentials configuration, the configuration is used to create
 * credentials builder
 */
struct credentials_configuration {
    std::optional<certificate> truststore;
    std::optional<key_store> k_store;
    std::optional<certificate> crl;
    ss::tls::tls_version min_tls_version;
    bool enable_renegotiation = false;
    bool require_client_auth = false;
    std::optional<ss::sstring> tls_v1_2_cipher_suites;
    std::optional<ss::sstring> tls_v1_3_cipher_suites;
    fmt::iterator format_to(fmt::iterator it) const;
};

/**
 * Returns credentials builder based on the provided configuration
 */
ss::future<ss::tls::credentials_builder>
get_credentials_builder(credentials_configuration cfg);

/// Find CA trust file using the predefined set of locations
///
/// Historically, different linux distributions use different locations to
/// store certificates for their private key infrastructure. This is just a
/// convention and can't be queried by the application code. The application
/// is required to 'know' where to find the certs. In case of OpenSSL the
/// location is configured during build time. It depend on distribution on
/// which OpenSSL is built. This approach doesn't work for Redpanda because
/// single Redpanda binary can be executed on any linux distro. So the default
/// option only work on some distributions. The rest require the location to
/// be explicitly specified. This function does different thing. It probes
/// the set of default locations for different distributions untill it finds
/// the one that exists. This path is then passed to OpenSSL.
ss::future<std::optional<ss::sstring>> find_ca_file();

} // namespace net
