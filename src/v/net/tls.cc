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

#include "net/tls.h"

#include "base/vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <array>
namespace net {

static ss::logger tlslog("net_tls");

ss::future<std::optional<ss::sstring>> find_ca_file() {
    // list of all possible ca-cert file locations on different linux distros
    static constexpr std::array<std::string_view, 6> ca_cert_locations = {{
      "/etc/ssl/certs/ca-certificates.crt",
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
      "/etc/ssl/cert.pem",
      "/etc/ssl/ca-bundle.pem",
      "/etc/pki/tls/cacert.pem",
    }};

    for (auto ca_loc : ca_cert_locations) {
        if (co_await ss::file_exists(ca_loc)) {
            co_return ca_loc;
        }
    }
    co_return std::nullopt;
}
const std::string_view tls_v1_2_cipher_suites
  = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:"
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:"
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:"
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:"
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:"
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256";

const std::string_view tls_v1_3_cipher_suites = "TLS_AES_128_GCM_SHA256:"
                                                "TLS_AES_256_GCM_SHA384:"
                                                "TLS_CHACHA20_POLY1305_SHA256:"
                                                "TLS_AES_128_CCM_SHA256";

const std::string_view tls_v1_3_cipher_suites_strict
  = "TLS_AES_256_GCM_SHA384:"
    "TLS_CHACHA20_POLY1305_SHA256:"
    "TLS_AES_128_GCM_SHA256";

ss::future<ss::tls::credentials_builder>
get_credentials_builder(credentials_configuration cfg) {
    ss::tls::credentials_builder builder;

    builder.enable_server_precedence();
    builder.set_cipher_string(
      cfg.tls_v1_2_cipher_suites.value_or(ss::sstring{tls_v1_2_cipher_suites}));
    builder.set_ciphersuites(
      cfg.tls_v1_3_cipher_suites.value_or(ss::sstring{tls_v1_3_cipher_suites}));
    builder.set_minimum_tls_version(cfg.min_tls_version);
    builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);

    if (cfg.enable_renegotiation) {
        builder.enable_tls_renegotiation();
    }
    if (cfg.require_client_auth) {
        builder.set_client_auth(ss::tls::client_auth::REQUIRE);
    }
    vlog(tlslog.debug, "Creating builder for credentials with config: {}", cfg);

    if (cfg.truststore) {
        co_await ss::visit(
          *cfg.truststore,
          [&builder](const std::filesystem::path& path) {
              return builder.set_x509_trust_file(
                path.string(), ss::tls::x509_crt_format::PEM);
          },
          [&builder](const ss::sstring& truststore_str) {
              builder.set_x509_trust(
                truststore_str, ss::tls::x509_crt_format::PEM);
              return ss::now();
          });
    } else {
        auto ca_file = co_await find_ca_file();
        if (ca_file) {
            vlog(tlslog.info, "Found system CA trust file at {}", *ca_file);
            co_await builder.set_x509_trust_file(
              *ca_file, ss::tls::x509_crt_format::PEM);
        } else {
            vlog(
              tlslog.info, "No system CA trust file found, using system trust");
            co_await builder.set_system_trust();
        }
    }

    if (cfg.crl) {
        co_await ss::visit(
          *cfg.crl,
          [&builder](const std::filesystem::path& path) {
              return builder.set_x509_crl_file(
                path.string(), ss::tls::x509_crt_format::PEM);
          },
          [&builder](const ss::sstring& crl_str) {
              builder.set_x509_crl(crl_str, ss::tls::x509_crt_format::PEM);
              return ss::now();
          });
    }

    if (cfg.k_store) {
        co_await ss::visit(
          *cfg.k_store,
          [&builder](const key_cert_path& kc) {
              return builder.set_x509_key_file(
                kc.cert.string(),
                kc.key.string(),
                ss::tls::x509_crt_format::PEM);
          },
          [&builder](const key_cert& kc) {
              builder.set_x509_key(
                kc.cert, kc.key, ss::tls::x509_crt_format::PEM);
              return ss::now();
          },
          [&builder](const pkcs12& pkcs) {
              return ss::visit(
                pkcs.cert,
                [&builder, &pkcs](const std::filesystem::path& p) {
                    return builder.set_simple_pkcs12_file(
                      p.string(), ss::tls::x509_crt_format::PEM, pkcs.password);
                },
                [&pkcs, &builder](const ss::sstring& p12_str) {
                    builder.set_simple_pkcs12(
                      p12_str, ss::tls::x509_crt_format::PEM, pkcs.password);
                    return ss::now();
                });
          });
    }

    co_return builder;
}

fmt::iterator format_cert(fmt::iterator it, const net::certificate& cert) {
    return ss::visit(
      cert,
      [&it](const std::filesystem::path& path) {
          return fmt::format_to(it, "{{path: {}}}", path);
      },
      [&it](const ss::sstring&) {
          return fmt::format_to(it, "{{cert: <redacted>}}");
      });
}

fmt::iterator format_keystore(fmt::iterator it, const net::key_store& store) {
    return ss::visit(
      store,
      [&it](const net::key_cert& kc) {
          return fmt::format_to(it, "{{key_cert: {}}}", kc);
      },
      [&it](const net::key_cert_path& kc) {
          return fmt::format_to(it, "{{key_cert_path: {}}}", kc);
      },
      [&it](const net::pkcs12& pkcs) {
          return fmt::format_to(it, "{{pkcs12: {}}}", pkcs);
      });
}

fmt::iterator key_cert_path::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{key: {}, cert: {}}}", key.string(), cert.string());
}
fmt::iterator key_cert::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{key: <redacted>, cert: <redacted>}}");
}

fmt::iterator pkcs12::format_to(fmt::iterator it) const {
    fmt::format_to(it, "{{cert: ");
    format_cert(it, cert);
    return fmt::format_to(it, ", password: <redacted>}}");
}

fmt::iterator credentials_configuration::format_to(fmt::iterator it) const {
    fmt::format_to(it, "{{truststore: ");
    if (truststore) {
        format_cert(it, *truststore);
    } else {
        fmt::format_to(it, "null");
    }
    fmt::format_to(it, ", k_store: ");
    if (k_store) {
        format_keystore(it, *k_store);
    } else {
        fmt::format_to(it, "null");
    }
    fmt::format_to(it, ", crl: ");
    if (crl) {
        format_cert(it, *crl);
    } else {
        fmt::format_to(it, "null");
    }
    return fmt::format_to(
      it,
      ", min_tls_version: {}, enable_renegotiation: {}, "
      "require_client_auth: {}}}",
      min_tls_version,
      enable_renegotiation,
      require_client_auth);
}

} // namespace net
