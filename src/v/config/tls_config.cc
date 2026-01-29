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

#include "tls_config.h"

#include "config/configuration.h"
#include "config/convert.h"
#include "net/tls.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/variant_utils.hh>

#include <openssl/err.h>
#include <openssl/ssl.h>

namespace config {
namespace {
net::key_store create_key_store(const key_cert_container& container) {
    return ss::visit(
      container,
      [](const key_cert& kc) {
          return net::key_store{net::key_cert_path{
            .key = std::filesystem::path(kc.key_file),
            .cert = std::filesystem::path(kc.cert_file),
          }};
      },
      [](const p12_container& pkcs) {
          return net::key_store{net::pkcs12{
            .cert = std::filesystem::path(pkcs.p12_path),
            .password = pkcs.p12_password,
          }};
      });
}

template<typename T, auto fn>
struct ssl_deleter {
    void operator()(T* ptr) { fn(ptr); }
};

template<typename T, auto fn>
using ssl_handle = std::unique_ptr<T, ssl_deleter<T, fn>>;

using ssl_ctx_ptr = ssl_handle<SSL_CTX, SSL_CTX_free>;

bool ssl_clean_room(auto func) {
    auto cleanup = ss::defer([] { ERR_clear_error(); });
    ssl_ctx_ptr ctx{SSL_CTX_new(TLS_method())};
    return ctx && func(ctx.get());
}
} // namespace

ss::future<std::optional<ss::tls::credentials_builder>>
tls_config::get_credentials_builder() const& {
    if (_enabled) {
        const auto& cfg = config::shard_local_cfg();
        auto builder = co_await net::get_credentials_builder({
          .truststore = _truststore_file.transform(
            [](auto& f) { return net::certificate(std::filesystem::path(f)); }),
          .k_store = _key_cert.transform(create_key_store),
          .crl = _crl_file.transform(
            [](auto& f) { return net::certificate(std::filesystem::path(f)); }),
          .min_tls_version = from_config(
            _min_tls_version.value_or(cfg.tls_min_version())),
          .enable_renegotiation = _enable_renegotiation.value_or(
            cfg.tls_enable_renegotiation()),
          .require_client_auth = _require_client_auth,
          .tls_v1_2_cipher_suites = _tls_v1_2_cipher_suites.value_or(
            cfg.tls_v1_2_cipher_suites()),
          .tls_v1_3_cipher_suites = _tls_v1_3_cipher_suites.value_or(
            cfg.tls_v1_3_cipher_suites()),
        });
        co_return builder;
    }

    co_return std::nullopt;
}

ss::future<std::optional<ss::tls::credentials_builder>>
tls_config::get_credentials_builder() && {
    auto ptr = ss::make_lw_shared(std::move(*this));
    return ptr->get_credentials_builder().finally([ptr] {});
}

std::optional<ss::sstring> tls_config::validate(const tls_config& c) {
    const auto contains_p12_file = [&c]() {
        if (c.get_key_cert_files()) {
            return std::holds_alternative<config::p12_container>(
              (*c.get_key_cert_files()));
        }
        return false;
    };
    if (
      c.get_require_client_auth() && !c.get_truststore_file()
      && !contains_p12_file()) {
        return "Trust store or P12 file is required when client authentication "
               "is enabled";
    }

    return std::nullopt;
}

std::ostream& operator<<(std::ostream& o, const config::p12_container& p) {
    fmt::print(o, "{{ p12 file: {}, p12 password: REDACTED }}", p.p12_path);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config::key_cert& c) {
    o << "{ "
      << "key_file: " << c.key_file << " "
      << "cert_file: " << c.cert_file << " }";
    return o;
}

std::ostream& operator<<(std::ostream& o, const config::tls_config& c) {
    o << "{ "
      << "enabled: " << c.is_enabled() << " "
      << "key/cert files: " << c.get_key_cert_files() << " "
      << "ca file: " << c.get_truststore_file() << " "
      << "crl file: " << c.get_crl_file() << " "
      << "client_auth_required: " << c.get_require_client_auth() << ""
      << " }";
    return o;
}

bool validate_tls_v1_2_cipher_suites(const ss::sstring& s) {
    return ssl_clean_room(
      [&](auto ctx) { return SSL_CTX_set_cipher_list(ctx, s.data()) == 1; });
}

bool validate_tls_v1_3_cipher_suites(const ss::sstring& s) {
    return ssl_clean_room(
      [&](auto ctx) { return SSL_CTX_set_ciphersuites(ctx, s.data()) == 1; });
}

} // namespace config

namespace YAML {

inline ss::sstring to_absolute(const ss::sstring& path) {
    namespace fs = std::filesystem;
    if (path.empty()) {
        return path;
    }
    return fs::absolute(fs::path(path)).native();
}

inline std::optional<ss::sstring>
to_absolute(const std::optional<ss::sstring>& path) {
    if (path) {
        return to_absolute(*path);
    }
    return std::nullopt;
}

Node convert<config::tls_config>::encode(const config::tls_config& rhs) {
    Node node;

    node["enabled"] = rhs.is_enabled();
    node["require_client_auth"] = rhs.get_require_client_auth();

    if (rhs.get_key_cert_files()) {
        ss::visit(
          rhs.get_key_cert_files().value(),
          [&node](const config::key_cert& c) {
              node["cert_file"] = c.cert_file;
              node["key_file"] = c.key_file;
          },
          [&node](const config::p12_container& p12) {
              node["p12_file"] = p12.p12_path;
              node["p12_password"] = "REDACTED";
          });
    }

    if (rhs.get_truststore_file()) {
        node["truststore_file"] = *rhs.get_truststore_file();
    }

    if (rhs.get_tls_v1_2_cipher_suites()) {
        node["tls_v1_2_cipher_suites"] = *rhs.get_tls_v1_2_cipher_suites();
    }

    if (rhs.get_tls_v1_3_cipher_suites()) {
        node["tls_v1_3_cipher_suites"] = *rhs.get_tls_v1_3_cipher_suites();
    }

    if (rhs.get_min_tls_version()) {
        node["min_tls_version"] = *rhs.get_min_tls_version();
    }

    if (rhs.get_enable_renegotiation()) {
        node["enable_renegotiation"] = *rhs.get_enable_renegotiation();
    }

    return node;
}

std::optional<ss::sstring> convert<config::tls_config>::read_optional(
  const Node& node, const ss::sstring& key) {
    if (node[key]) {
        return node[key].as<ss::sstring>();
    }
    return std::nullopt;
}

bool convert<config::tls_config>::decode(
  const Node& node, config::tls_config& rhs) {
    // either both true or both false
    if (
      static_cast<bool>(node["key_file"])
      ^ static_cast<bool>(node["cert_file"])) {
        return false;
    }

    // Must have either both or neither for PKCS#12 files
    if (
      static_cast<bool>(node["p12_file"])
      ^ static_cast<bool>(node["p12_password"])) {
        return false;
    }

    // Cannot have both key/cert and P12 file
    if (
      static_cast<bool>(node["key_file"])
      && static_cast<bool>(node["p12_file"])) {
        return false;
    }
    auto enabled = node["enabled"] && node["enabled"].as<bool>();
    if (!enabled) {
        rhs = config::tls_config(
          false, std::nullopt, std::nullopt, std::nullopt, false);
    } else {
        std::optional<config::key_cert_container> container;
        if (node["key_file"]) {
            container.emplace(
              config::key_cert{
                to_absolute(node["key_file"].as<ss::sstring>()),
                to_absolute(node["cert_file"].as<ss::sstring>())});
        } else if (node["p12_file"]) {
            container.emplace(
              config::p12_container{
                to_absolute(node["p12_file"].as<ss::sstring>()),
                node["p12_password"].as<ss::sstring>()});
        }
        rhs = config::tls_config(
          enabled,
          container,
          to_absolute(read_optional(node, "truststore_file")),
          to_absolute(read_optional(node, "crl_file")),
          node["require_client_auth"] && node["require_client_auth"].as<bool>(),
          read_optional(node, "tls_v1_2_cipher_suites"),
          read_optional(node, "tls_v1_3_cipher_suites"),
          node["min_tls_version"]
            ? node["min_tls_version"].as<config::tls_version>()
            : std::optional<config::tls_version>(),
          node["enable_renegotiation"] ? node["enable_renegotiation"].as<bool>()
                                       : std::optional<bool>());
    }
    return true;
}

} // namespace YAML
