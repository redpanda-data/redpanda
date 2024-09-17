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
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/variant_utils.hh>

namespace config {
ss::future<std::optional<ss::tls::credentials_builder>>
tls_config::get_credentials_builder() const& {
    if (_enabled) {
        return ss::do_with(
          ss::tls::credentials_builder{},
          [this](ss::tls::credentials_builder& builder) {
              builder.enable_server_precedence();
              builder.set_cipher_string(
                {tlsv1_2_cipher_string.data(), tlsv1_2_cipher_string.size()});
              builder.set_ciphersuites(
                {tlsv1_3_ciphersuites.data(), tlsv1_3_ciphersuites.size()});
              builder.set_minimum_tls_version(
                from_config(config::shard_local_cfg().tls_min_version()));
              builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);
              if (_require_client_auth) {
                  builder.set_client_auth(ss::tls::client_auth::REQUIRE);
              }

              auto f = _truststore_file
                         ? builder.set_x509_trust_file(
                             *_truststore_file, ss::tls::x509_crt_format::PEM)
                         : builder.set_system_trust();

              if (_crl_file) {
                  f = f.then([this, &builder] {
                      return builder.set_x509_crl_file(
                        *_crl_file, ss::tls::x509_crt_format::PEM);
                  });
              }

              if (_key_cert) {
                  f = f.then([this, &builder] {
                      return ss::visit(
                        _key_cert.value(),
                        [&builder](const key_cert& c) {
                            return builder.set_x509_key_file(
                              c.cert_file,
                              c.key_file,
                              ss::tls::x509_crt_format::PEM);
                        },
                        [&builder](const p12_container& p) {
                            return builder.set_simple_pkcs12_file(
                              p.p12_path,
                              ss::tls::x509_crt_format::PEM,
                              p.p12_password);
                        });
                  });
              }

              return f.then([&builder]() {
                  return std::make_optional(std::move(builder));
              });
          });
    }

    return ss::make_ready_future<std::optional<ss::tls::credentials_builder>>(
      std::nullopt);
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
            container.emplace(config::key_cert{
              to_absolute(node["key_file"].as<ss::sstring>()),
              to_absolute(node["cert_file"].as<ss::sstring>())});
        } else if (node["p12_file"]) {
            container.emplace(config::p12_container{
              to_absolute(node["p12_file"].as<ss::sstring>()),
              node["p12_password"].as<ss::sstring>()});
        }
        rhs = config::tls_config(
          enabled,
          container,
          to_absolute(read_optional(node, "truststore_file")),
          to_absolute(read_optional(node, "crl_file")),
          node["require_client_auth"]
            && node["require_client_auth"].as<bool>());
    }
    return true;
}

} // namespace YAML
