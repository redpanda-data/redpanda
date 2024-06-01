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

#include <seastar/net/tls.hh>

namespace config {
ss::future<std::optional<ss::tls::credentials_builder>>
tls_config::get_credentials_builder() const& {
    if (_enabled) {
        return ss::do_with(
          ss::tls::credentials_builder{},
          [this](ss::tls::credentials_builder& builder) {
              builder.set_priority_string("PERFORMANCE:%SERVER_PRECEDENCE");
              builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);
              if (_require_client_auth) {
                  builder.set_client_auth(ss::tls::client_auth::REQUIRE);
              }

              auto f = _truststore_file ? builder.set_x509_trust_file(
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
                      return builder.set_x509_key_file(
                        (*_key_cert).cert_file,
                        (*_key_cert).key_file,
                        ss::tls::x509_crt_format::PEM);
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
    if (c.get_require_client_auth() && !c.get_truststore_file()) {
        return "Trust store is required when client authentication is "
               "enabled";
    }

    return std::nullopt;
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
