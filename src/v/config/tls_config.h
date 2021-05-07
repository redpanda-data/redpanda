/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/tls.hh>

#include <optional>
#include <utility>

namespace config {

struct key_cert {
    ss::sstring key_file;
    ss::sstring cert_file;
};

class tls_config {
public:
    tls_config()
      : _key_cert(std::nullopt)
      , _truststore_file(std::nullopt) {}

    tls_config(
      bool enabled,
      std::optional<key_cert> key_cert,
      std::optional<ss::sstring> truststore,
      bool require_client_auth)
      : _enabled(enabled)
      , _key_cert(std::move(key_cert))
      , _truststore_file(std::move(truststore))
      , _require_client_auth(require_client_auth) {}

    bool is_enabled() const { return _enabled; }

    const std::optional<key_cert>& get_key_cert_files() const {
        return _key_cert;
    }

    const std::optional<ss::sstring>& get_truststore_file() const {
        return _truststore_file;
    }

    bool get_require_client_auth() const { return _require_client_auth; }

    ss::future<std::optional<ss::tls::credentials_builder>>
    get_credentials_builder() const& {
        if (_enabled) {
            return ss::do_with(
              ss::tls::credentials_builder{},
              [this](ss::tls::credentials_builder& builder) {
                  builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);
                  if (_require_client_auth) {
                      builder.set_client_auth(ss::tls::client_auth::REQUIRE);
                  }

                  auto f = _truststore_file ? builder.set_x509_trust_file(
                             *_truststore_file, ss::tls::x509_crt_format::PEM)
                                            : builder.set_system_trust();
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

        return ss::make_ready_future<
          std::optional<ss::tls::credentials_builder>>(std::nullopt);
    }

    ss::future<std::optional<ss::tls::credentials_builder>>
    get_credentials_builder() && {
        auto ptr = ss::make_lw_shared(std::move(*this));
        return ptr->get_credentials_builder().finally([ptr] {});
    }

    static std::optional<ss::sstring> validate(const tls_config& c) {
        if (c.get_require_client_auth() && !c.get_truststore_file()) {
            return "Trust store is required when client authentication is "
                   "enabled";
        }

        return std::nullopt;
    }

private:
    bool _enabled{false};
    std::optional<key_cert> _key_cert;
    std::optional<ss::sstring> _truststore_file;
    bool _require_client_auth{false};
};

} // namespace config
namespace std {
static inline ostream& operator<<(ostream& o, const config::key_cert& c) {
    o << "{ "
      << "key_file: " << c.key_file << " "
      << "cert_file: " << c.cert_file << " }";
    return o;
}
static inline ostream& operator<<(ostream& o, const config::tls_config& c) {
    o << "{ "
      << "enabled: " << c.is_enabled() << " "
      << "key/cert files: " << c.get_key_cert_files() << " "
      << "ca file: " << c.get_truststore_file() << " "
      << "client_auth_required: " << c.get_require_client_auth() << " }";
    return o;
}
} // namespace std
