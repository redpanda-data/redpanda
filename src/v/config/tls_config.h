/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "config/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/tls.hh>

#include <yaml-cpp/yaml.h>

#include <optional>
#include <variant>

namespace config {

inline ss::tls::tls_version from_config(tls_version v) {
    switch (v) {
    case tls_version::v1_0:
        return ss::tls::tls_version::tlsv1_0;
    case tls_version::v1_1:
        return ss::tls::tls_version::tlsv1_1;
    case tls_version::v1_2:
        return ss::tls::tls_version::tlsv1_2;
    case tls_version::v1_3:
        return ss::tls::tls_version::tlsv1_3;
    }
}

struct key_cert {
    ss::sstring key_file;
    ss::sstring cert_file;

    bool operator==(const key_cert& rhs) const {
        return key_file == rhs.key_file && cert_file == rhs.cert_file;
    }

    friend std::ostream& operator<<(std::ostream& o, const key_cert& c);
};

struct p12_container {
    ss::sstring p12_path;
    ss::sstring p12_password;

    friend bool operator==(const p12_container&, const p12_container&)
      = default;

    friend std::ostream& operator<<(std::ostream& os, const p12_container& p);
};

using key_cert_container = std::variant<key_cert, p12_container>;

inline constexpr std::string_view tlsv1_2_cipher_string
  = "ECDHE-RSA-AES128-GCM-SHA256:AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:"
    "AES256-GCM-SHA384:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-SHA:AES128-"
    "SHA:AES128-CCM:ECDHE-RSA-AES256-SHA:AES256-SHA:AES256-CCM";

inline constexpr std::string_view tlsv1_3_ciphersuites
  = "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_"
    "SHA256:TLS_AES_128_CCM_SHA256";
class tls_config {
public:
    tls_config()
      : _key_cert(std::nullopt)
      , _truststore_file(std::nullopt)
      , _crl_file(std::nullopt) {}

    tls_config(
      bool enabled,
      std::optional<key_cert_container> key_cert,
      std::optional<ss::sstring> truststore,
      std::optional<ss::sstring> crl,
      bool require_client_auth)
      : _enabled(enabled)
      , _key_cert(std::move(key_cert))
      , _truststore_file(std::move(truststore))
      , _crl_file(std::move(crl))
      , _require_client_auth(require_client_auth) {}

    bool is_enabled() const { return _enabled; }

    const std::optional<key_cert_container>& get_key_cert_files() const {
        return _key_cert;
    }

    const std::optional<ss::sstring>& get_truststore_file() const {
        return _truststore_file;
    }

    const std::optional<ss::sstring>& get_crl_file() const { return _crl_file; }

    bool get_require_client_auth() const { return _require_client_auth; }

    ss::future<std::optional<ss::tls::credentials_builder>>

    get_credentials_builder() const&;

    ss::future<std::optional<ss::tls::credentials_builder>>
    get_credentials_builder() &&;

    static std::optional<ss::sstring> validate(const tls_config& c);

    bool operator==(const tls_config& rhs) const = default;

    friend std::ostream&
    operator<<(std::ostream& o, const config::tls_config& c);

private:
    bool _enabled{false};
    std::optional<key_cert_container> _key_cert;
    std::optional<ss::sstring> _truststore_file;
    std::optional<ss::sstring> _crl_file;
    bool _require_client_auth{false};
};

} // namespace config

namespace YAML {

template<>
struct convert<config::tls_config> {
    static Node encode(const config::tls_config& rhs);

    static std::optional<ss::sstring>
    read_optional(const Node& node, const ss::sstring& key);

    static bool decode(const Node& node, config::tls_config& rhs);
};

} // namespace YAML
