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
#include "config/convert.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/tls.hh>

#include <boost/filesystem.hpp>
#include <yaml-cpp/yaml.h>

#include <optional>
#include <utility>

namespace config {

struct key_cert {
    ss::sstring key_file;
    ss::sstring cert_file;

    bool operator==(const key_cert& rhs) const {
        return key_file == rhs.key_file && cert_file == rhs.cert_file;
    }

    friend std::ostream& operator<<(std::ostream& o, const key_cert& c) {
        o << "{ "
          << "key_file: " << c.key_file << " "
          << "cert_file: " << c.cert_file << " }";
        return o;
    }
};

class tls_config {
public:
    tls_config()
      : _key_cert(std::nullopt)
      , _truststore_file(std::nullopt)
      , _crl_file(std::nullopt) {}

    tls_config(
      bool enabled,
      std::optional<key_cert> key_cert,
      std::optional<ss::sstring> truststore,
      std::optional<ss::sstring> crl,
      bool require_client_auth)
      : _enabled(enabled)
      , _key_cert(std::move(key_cert))
      , _truststore_file(std::move(truststore))
      , _crl_file(std::move(crl))
      , _require_client_auth(require_client_auth) {}

    bool is_enabled() const { return _enabled; }

    const std::optional<key_cert>& get_key_cert_files() const {
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
    std::optional<key_cert> _key_cert;
    std::optional<ss::sstring> _truststore_file;
    std::optional<ss::sstring> _crl_file;
    bool _require_client_auth{false};
};

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

template<>
struct convert<config::tls_config> {
    static Node encode(const config::tls_config& rhs) {
        Node node;

        node["enabled"] = rhs.is_enabled();
        node["require_client_auth"] = rhs.get_require_client_auth();

        if (rhs.get_key_cert_files()) {
            node["cert_file"] = (*rhs.get_key_cert_files()).key_file;
            node["key_file"] = (*rhs.get_key_cert_files()).cert_file;
        }

        if (rhs.get_truststore_file()) {
            node["truststore_file"] = *rhs.get_truststore_file();
        }

        return node;
    }

    static std::optional<ss::sstring>
    read_optional(const Node& node, const ss::sstring& key) {
        if (node[key]) {
            return node[key].as<ss::sstring>();
        }
        return std::nullopt;
    }

    static bool decode(const Node& node, config::tls_config& rhs) {
        // either both true or both false
        if (
          static_cast<bool>(node["key_file"])
          ^ static_cast<bool>(node["cert_file"])) {
            return false;
        }
        auto enabled = node["enabled"] && node["enabled"].as<bool>();
        if (!enabled) {
            rhs = config::tls_config(
              false, std::nullopt, std::nullopt, std::nullopt, false);
        } else {
            auto key_cert
              = node["key_file"]
                  ? std::make_optional<config::key_cert>(config::key_cert{
                    to_absolute(node["key_file"].as<ss::sstring>()),
                    to_absolute(node["cert_file"].as<ss::sstring>())})
                  : std::nullopt;
            rhs = config::tls_config(
              enabled,
              key_cert,
              to_absolute(read_optional(node, "truststore_file")),
              to_absolute(read_optional(node, "crl_file")),
              node["require_client_auth"]
                && node["require_client_auth"].as<bool>());
        }
        return true;
    }
};

} // namespace YAML
