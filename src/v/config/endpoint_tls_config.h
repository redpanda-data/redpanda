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
#include "config/tls_config.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/yaml.h>

namespace config {
struct endpoint_tls_config {
    ss::sstring name;
    tls_config config;

    friend std::ostream&
    operator<<(std::ostream& o, const endpoint_tls_config& cfg) {
        fmt::print(o, "{{name: {}, tls_config: {}}}", cfg.name, cfg.config);
        return o;
    }

    bool operator==(const endpoint_tls_config& rhs) const = default;

    static std::optional<ss::sstring> validate(const endpoint_tls_config& ec) {
        return tls_config::validate(ec.config);
    }
    static std::optional<ss::sstring>
    validate_many(const std::vector<endpoint_tls_config>& v) {
        for (auto& ec : v) {
            auto err = tls_config::validate(ec.config);
            if (err) {
                return err;
            }
        }
        return std::nullopt;
    }
};
} // namespace config

namespace YAML {
template<>
struct convert<config::endpoint_tls_config> {
    using type = config::endpoint_tls_config;
    static Node encode(const type& rhs) {
        Node node;
        node["name"] = rhs.name;
        node["config"] = rhs.config;
        return node;
    }

    static bool decode(const Node& node, type& rhs) {
        ss::sstring name;
        if (node["name"]) {
            name = node["name"].as<ss::sstring>();
        }
        config::tls_config tls_cfg;
        auto res = convert<config::tls_config>{}.decode(node, tls_cfg);
        if (!res) {
            return res;
        }
        rhs = config::endpoint_tls_config{
          .name = name,
          .config = tls_cfg,
        };

        return true;
    }
};
} // namespace YAML
