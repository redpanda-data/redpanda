/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "seastarx.h"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>

namespace config {
struct authentication_method {
    model::broker_endpoint endpoint;
    ss::sstring authentication_type;

    static bool is_supported_authn_type(ss::sstring authn_type) {
        ss::sstring type{authn_type};
        // Standardize the format to lower case
        std::transform(
          authn_type.cbegin(), authn_type.cend(), type.begin(), ::tolower);
        return type == "null" || type == "http_basic" || type == "mtls";
    }

    friend std::ostream&
    operator<<(std::ostream& o, const authentication_method& cfg) {
        fmt::print(o, "{{type: {}}}", cfg.authentication_type);
        return o;
    }

    bool operator==(authentication_method const& rhs) const = default;

    static std::optional<ss::sstring>
    validate(const authentication_method& cfg) {
        if (!is_supported_authn_type(cfg.authentication_type)) {
            return "Unsupported authentication type";
        }

        return std::nullopt;
    }
};
} // namespace config

namespace YAML {
template<>
struct convert<config::authentication_method> {
    using rhs_type = config::authentication_method;
    static Node encode(const rhs_type& rhs) {
        Node node;
        node["authentication_type"] = rhs.authentication_type;
        return node;
    }

    static bool decode(const Node& node, rhs_type& rhs) {
        ss::sstring authentication_type;
        if (node["authentication_type"]) {
            authentication_type = node["authentication_type"].as<ss::sstring>();
        } else {
            return false;
        }

        rhs = config::authentication_method{
          .authentication_type = authentication_type};

        return true;
    }
};
} // namespace YAML