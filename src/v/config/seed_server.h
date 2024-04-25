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
#include "utils/unresolved_address.h"

#include <model/metadata.h>
#include <yaml-cpp/yaml.h>

#include <cstdint>

namespace config {
struct seed_server {
    net::unresolved_address addr;

    bool operator==(const seed_server&) const = default;
    friend bool operator<(const seed_server& lhs, const seed_server& rhs) {
        return lhs.addr < rhs.addr;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const config::seed_server& s) {
        fmt::print(o, "addr: {}", s.addr);
        return o;
    }
};
} // namespace config

namespace YAML {
template<>
struct convert<config::seed_server> {
    using type = config::seed_server;
    static Node encode(const type& rhs) {
        Node node;
        node = rhs.addr;
        return node;
    }

    /**
     * We support two seed server YAML representations:
     *
     *  1) Old one
     *      seed_servers:
     *      - host:
     *          address: ...
     *          port: ...
     *        node_id : ...
     *
     *  2) New one
     *      seed_servers:
     *      - address: ...
     *        port: ...
     *      - address:
     *        port: ...
     *
     * Node id field is not used
     *
     */
    static bool decode(const Node& node, type& rhs) {
        // Required fields
        if (!node["host"] && !node["address"]) {
            return false;
        }

        if (node["host"]) {
            rhs.addr = node["host"].as<net::unresolved_address>();
            return true;
        } else {
            rhs.addr = node.as<net::unresolved_address>();
        }

        return true;
    }
};
} // namespace YAML
