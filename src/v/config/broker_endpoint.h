// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "config/convert.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/yaml.h>

namespace YAML {

template<>
struct convert<model::broker_endpoint> {
    using type = model::broker_endpoint;

    static Node encode(const type& rhs) {
        Node node;
        node["name"] = rhs.name;
        node["address"] = rhs.address.host();
        node["port"] = rhs.address.port();
        return node;
    }

    static bool decode(const Node& node, type& rhs) {
        for (auto s : {"address", "port"}) {
            if (!node[s]) {
                return false;
            }
        }
        ss::sstring name;
        if (node["name"]) {
            name = node["name"].as<ss::sstring>();
        }
        auto address = node["address"].as<ss::sstring>();
        auto port = node["port"].as<uint16_t>();
        auto addr = net::unresolved_address(std::move(address), port);
        rhs = model::broker_endpoint(std::move(name), std::move(addr));
        return true;
    }
};

} // namespace YAML
