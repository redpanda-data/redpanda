/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <boost/lexical_cast.hpp>
#include <yaml-cpp/yaml.h>

namespace YAML {

template<>
struct convert<ss::sstring> {
    static Node encode(const ss::sstring& rhs) { return Node(rhs.c_str()); }
    static bool decode(const Node& node, ss::sstring& rhs) {
        if (!node.IsScalar()) {
            return false;
        }
        rhs = node.as<std::string>();
        return true;
    }
};

template<typename T>
struct convert<std::optional<T>> {
    using type = std::optional<T>;

    static Node encode(const type& rhs) {
        if (rhs) {
            return Node(*rhs);
        }
    }

    static bool decode(const Node& node, type& rhs) {
        if (node && !node.IsNull()) {
            rhs = std::make_optional<T>(node.as<T>());
        } else {
            rhs = std::nullopt;
        }
        return true;
    }
};

template<>
struct convert<model::violation_recovery_policy> {
    using type = model::violation_recovery_policy;
    static Node encode(const type& rhs) {
        Node node;
        if (rhs == model::violation_recovery_policy::crash) {
            node = "crash";
        } else if (rhs == model::violation_recovery_policy::best_effort) {
            node = "best_effort";
        } else {
            node = "crash";
        }
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (value == "crash") {
            rhs = model::violation_recovery_policy::crash;
        } else if (value == "best_effort") {
            rhs = model::violation_recovery_policy::best_effort;
        } else {
            return false;
        }

        return true;
    }
};

template<>
struct convert<ss::socket_address> {
    using type = ss::socket_address;
    static Node encode(const type& rhs) {
        Node node;
        std::ostringstream o;
        o << rhs.addr();
        node["address"] = o.str();
        node["port"] = rhs.port();
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        for (auto s : {"address", "port"}) {
            if (!node[s]) {
                return false;
            }
        }
        auto addr_str = node["address"].as<ss::sstring>();
        auto port = node["port"].as<uint16_t>();
        if (addr_str == "localhost") {
            rhs = ss::socket_address(ss::net::inet_address("127.0.0.1"), port);
        } else {
            rhs = ss::socket_address(addr_str, port);
        }
        return true;
    }
};

template<>
struct convert<net::unresolved_address> {
    using type = net::unresolved_address;
    static Node encode(const type& rhs) {
        Node node;
        node["address"] = rhs.host();
        node["port"] = rhs.port();
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        for (auto s : {"address", "port"}) {
            if (!node[s]) {
                return false;
            }
        }
        auto addr_str = node["address"].as<ss::sstring>();
        auto port = node["port"].as<uint16_t>();
        rhs = net::unresolved_address(addr_str, port);
        return true;
    }
};

template<>
struct convert<std::chrono::milliseconds> {
    using type = std::chrono::milliseconds;

    static Node encode(const type& rhs) { return Node(rhs.count()); }

    static bool decode(const Node& node, type& rhs) {
        type::rep secs;
        auto res = convert<type::rep>::decode(node, secs);
        if (!res) {
            return res;
        }
        rhs = std::chrono::milliseconds(secs);
        return true;
    }
};

template<>
struct convert<std::chrono::seconds> {
    using type = std::chrono::seconds;

    static Node encode(const type& rhs) { return Node(rhs.count()); }

    static bool decode(const Node& node, type& rhs) {
        type::rep secs;
        auto res = convert<type::rep>::decode(node, secs);
        if (!res) {
            return res;
        }
        rhs = std::chrono::seconds(secs);
        return true;
    }
};

template<typename T, typename Tag>
struct convert<named_type<T, Tag>> {
    using type = named_type<T, Tag>;

    static Node encode(const type& rhs) { return Node(rhs()); }

    static bool decode(const Node& node, type& rhs) {
        if (!node) {
            return false;
        }
        rhs = type{node.as<T>()};
        return true;
    }
};

template<>
struct convert<model::cleanup_policy_bitflags> {
    using type = model::cleanup_policy_bitflags;
    static Node encode(const type& rhs) {
        Node node;

        auto compaction = (rhs & model::cleanup_policy_bitflags::compaction)
                          == model::cleanup_policy_bitflags::compaction;
        auto deletion = (rhs & model::cleanup_policy_bitflags::deletion)
                        == model::cleanup_policy_bitflags::deletion;

        if (compaction && deletion) {
            node = "compact,delete";

        } else if (compaction) {
            node = "compact";

        } else if (deletion) {
            node = "delete";
        }
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        // normalize cleanup policy string (remove all whitespaces)
        std::erase_if(value, isspace);
        rhs = boost::lexical_cast<type>(value);

        return true;
    }
};
template<>
struct convert<model::compression> {
    using type = model::compression;
    static Node encode(const type& rhs) {
        Node node;
        return node = fmt::format("{}", rhs);
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        rhs = boost::lexical_cast<type>(value);

        return true;
    }
};

template<>
struct convert<model::timestamp_type> {
    using type = model::timestamp_type;
    static Node encode(const type& rhs) {
        Node node;
        return node = fmt::format("{}", rhs);
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        rhs = boost::lexical_cast<type>(value);

        return true;
    }
};
} // namespace YAML
