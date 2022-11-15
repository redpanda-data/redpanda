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
#include "utils/string_switch.h"

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

template<::detail::NamedTypeTrivialSubclass T>
struct convert<T> {

    static Node encode(const T& rhs) { return Node(rhs()); }

    static bool decode(const Node& node, T& rhs) {
        if (!node) {
            return false;
        }
        rhs = T{node.as<typename T::type>()};
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

template<>
struct convert<model::cloud_credentials_source> {
    using type = model::cloud_credentials_source;

    static constexpr std::array<const char*, 4> acceptable_values{
      "config_file", "aws_instance_metadata", "gcp_instance_metadata", "sts"};

    static Node encode(const type& rhs) {
        Node node;
        switch (rhs) {
        case model::cloud_credentials_source::config_file:
            node = "config_file";
            break;
        case model::cloud_credentials_source::aws_instance_metadata:
            node = "aws_instance_metadata";
            break;
        case model::cloud_credentials_source::sts:
            node = "sts";
            break;
        case model::cloud_credentials_source::gcp_instance_metadata:
            node = "gcp_instance_metadata";
            break;
        }
        return node;
    }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match(
                  "config_file", model::cloud_credentials_source::config_file)
                .match(
                  "aws_instance_metadata",
                  model::cloud_credentials_source::aws_instance_metadata)
                .match(
                  "gcp_instance_metadata",
                  model::cloud_credentials_source::gcp_instance_metadata)
                .match("sts", model::cloud_credentials_source::sts);

        return true;
    }
};

template<>
struct convert<model::partition_autobalancing_mode> {
    using type = model::partition_autobalancing_mode;
    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (value == "off") {
            rhs = model::partition_autobalancing_mode::off;
        } else if (value == "node_add") {
            rhs = model::partition_autobalancing_mode::node_add;
        } else if (value == "continuous") {
            rhs = model::partition_autobalancing_mode::continuous;
        } else {
            return false;
        }

        return true;
    }
};

} // namespace YAML
