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

#include "config/leaders_preference.h"
#include "config/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "strings/string_switch.h"

#include <boost/lexical_cast.hpp>
#include <yaml-cpp/yaml.h>

#include <unordered_map>

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
struct convert<ss::socket_address> {
    using type = ss::socket_address;
    static Node encode(const type& rhs) {
        Node node;
        std::ostringstream o;
        o << rhs.addr();
        if (!o.good()) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "failed to format socket_address, state: {}",
              o.rdstate()));
        }
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

        auto compaction = model::is_compaction_enabled(rhs);
        auto deletion = model::is_deletion_enabled(rhs);

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
struct convert<config::s3_url_style> {
    using type = config::s3_url_style;
    static Node encode(const type& rhs) {
        Node node;
        return node = fmt::format("{}", rhs);
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        rhs = string_switch<type>(std::string_view{value})
                .match("virtual_host", type::virtual_host)
                .match("path", type::path);

        return true;
    }
};

template<>
struct convert<model::cloud_credentials_source> {
    using type = model::cloud_credentials_source;

    static constexpr auto acceptable_values = std::to_array(
      {"config_file",
       "aws_instance_metadata",
       "gcp_instance_metadata",
       "sts",
       "azure_aks_oidc_federation",
       "azure_vm_instance_metadata"});

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
        case model::cloud_credentials_source::azure_aks_oidc_federation:
            node = "azure_aks_oidc_federation";
            break;
        case model::cloud_credentials_source::azure_vm_instance_metadata:
            node = "azure_vm_instance_metadata";
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
                .match("sts", model::cloud_credentials_source::sts)
                .match(
                  "azure_aks_oidc_federation",
                  model::cloud_credentials_source::azure_aks_oidc_federation)
                .match(
                  "azure_vm_instance_metadata",
                  model::cloud_credentials_source::azure_vm_instance_metadata);

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

template<typename T>
concept has_hashable_key_type = requires(T x) {
    x.key_name();
    {
        std::hash<typename T::key_type>{}(x.key())
    } -> std::convertible_to<std::size_t>;
};

template<has_hashable_key_type T>
struct convert<std::unordered_map<typename T::key_type, T>> {
    using type = std::unordered_map<typename T::key_type, T>;
    static Node encode(const type& rhs) {
        Node node;
        for (const auto& group : rhs) {
            node.push_back(convert<T>::encode(group.second));
        }
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        rhs = std::unordered_map<typename T::key_type, T>{};
        if (node.IsSequence()) {
            for (auto elem : node) {
                if (!elem[T::key_name()]) {
                    return false;
                }
                auto elem_val = elem.as<T>();
                rhs.emplace(elem_val.key(), elem_val);
            }
        } else {
            auto elem_val = node.as<T>();
            rhs.emplace(elem_val.key(), elem_val);
        }
        return true;
    }
};

template<>
struct convert<model::cloud_storage_backend> {
    using type = model::cloud_storage_backend;

    static constexpr auto acceptable_values = std::to_array(
      {"aws",
       "google_s3_compat",
       "azure",
       "minio",
       "oracle_s3_compat",
       "unknown"});

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match("aws", model::cloud_storage_backend::aws)
                .match(
                  "google_s3_compat",
                  model::cloud_storage_backend::google_s3_compat)
                .match("minio", model::cloud_storage_backend::minio)
                .match("azure", model::cloud_storage_backend::azure)
                .match(
                  "oracle_s3_compat",
                  model::cloud_storage_backend::oracle_s3_compat)
                .match("unknown", model::cloud_storage_backend::unknown);

        return true;
    }
};

template<>
struct convert<std::filesystem::path> {
    using type = std::filesystem::path;

    static Node encode(const type& rhs) { return Node(rhs.native()); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        rhs = std::filesystem::path{value};

        return true;
    }
};

template<>
struct convert<model::cloud_storage_chunk_eviction_strategy> {
    using type = model::cloud_storage_chunk_eviction_strategy;

    static constexpr auto acceptable_values = std::to_array(
      {"eager", "capped", "predictive"});

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match(
                  "eager", model::cloud_storage_chunk_eviction_strategy::eager)
                .match(
                  "capped",
                  model::cloud_storage_chunk_eviction_strategy::capped)
                .match(
                  "predictive",
                  model::cloud_storage_chunk_eviction_strategy::predictive);
        return true;
    }
};

template<>
struct convert<pandaproxy::schema_registry::schema_id_validation_mode> {
    using type = pandaproxy::schema_registry::schema_id_validation_mode;

    static constexpr auto acceptable_values = std::to_array(
      {to_string_view(type::none),
       to_string_view(type::redpanda),
       to_string_view(type::compat)});

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match(to_string_view(type::none), type::none)
                .match(to_string_view(type::redpanda), type::redpanda)
                .match(to_string_view(type::compat), type::compat);

        return true;
    }
};

template<>
struct convert<model::fetch_read_strategy> {
    using type = model::fetch_read_strategy;

    static constexpr auto acceptable_values = std::to_array({
      model::fetch_read_strategy_to_string(type::polling),
      model::fetch_read_strategy_to_string(type::non_polling),
      model::fetch_read_strategy_to_string(type::non_polling_with_debounce),
      model::fetch_read_strategy_to_string(type::non_polling_with_pid),
    });

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();

        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match(
                  model::fetch_read_strategy_to_string(type::polling),
                  type::polling)
                .match(
                  model::fetch_read_strategy_to_string(type::non_polling),
                  type::non_polling)
                .match(
                  model::fetch_read_strategy_to_string(
                    type::non_polling_with_debounce),
                  type::non_polling_with_debounce)
                .match(
                  model::fetch_read_strategy_to_string(
                    type::non_polling_with_pid),
                  type::non_polling_with_pid);

        return true;
    }
};

template<>
struct convert<model::write_caching_mode> {
    using type = model::write_caching_mode;

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        auto mode = model::write_caching_mode_from_string(value);
        if (!mode) {
            return false;
        }
        rhs = mode.value();
        return true;
    }
};

template<>
struct convert<model::recovery_validation_mode> {
    using type = model::recovery_validation_mode;
    constexpr static auto acceptable_values = std::to_array(
      {"check_manifest_existence",
       "check_manifest_and_segment_metadata",
       "no_check"});
    static Node encode(const type& rhs) {
        Node node;
        return Node{boost::lexical_cast<std::string>(rhs)};
    }
    static bool decode(const Node& node, type& rhs) {
        auto node_str = node.as<std::string>();
        if (
          std::ranges::find(acceptable_values, node_str)
          == acceptable_values.end()) {
            return false;
        }
        rhs = boost::lexical_cast<type>(node_str);
        return true;
    }
};

template<>
struct convert<config::fips_mode_flag> {
    using type = config::fips_mode_flag;

    static constexpr auto acceptable_values = std::to_array(
      {to_string_view(type::disabled),
       to_string_view(type::enabled),
       to_string_view(type::permissive)});

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        if (
          std::find(acceptable_values.begin(), acceptable_values.end(), value)
          == acceptable_values.end()) {
            return false;
        }

        rhs = string_switch<type>(std::string_view{value})
                .match(to_string_view(type::disabled), type::disabled)
                .match(to_string_view(type::enabled), type::enabled)
                .match(to_string_view(type::permissive), type::permissive);

        return true;
    }
};

template<>
struct convert<config::tls_version> {
    using type = config::tls_version;

    static Node encode(const type& rhs) { return Node(fmt::format("{}", rhs)); }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        auto out = string_switch<std::optional<type>>(std::string_view{value})
                     .match(to_string_view(type::v1_0), type::v1_0)
                     .match(to_string_view(type::v1_1), type::v1_1)
                     .match(to_string_view(type::v1_2), type::v1_2)
                     .match(to_string_view(type::v1_3), type::v1_3)
                     .default_match(std::nullopt);
        if (out.has_value()) {
            rhs = out.value();
        }
        return out.has_value();
    }
};

template<>
struct convert<model::node_uuid> {
    using type = model::node_uuid;
    static Node encode(const type& rhs) {
        return Node(ssx::sformat("{}", rhs));
    }
    static bool decode(const Node& node, type& rhs) {
        auto value = node.as<std::string>();
        auto out = [&value]() -> std::optional<model::node_uuid> {
            try {
                return model::node_uuid(uuid_t::from_string(value));
            } catch (const std::runtime_error& e) {
                return std::nullopt;
            }
        }();
        if (out.has_value()) {
            rhs = out.value();
        }
        return out.has_value();
    }
};

template<>
struct convert<config::leaders_preference> {
    using type = config::leaders_preference;

    static Node encode(const type& rhs) { return Node(fmt::to_string(rhs)); }

    static bool decode(const Node& node, type& rhs) {
        auto node_str = node.as<std::string>();
        try {
            rhs = config::leaders_preference::parse(node_str);
            return true;
        } catch (const std::runtime_error&) {
            return false;
        }
    }
};

} // namespace YAML
