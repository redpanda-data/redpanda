#pragma once

#include "redpanda/config/config_store.h"
#include "redpanda/config/seed_server.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

namespace config {
struct configuration final : public config_store {
    // WAL
    property<sstring> data_directory;
    property<int64_t> log_segment_size_bytes;
    property<bool> developer_mode;
    property<int64_t> max_retention_period_hours;
    property<int64_t> writer_flush_period_ms;
    property<int64_t> max_retention_size;
    property<int32_t> max_bytes_in_writer_cache;
    // Network
    property<socket_address> rpc_server;
    // Raft
    property<int32_t> node_id;
    property<int32_t> seed_server_meta_topic_partitions;
    property<std::vector<seed_server>> seed_servers;
    property<int16_t> min_version;
    property<int16_t> max_version;
    // Kafka
    property<socket_address> kafka_api;
    property<bool> use_scheduling_groups;
    property<socket_address> admin;
    property<sstring> admin_api_doc_dir;
    property<bool> enable_admin_api;
    property<int16_t> default_num_windows;
    property<std::chrono::milliseconds> default_window_sec;
    property<std::chrono::milliseconds> quota_manager_gc_sec;
    property<uint32_t> target_quota_byte_rate;

    configuration();

    void read_yaml(const YAML::Node& root_node) override;
};

using conf_ref = typename std::reference_wrapper<configuration>;

}; // namespace config

namespace nlohmann {
template<>
struct adl_serializer<seastar::sstring> {
    static void to_json(json& j, const seastar::sstring& v) {
        j = std::string(v);
    }
};

template<>
struct adl_serializer<seastar::socket_address> {
    static void to_json(json& j, const seastar::socket_address& v) {
        // seastar doesn't have a fmt::formatter for inet_address
        std::ostringstream a;
        a << v.addr();
        j = {{"address", a.str()}, {"port", v.port()}};
    }
};

template<>
struct adl_serializer<std::chrono::milliseconds> {
    static void to_json(json& j, const std::chrono::milliseconds& v) {
        j = v.count();
    }
};
} // namespace nlohmann

namespace config {
static void to_json(nlohmann::json& j, const seed_server& v) {
    j = {{"node_id", v.id}, {"host", v.addr}};
}
} // namespace config

namespace YAML {
template<>
struct convert<config::seed_server> {
    using type = config::seed_server;
    static Node encode(const type& rhs) {
        Node node;
        node["node_id"] = rhs.id;
        node["host"] = rhs.addr;
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        // Required fields
        for (auto s : {"node_id", "host"}) {
            if (!node[s]) {
                return false;
            }
        }
        rhs.id = node["node_id"].as<int64_t>();
        rhs.addr = node["host"].as<socket_address>();
        return true;
    }
};

template<>
struct convert<socket_address> {
    using type = socket_address;
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
        auto addr_str = node["address"].as<sstring>();
        auto port = node["port"].as<uint16_t>();
        if (addr_str == "localhost") {
            rhs = socket_address(net::inet_address("127.0.0.1"), port);
        } else {
            rhs = socket_address(addr_str, port);
        }
        return true;
    }
};

template<>
struct convert<std::chrono::milliseconds> {
    using type = std::chrono::milliseconds;

    static Node encode(const type& rhs) {
        return Node(rhs.count());
    }

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

}; // namespace YAML
