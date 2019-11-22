#pragma once
#include "config/config_store.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "model/metadata.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <cstdlib>
#include <filesystem>

namespace config {
struct data_directory_path {
    std::filesystem::path path;
    sstring as_sstring() const { return path.c_str(); }
};

struct configuration final : public config_store {
    // WAL
    property<data_directory_path> data_directory;
    property<bool> developer_mode;
    property<uint64_t> log_segment_size;
    // Network
    property<socket_address> rpc_server;
    // Raft
    property<model::node_id> node_id;
    property<int32_t> seed_server_meta_topic_partitions;
    property<std::chrono::milliseconds> raft_timeout;
    property<std::vector<seed_server>> seed_servers;
    property<int16_t> min_version;
    property<int16_t> max_version;
    // Kafka
    property<socket_address> kafka_api;
    property<tls_config> kafka_api_tls;
    property<bool> use_scheduling_groups;
    property<socket_address> admin;
    property<sstring> admin_api_doc_dir;
    property<bool> enable_admin_api;
    property<int16_t> default_num_windows;
    property<std::chrono::milliseconds> default_window_sec;
    property<std::chrono::milliseconds> quota_manager_gc_sec;
    property<uint32_t> target_quota_byte_rate;
    property<std::optional<sstring>> rack;
    property<bool> disable_metrics;

    configuration();

    void read_yaml(const YAML::Node& root_node) override;

    socket_address advertised_kafka_api() const {
        return _advertised_kafka_api().value_or(kafka_api());
    }

    socket_address advertised_rpc_api() const {
        return _advertised_rpc_api().value_or(rpc_server());
    }

private:
    property<std::optional<socket_address>> _advertised_kafka_api;
    property<std::optional<socket_address>> _advertised_rpc_api;
};

configuration& shard_local_cfg();

using conf_ref = typename std::reference_wrapper<configuration>;

static inline model::broker make_self_broker(const configuration& cfg) {
    auto kafka_addr = cfg.advertised_kafka_api();
    return model::broker(
      model::node_id(cfg.node_id),
      fmt::format("{}", kafka_addr.addr()),
      kafka_addr.port(),
      cfg.rack);
}

} // namespace config

namespace std {
inline ostream& operator<<(ostream& o, const config::data_directory_path& p) {
    return o << "{data_directory=" << p.path << "}";
}
} // namespace std

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

template<typename T>
struct adl_serializer<std::optional<T>> {
    static void to_json(json& j, const std::optional<T>& v) {
        if (v) {
            j = *v;
        }
    }
};
template<typename T, typename Tag>
struct adl_serializer<named_type<T, Tag>> {
    static void to_json(json& j, const named_type<T, Tag>& v) { j = v(); }
};
} // namespace nlohmann

namespace config {
static void to_json(nlohmann::json& j, const data_directory_path& v) {
    j["data_directory"] = v.path.c_str();
}

static void to_json(nlohmann::json& j, const seed_server& v) {
    j = {{"node_id", v.id}, {"host", v.addr}};
}

static void to_json(nlohmann::json& j, const key_cert& v) {
    j = {{"key_file", v.key_file}, {"cert_file", v.cert_file}};
}

static void to_json(nlohmann::json& j, const tls_config& v) {
    j = {{"enabled", v.is_enabled()},
         {"client_auth", v.get_require_client_auth()}};
    if (v.get_key_cert_files()) {
        j["key_cert"] = *(v.get_key_cert_files());
    }
    if (v.get_truststore_file()) {
        j["truststore_file"] = *(v.get_truststore_file());
    }
}
} // namespace config

namespace YAML {
template<>
struct convert<config::data_directory_path> {
    using type = config::data_directory_path;
    static Node encode(const type& rhs) {
        Node node;
        node = rhs.path.c_str();
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        auto pstr = node.as<std::string>();
        if (pstr[0] == '~') {
            const char* home = std::getenv("HOME");
            if (!home) {
                return false;
            }
            pstr = fmt::format("{}{}", home, pstr.erase(0, 1));
        }
        rhs.path = pstr;
        return true;
    }
};

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

    static std::optional<sstring>
    read_optional(const Node& node, const sstring& key) {
        if (node[key]) {
            return node[key].as<sstring>();
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

        auto key_cert = node["key_file"] ? std::make_optional<config::key_cert>(
                          config::key_cert{node["key_file"].as<sstring>(),
                                           node["cert_file"].as<sstring>()})
                                         : std::nullopt;
        rhs = config::tls_config(
          node["enabled"] && node["enabled"].as<bool>(),
          key_cert,
          read_optional(node, "truststore_file"),
          node["require_client_auth"]
            && node["require_client_auth"].as<bool>());
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
        if (node) {
            rhs = std::make_optional<T>(node.as<T>());
        } else {
            rhs = std::nullopt;
        }
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

}; // namespace YAML
