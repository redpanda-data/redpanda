/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/config_store.h"
#include "config/data_directory_path.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "model/metadata.h"
#include "utils/unresolved_address.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/filesystem.hpp>

namespace config {

/// Redpanda configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.

struct configuration final : public config_store {
    // WAL
    property<data_directory_path> data_directory;
    property<bool> developer_mode;
    property<uint64_t> log_segment_size;
    property<uint64_t> compacted_log_segment_size;
    // Network
    property<unresolved_address> rpc_server;
    property<tls_config> rpc_server_tls;
    // Coproc
    property<bool> enable_coproc;
    property<unresolved_address> coproc_script_manager_server;
    property<unresolved_address> coproc_supervisor_server;
    property<std::size_t> coproc_max_inflight_bytes;
    property<std::size_t> coproc_max_ingest_bytes;
    property<std::size_t> coproc_max_batch_size;
    property<std::chrono::milliseconds> coproc_offset_flush_interval_ms;

    // Raft
    property<int32_t> node_id;
    property<int32_t> seed_server_meta_topic_partitions;
    property<std::chrono::milliseconds> raft_heartbeat_interval_ms;
    property<std::vector<seed_server>> seed_servers;
    property<int16_t> min_version;
    property<int16_t> max_version;
    // Kafka
    one_or_many_property<model::broker_endpoint> kafka_api;
    property<tls_config> kafka_api_tls;
    property<bool> use_scheduling_groups;
    property<unresolved_address> admin;
    property<tls_config> admin_api_tls;
    property<bool> enable_admin_api;
    property<ss::sstring> admin_api_doc_dir;
    property<int16_t> default_num_windows;
    property<std::chrono::milliseconds> default_window_sec;
    property<std::chrono::milliseconds> quota_manager_gc_sec;
    property<uint32_t> target_quota_byte_rate;
    property<std::optional<ss::sstring>> rack;
    property<std::optional<ss::sstring>> dashboard_dir;
    property<bool> disable_metrics;
    property<std::chrono::milliseconds> group_min_session_timeout_ms;
    property<std::chrono::milliseconds> group_max_session_timeout_ms;
    property<std::chrono::milliseconds> group_initial_rebalance_delay;
    property<std::chrono::milliseconds> group_new_member_join_timeout;
    property<std::chrono::milliseconds> metadata_dissemination_interval_ms;
    property<std::chrono::milliseconds> fetch_reads_debounce_timeout;
    // same as transactional.id.expiration.ms in kafka
    property<std::chrono::milliseconds> transactional_id_expiration_ms;
    property<bool> enable_idempotence;
    // same as delete.retention.ms in kafka
    property<std::chrono::milliseconds> delete_retention_ms;
    property<std::chrono::milliseconds> log_compaction_interval_ms;
    // same as retention.size in kafka - TODO: size not implemented
    property<std::optional<size_t>> retention_bytes;
    property<int32_t> group_topic_partitions;
    property<int16_t> default_topic_replication;
    property<std::chrono::milliseconds> create_topic_timeout_ms;
    property<std::chrono::milliseconds> wait_for_leader_timeout_ms;
    property<int32_t> default_topic_partitions;
    property<bool> disable_batch_cache;
    property<std::chrono::milliseconds> raft_election_timeout_ms;
    property<std::chrono::milliseconds> kafka_group_recovery_timeout_ms;
    property<std::chrono::milliseconds> replicate_append_timeout_ms;
    property<std::chrono::milliseconds> recovery_append_timeout_ms;
    property<size_t> raft_replicate_batch_window_size;

    property<size_t> reclaim_min_size;
    property<size_t> reclaim_max_size;
    property<std::chrono::milliseconds> reclaim_growth_window;
    property<std::chrono::milliseconds> reclaim_stable_window;
    property<bool> auto_create_topics_enabled;
    property<bool> enable_pid_file;
    property<std::chrono::milliseconds> kvstore_flush_interval;
    property<size_t> kvstore_max_segment_size;
    property<std::chrono::milliseconds> max_kafka_throttle_delay_ms;
    property<std::chrono::milliseconds> raft_io_timeout_ms;
    property<std::chrono::milliseconds> join_retry_timeout_ms;
    property<std::chrono::milliseconds> raft_timeout_now_timeout_ms;
    property<std::chrono::milliseconds>
      raft_transfer_leader_recovery_timeout_ms;
    property<bool> release_cache_on_segment_roll;
    property<std::chrono::milliseconds> segment_appender_flush_timeout_ms;
    property<std::chrono::milliseconds> fetch_session_eviction_timeout_ms;
    property<size_t> max_compacted_log_segment_size;
    property<int16_t> id_allocator_log_capacity;
    property<int16_t> id_allocator_batch_size;

    configuration();

    void read_yaml(const YAML::Node& root_node) override;

    const std::vector<model::broker_endpoint>& advertised_kafka_api() const {
        if (_advertised_kafka_api().empty()) {
            return kafka_api();
        }
        return _advertised_kafka_api();
    }

    unresolved_address advertised_rpc_api() const {
        return _advertised_rpc_api().value_or(rpc_server());
    }

    // build pidfile path: `<data_directory>/pid.lock`
    std::filesystem::path pidfile_path() const {
        return data_directory().path / "pid.lock";
    }

private:
    one_or_many_property<model::broker_endpoint> _advertised_kafka_api;
    property<std::optional<unresolved_address>> _advertised_rpc_api;
};

configuration& shard_local_cfg();

using conf_ref = typename std::reference_wrapper<configuration>;

} // namespace config

namespace std {
inline ostream& operator<<(ostream& o, const config::data_directory_path& p) {
    return o << "{data_directory=" << p.path << "}";
}
} // namespace std

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
            rhs.addr = node["host"].as<unresolved_address>();
            return true;
        } else {
            rhs.addr = node.as<unresolved_address>();
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
struct convert<unresolved_address> {
    using type = unresolved_address;
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
        rhs = unresolved_address(addr_str, port);
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

inline ss::sstring to_absolute(const ss::sstring& path) {
    namespace fs = boost::filesystem;
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
            rhs = config::tls_config(false, std::nullopt, std::nullopt, false);
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
              node["require_client_auth"]
                && node["require_client_auth"].as<bool>());
        }
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
        auto addr = unresolved_address(std::move(address), port);
        rhs = model::broker_endpoint(std::move(name), std::move(addr));
        return true;
    }
};

}; // namespace YAML
