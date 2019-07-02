#pragma once

#include "filesystem/wal_opts.h"
#include "raft/raft_cfg.h"

#include <seastar/core/sstring.hh>

#include <smf/human_bytes.h>
#include <smf/log.h>
#include <smf/rpc_server_args.h>

#include <fmt/format.h>
#include <yaml-cpp/yaml.h>

#include <filesystem>

struct redpanda_cfg {
    seastar::sstring directory;
    std::string ip;
    uint16_t port;
    uint16_t kafka_transport_port = 9092;
    int32_t retention_period_hrs = 168;
    int64_t flush_period_ms = 1000;
    int64_t log_segment_size_bytes = wal_file_size_aligned();
    bool developer_mode = false;
    int64_t retention_size_bytes = -1;
    int32_t bytes_in_memory_per_writer = 1 << 20;
    int64_t id = -1;
    int16_t min_version = 0;
    int16_t max_version = 0;
    int32_t seed_server_meta_topic_partitions = 7;
    std::vector<raft_seed_server> seed_servers;

    wal_opts wal_cfg() const {
        auto o = wal_opts{directory,
                          std::chrono::milliseconds(flush_period_ms),
                          std::chrono::hours(retention_period_hrs),
                          retention_size_bytes,
                          bytes_in_memory_per_writer,
                          log_segment_size_bytes};
        auto validation = wal_opts::validate(o);
        using ut =
          typename std::underlying_type<wal_opts::validation_status>::type;
        LOG_THROW_IF(
          validation != wal_opts::validation_status::ok,
          "Invalid write ahead log settings: {}. opts: {}",
          static_cast<ut>(validation),
          o);
        return o;
    }

    raft_cfg raft() const {
        return raft_cfg{id,
                        min_version,
                        max_version,
                        seed_server_meta_topic_partitions,
                        seed_servers};
    }

    smf::rpc_server_args rpc_cfg() const {
        smf::rpc_server_args o;
        o.rpc_port = port;
        o.ip = ip;
        o.memory_avail_per_core = static_cast<uint64_t>(
          0.3 * seastar::memory::stats().total_memory());
        return o;
    }
};
namespace YAML {
template<>
struct convert<seastar::sstring> {
    static Node encode(const seastar::sstring& rhs) {
        return Node(rhs.c_str());
    }
    static bool decode(const Node& node, seastar::sstring& rhs) {
        if (!node.IsScalar())
            return false;
        rhs = node.as<std::string>();
        return true;
    }
};

template<>
struct convert<raft_seed_server> {
    using type = raft_seed_server;
    static Node encode(const type& rhs) {
        Node node;
        node["id"] = rhs.id;
        node["addr"] = fmt::format("{}", rhs.addr);
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        // Required fields
        for (auto s : {"id", "addr"}) {
            if (!node[s]) {
                return false;
            }
        }
        rhs.id = node["id"].as<int64_t>();
        rhs.addr = seastar::ipv4_addr(node["addr"].as<std::string>());
        return true;
    }
};

template<>
struct convert<redpanda_cfg> {
    static Node encode(const redpanda_cfg& rhs) {
        Node node;
        node["directory"] = rhs.directory;
        node["flush_period_ms"] = rhs.flush_period_ms;
        node["retention_size_bytes"] = rhs.retention_size_bytes;
        node["retention_period_hrs"] = rhs.retention_period_hrs;
        node["log_segment_size_bytes"] = rhs.log_segment_size_bytes;
        node["seed_server_meta_topic_partitions"]
          = rhs.seed_server_meta_topic_partitions;
        node["developer_mode"] = rhs.developer_mode;
        node["ip"] = rhs.ip;
        node["port"] = rhs.port;
        node["id"] = rhs.id;
        node["min_version"] = rhs.min_version;
        node["max_version"] = rhs.max_version;
        node["seed_servers"] = rhs.seed_servers;
        return node;
    }
    static bool decode(const Node& node, redpanda_cfg& rhs) {
        // Required fields
        for (auto s : {"directory", "port", "id", "seed_servers"}) {
            if (!node[s]) {
                return false;
            }
        }
        auto parse = [&node](const char* field, auto& v) {
            if (node[field]) {
                v = node[field].as<std::decay_t<decltype(v)>>();
            }
        };
        parse("directory", rhs.directory);
        parse("flush_period_ms", rhs.flush_period_ms);
        parse("retention_size_bytes", rhs.retention_size_bytes);
        parse("retention_period_hrs", rhs.retention_period_hrs);
        parse("log_segment_size_bytes", rhs.log_segment_size_bytes);
        parse("developer_mode", rhs.developer_mode);
        parse(
          "seed_server_meta_topic_partitions",
          rhs.seed_server_meta_topic_partitions);
        parse("ip", rhs.ip);
        parse("port", rhs.port);
        parse("id", rhs.id);
        parse("min_version", rhs.min_version);
        parse("max_version", rhs.max_version);
        parse("seed_servers", rhs.seed_servers);
        parse("kafka_transport_port", rhs.kafka_transport_port);
        // get full path
        if (rhs.directory[0] == '~') {
            const char* tilde = ::getenv("HOME");
            rhs.directory = seastar::sstring(tilde) + rhs.directory.substr(1);
        }
        {
            std::error_code ec;
            auto p = std::filesystem::canonical(rhs.directory.c_str(), ec);
            if (!ec) {
                rhs.directory = p.string();
            }
        }
        return true;
    }
};
} // namespace YAML

namespace std {
static inline ostream& operator<<(ostream& o, const redpanda_cfg& c) {
    o << "redpanda_cfg{directory:" << c.directory << ", ip:" << c.ip
      << ", port:" << c.port
      << ", retention_period_hrs:" << c.retention_period_hrs
      << ", flush_period_ms:" << c.flush_period_ms
      << ", log_segment_size_bytes:"
      << smf::human_bytes(c.log_segment_size_bytes)
      << ", developer_mode:" << c.developer_mode
      << ", retention_size_bytes:" << smf::human_bytes(c.retention_size_bytes)
      << ", bytes_in_memory_per_writer:"
      << smf::human_bytes(c.bytes_in_memory_per_writer) << "}"
      << "seastar::memory::stats{total_memory:"
      << smf::human_bytes(seastar::memory::stats().total_memory())
      << ", allocated_memory: "
      << smf::human_bytes(seastar::memory::stats().allocated_memory())
      << ", free_memory: "
      << smf::human_bytes(seastar::memory::stats().free_memory()) << "}";
    return o;
}
} // namespace std
