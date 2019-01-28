#pragma once

#include <filesystem>

#include <seastar/core/sstring.hh>
#include <smf/human_bytes.h>
#include <smf/log.h>
#include <smf/rpc_server_args.h>
#include <yaml-cpp/yaml.h>

#include "filesystem/wal_opts.h"

namespace v {

struct redpanda_cfg {
  seastar::sstring directory;
  seastar::sstring ip;
  uint16_t port;
  int32_t retention_period_hrs = 168;
  int64_t flush_period_ms = 10000;
  int64_t log_segment_size_bytes = wal_file_size_aligned();
  bool developer_mode = false;
  int64_t retention_size_bytes = -1;

  // Note: the following are not exposed in yet
  // Please think carefully about exposing them
  int8_t writer_concurrency_pages = 4;
  int32_t bytes_in_memory_per_writer = 1024 * 1024;

  inline wal_opts
  wal_cfg() const {
    auto o = wal_opts{directory,
                      std::chrono::milliseconds(flush_period_ms),
                      std::chrono::hours(retention_period_hrs),
                      retention_size_bytes,
                      writer_concurrency_pages,
                      bytes_in_memory_per_writer,
                      log_segment_size_bytes};
    auto validation = wal_opts::validate(o);
    using ut = typename std::underlying_type<wal_opts::validation_status>::type;
    LOG_THROW_IF(validation != wal_opts::validation_status::ok,
                 "Invalid write ahead log settings: {}. opts: {}",
                 static_cast<ut>(validation), o);
    return std::move(o);
  };
  inline smf::rpc_server_args
  rpc_cfg() const {
    smf::rpc_server_args o;
    o.rpc_port = port;
    o.ip = ip;
    o.memory_avail_per_core =
      static_cast<uint64_t>(0.3 * seastar::memory::stats().total_memory());
    return o;
  }
};
}  // namespace v
namespace YAML {
template <>
struct convert<seastar::sstring> {
  static Node
  encode(const seastar::sstring &rhs) {
    return Node(rhs.c_str());
  }
  static bool
  decode(const Node &node, seastar::sstring &rhs) {
    if (!node.IsScalar()) return false;
    rhs = node.as<std::string>();
    return true;
  }
};
template <>
struct convert<::v::redpanda_cfg> {
  static Node
  encode(const ::v::redpanda_cfg &rhs) {
    Node node;
    node["directory"] = rhs.directory;
    node["flush_period_ms"] = rhs.flush_period_ms;
    node["retention_size_bytes"] = rhs.retention_size_bytes;
    node["retention_period_hrs"] = rhs.retention_period_hrs;
    node["log_segment_size_bytes"] = rhs.log_segment_size_bytes;
    node["developer_mode"] = rhs.developer_mode;
    node["ip"] = rhs.ip;
    node["port"] = rhs.port;
    return node;
  }
  static bool
  decode(const Node &node, ::v::redpanda_cfg &rhs) {
    // Required fields
    for (auto s : {"directory", "port"}) {
      if (!node[s]) { return false; }
    }
    auto parse = [&node](const char *field, auto &v) {
      if (node[field]) {
        v = node[field].as<typename std::decay<decltype(v)>::type>();
      }
    };
    parse("directory", rhs.directory);
    parse("flush_period_ms", rhs.flush_period_ms);
    parse("retention_size_bytes", rhs.retention_size_bytes);
    parse("retention_period_hrs", rhs.retention_period_hrs);
    parse("log_segment_size_bytes", rhs.log_segment_size_bytes);
    parse("developer_mode", rhs.developer_mode);
    parse("ip", rhs.ip);
    parse("port", rhs.port);
    // get full path
    if (rhs.directory[0] == '~') {
      const char *tilde = ::getenv("HOME");
      rhs.directory = seastar::sstring(tilde) + rhs.directory.substr(1);
    }
    rhs.directory = std::filesystem::canonical(rhs.directory.c_str()).string();
    return true;
  }
};
}  // namespace YAML

namespace std {
static inline ostream &
operator<<(ostream &o, const ::v::redpanda_cfg &c) {
  o << "v::redpanda_cfg{directory:" << c.directory << ", ip:" << c.ip
    << ", port:" << c.port
    << ", retention_period_hrs:" << c.retention_period_hrs
    << ", flush_period_ms:" << c.flush_period_ms
    << ", log_segment_size_bytes:" << smf::human_bytes(c.log_segment_size_bytes)
    << ", developer_mode:" << c.developer_mode
    << ", retention_size_bytes:" << smf::human_bytes(c.retention_size_bytes)
    << ", writer_concurrency_pages:" << int32_t(c.writer_concurrency_pages)
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
}  // namespace std
