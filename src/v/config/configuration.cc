#include "config/configuration.h"

namespace config {

configuration::configuration()
  : data_directory(
    *this,
    "data_directory",
    "Place where redpanda will keep the data",
    required::yes)
  , developer_mode(
      *this,
      "developer_mode",
      "Skips most of the checks performed at startup, not recomended for "
      "production use",
      required::no,
      false)
  , log_segment_size(
      *this,
      "log_segment_size",
      "How large in bytes should each log segment be (1<<31)",
      required::no,
      1 << 31)
  , rpc_server(
      *this,
      "rpc_server",
      "IpAddress and port for RPC server",
      required::no,
      socket_address(net::inet_address("127.0.0.1"), 33145))
  , node_id(
      *this,
      "node_id",
      "Unique id identifying a node in the cluster",
      required::yes)
  , seed_server_meta_topic_partitions(
      *this,
      "seed_server_meta_topic_partitions",
      "Number of partitions in internal raft metadata topic",
      required::no,
      7)
  , raft_timeout(
      *this,
      "raft_timeout",
      "Milliseconds for raft leader election/heartbeats",
      required::no,
      std::chrono::milliseconds(150))
  , seed_servers(
      *this,
      "seed_servers",
      "Seed server address in form <address>:<rpc_port>",
      required::yes)
  , min_version(*this, "min_version", "TBD", required::no, 0)
  , max_version(*this, "max_version", "TBD", required::no, 1)
  , kafka_api(
      *this,
      "kafka_api",
      "Address and port of an interface to listen for Kafka API requests",
      required::no,
      socket_address(net::inet_address("127.0.0.1"), 9092))
  , kafka_api_tls(
      *this,
      "kafka_api_tls",
      "TLS configuration for Kafka API endpoint",
      required::no,
      tls_config(),
      tls_config::validate)
  , use_scheduling_groups(
      *this,
      "use_scheduling_groups",
      "Manage CPU scheduling",
      required::no,
      false)
  , admin(
      *this,
      "admin",
      "Address and port of admin server",
      required::no,
      socket_address(net::inet_address("127.0.0.1"), 9644))
  , enable_admin_api(
      *this, "enable_admin_api", "Enable the admin API", required::no, true)
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Admin API doc directory",
      required::no,
      "/etc/redpanda/admin-api-doc")
  , default_num_windows(
      *this,
      "default_num_windows",
      "Default number of quota tracking windows",
      required::no,
      10)
  , default_window_sec(
      *this,
      "default_window_sec",
      "Default quota tracking window size in milliseconds",
      required::no,
      std::chrono::milliseconds(1000))
  , quota_manager_gc_sec(
      *this,
      "quota_manager_gc_sec",
      "Quota manager GC frequency in milliseconds",
      required::no,
      std::chrono::milliseconds(30000))
  , target_quota_byte_rate(
      *this,
      "target_quota_byte_rate",
      "Target quota byte rate (bytes per second)",
      required::no,
      10 << 20)
  , _advertised_kafka_api(
      *this,
      "advertised_kafka_api",
      "Address of Kafka API published to the clients",
      required::no,
      std::nullopt)
  , rack(
      *this,
      "rack",
      "Rack identifier",
      required::no,
      std::nullopt) {
}

void configuration::read_yaml(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda'root is required");
    }
    config_store::read_yaml(root_node["redpanda"]);
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace config
