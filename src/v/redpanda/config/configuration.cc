#include "redpanda/config/configuration.h"

namespace config {

configuration::configuration()
  : data_directory(
    *this,
    "data_directory",
    "Place where redpanda will keep the data",
    required::yes)
  , log_segment_size_bytes(
      *this,
      "log_segment_size",
      "Size of single log segment in bytes",
      required::no,
      1024 * 1024 * 1024)
  , developer_mode(
      *this,
      "developer_mode",
      "Skips most of the checks performed at startup, not recomended for "
      "production use",
      required::no,
      false)
  , rpc_server(
      *this,
      "rpc_server",
      "IpAddress and port for RPC server",
      required::no,
      socket_address(inet_addr("127.0.0.1"), 33145))
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
      socket_address(inet_addr("127.0.0.1"), 9092))
  , max_retention_period_hours(
      *this, "max_retention_period_hours", "TBD", required::no, 168)
  , writer_flush_period_ms(
      *this, "writer_flush_period_ms", "TBD", required::no, 8000)
  , max_retention_size(*this, "max_retention_size", "TBD", required::no, -1)
  , max_bytes_in_writer_cache(
      *this, "max_bytes_in_writer_cache", "TBD", required::no, 1024 * 1024) {
}

void configuration::read_yaml(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda'root is required");
    }
    config_store::read_yaml(root_node["redpanda"]);
}
} // namespace config
