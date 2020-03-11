#include "config/configuration.h"

namespace config {
using namespace std::chrono_literals;

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
      unresolved_address("127.0.0.1", 33145))
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
  , raft_heartbeat_interval(
      *this,
      "raft_heartbeat_interval",
      "Milliseconds for raft leader heartbeats",
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
      unresolved_address("127.0.0.1", 9092))
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
      unresolved_address("127.0.0.1", 9644))
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
  , rack(*this, "rack", "Rack identifier", required::no, std::nullopt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering metrics",
      required::no,
      false)
  , _advertised_rpc_api(
      *this,
      "advertised_rpc_api",
      "Address of RPC endpoint published to other cluster members",
      required::no,
      std::nullopt)
  , group_min_session_timeout_ms(
      *this,
      "group_min_session_timeout_ms",
      "The minimum allowed session timeout for registered consumers. Shorter "
      "timeouts result in quicker failure detection at the cost of more "
      "frequent "
      "consumer heartbeating, which can overwhelm broker resources.",
      required::no,
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      "group_max_session_timeout_ms",
      "The maximum allowed session timeout for registered consumers. Longer "
      "timeouts give consumers more time to process messages in between "
      "heartbeats at the cost of a longer time to detect failures. "
      "Default quota tracking window size in milliseconds",
      required::no,
      30'000ms)
  , group_initial_rebalance_delay(
      *this,
      "group_initial_rebalance_delay",
      "Extra delay (ms) added to rebalance phase to wait for new members",
      required::no,
      300ms)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins",
      required::no,
      30'000ms)
  , metadata_dissemination_interval(
      *this,
      "metadata_dissemination_interval",
      "Interaval for metadata dissemination batching",
      required::no,
      3'000ms)
  , delete_retention_ms(
      *this,
      "delete_retention_ms",
      "delete segments older than this",
      required::no,
      86400000ms)
  , retention_bytes(
      *this,
      "retention_bytes",
      "max bytes per partition on disk before triggering a compaction",
      required::no,
      std::nullopt)
  , group_topic_partitions(
      *this,
      "group_topic_partitions",
      "Number of partitions in the internal group membership topic",
      required::no,
      1)
  , default_topic_replication(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics",
      required::no,
      1)
  , create_topic_timeout_ms(
      *this,
      "create_topic_timeout_ms",
      "Timeout (ms) to wait for new topic creation",
      required::no,
      2'000ms)
  , wait_for_leader_timeout_ms(
      *this,
      "wait_for_leader_timeout_ms",
      "Timeout (ms) to wait for leadership in metadata cache",
      required::no,
      5'000ms)
  , default_topic_partitions(
      *this,
      "default_topic_partitions",
      "Default number of partitions per topic",
      required::no,
      1)

{}

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
