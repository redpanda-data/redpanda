#include "configuration.h"

#include "units.h"

namespace pandaproxy::client {
using namespace std::chrono_literals;

configuration::configuration()
  : brokers(
    *this,
    "brokers",
    "List of address and port of the brokers",
    config::required::yes,
    std::vector<unresolved_address>({{"127.0.0.1", 9092}}))
  , broker_tls(
      *this,
      "broker_tls",
      "TLS configuration for the brokers",
      config::required::no,
      config::tls_config(),
      config::tls_config::validate)
  , retries(
      *this,
      "retries",
      "Number of times to retry a request to a broker",
      config::required::no,
      5)
  , retry_base_backoff(
      *this,
      "retry_base_backoff_ms",
      "Delay (in milliseconds) for initial retry backoff",
      config::required::no,
      100ms)
  , produce_batch_record_count(
      *this,
      "produce_batch_record_count",
      "Number of records to batch before sending to broker",
      config::required::no,
      1000)
  , produce_batch_size_bytes(
      *this,
      "produce_batch_size_bytes",
      "Number of bytes to batch before sending to broker",
      config::required::no,
      1048576)
  , produce_batch_delay(
      *this,
      "produce_batch_delay_ms",
      "Delay (in milliseconds) to wait before sending batch",
      config::required::no,
      100ms) {}

void configuration::read_yaml(const YAML::Node& root_node) {
    if (!root_node["pandaproxy_client"]) {
        throw std::invalid_argument("'pandaproxy_client' root is required");
    }
    config_store::read_yaml(root_node["pandaproxy_client"]);
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace pandaproxy::client
