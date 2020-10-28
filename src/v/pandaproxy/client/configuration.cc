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
      config::tls_config::validate) {}

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
