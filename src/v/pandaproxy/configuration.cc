#include "configuration.h"

#include "units.h"

namespace pandaproxy {
using namespace std::chrono_literals;

configuration::configuration()
  : developer_mode(
    *this,
    "developer_mode",
    "Skips most of the checks performed at startup, not recomended for "
    "production use",
    config::required::no,
    false)
  , brokers(
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
  , pandaproxy_api(
      *this,
      "pandaproxy_api",
      "Rest API listen address and port",
      config::required::no,
      unresolved_address("127.0.0.1", 8082))
  , admin_api(
      *this,
      "admin_api",
      "Admin server listen address and port",
      config::required::no,
      unresolved_address("127.0.0.1", 9644))
  , enable_admin_api(
      *this,
      "enable_admin_api",
      "Enable the admin API",
      config::required::no,
      true)
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Admin API doc directory",
      config::required::no,
      "/etc/pandaproxy/admin-api-doc")
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "API doc directory",
      config::required::no,
      "/etc/pandaproxy/api-doc")
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering metrics",
      config::required::no,
      false) {}

void configuration::read_yaml(const YAML::Node& root_node) {
    if (!root_node["pandaproxy"]) {
        throw std::invalid_argument("'pandaproxy' root is required");
    }
    config_store::read_yaml(root_node["pandaproxy"]);
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace pandaproxy
