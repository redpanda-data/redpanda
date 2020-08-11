#pragma once
#include "config/config_store.h"
#include "config/configuration.h"
#include "config/tls_config.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

namespace pandaproxy {

/// Pandaproxy configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.
struct configuration final : public config::config_store {
    config::property<bool> developer_mode;

    config::property<std::vector<unresolved_address>> brokers;
    config::property<config::tls_config> broker_tls;
    config::property<unresolved_address> pandaproxy_api;
    config::property<unresolved_address> admin_api;
    config::property<bool> enable_admin_api;
    config::property<ss::sstring> admin_api_doc_dir;
    config::property<ss::sstring> api_doc_dir;
    config::property<bool> disable_metrics;

    configuration();

    void read_yaml(const YAML::Node& root_node) override;
};

configuration& shard_local_cfg();

using conf_ref = typename std::reference_wrapper<configuration>;

} // namespace pandaproxy
