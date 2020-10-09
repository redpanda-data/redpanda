#pragma once
#include "config/config_store.h"
#include "config/configuration.h"
#include "config/tls_config.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <chrono>

namespace pandaproxy::client {

/// Pandaproxy client configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.
struct configuration final : public config::config_store {
    config::property<std::vector<unresolved_address>> brokers;
    config::property<config::tls_config> broker_tls;
    config::property<size_t> retries;
    config::property<std::chrono::milliseconds> retry_base_backoff;
    config::property<int32_t> produce_batch_record_count;
    config::property<int32_t> produce_batch_size_bytes;
    config::property<std::chrono::milliseconds> produce_batch_delay;

    configuration();

    void read_yaml(const YAML::Node& root_node) override;
};

configuration& shard_local_cfg();

using conf_ref = typename std::reference_wrapper<configuration>;

} // namespace pandaproxy::client
