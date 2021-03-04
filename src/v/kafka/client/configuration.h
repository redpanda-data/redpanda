/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/config_store.h"
#include "config/tls_config.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <chrono>

namespace kafka::client {

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
    config::property<std::chrono::milliseconds> consumer_session_timeout;
    config::property<std::chrono::milliseconds> consumer_rebalance_timeout;
    config::property<std::chrono::milliseconds> consumer_heartbeat_interval;

    configuration();
};

YAML::Node to_yaml(const configuration& cfg);
configuration copy_configuration(const configuration& cfg);

using conf_ref = typename std::reference_wrapper<configuration>;

} // namespace kafka::client
