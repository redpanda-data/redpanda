/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/format_to.h"
#include "config/bounded_property.h"
#include "config/config_store.h"
#include "config/tls_config.h"
#include "net/tls.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <chrono>

namespace kafka::client {
struct configuration;

using acks = named_type<int16_t, struct acks_tag>;

static constexpr acks acks_none{0};
static constexpr acks acks_leader{1};
static constexpr acks acks_all{-1};
/**
 * Producer specific properties.
 */
struct producer_configuration {
    int32_t batch_record_count;
    int32_t batch_size_bytes;
    std::chrono::milliseconds batch_delay;
    model::compression compression_type;
    std::chrono::milliseconds shutdown_delay;
    acks ack_level;

    static producer_configuration from_config_store(const configuration& cfg);
};

/**
 * Consumer specific properties.
 */

struct consumer_configuration {
    std::chrono::milliseconds request_timeout;
    int32_t fetch_min_bytes;
    int32_t fetch_max_bytes;
    std::chrono::milliseconds session_timeout;
    std::chrono::milliseconds rebalance_timeout;
    std::chrono::milliseconds heartbeat_interval;
    static consumer_configuration from_config_store(const configuration& cfg);
};
/**
 * Common configuration for retries.
 */
struct retries_configuration {
    size_t max_retries;
    std::chrono::milliseconds retry_base_backoff;
};
struct tls_configuration {
    std::optional<net::certificate> truststore;
    std::optional<net::key_store> k_store;
    static std::optional<tls_configuration>
    from_tls_config(const config::tls_config&);

    ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
    build_credentials() const;

    bool provide_sni_hostname{false};

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool
    operator==(const tls_configuration&, const tls_configuration&) = default;
};

struct sasl_configuration {
    ss::sstring mechanism;
    ss::sstring username;
    ss::sstring password;

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool
    operator==(const sasl_configuration&, const sasl_configuration&) = default;
};

/**
 * Connection configuration for the Kafka client. If the TLS or SASL settings
 * are present,they will be used to connect to the brokers.
 */
struct connection_configuration {
    using include_authorized_ops_t
      = ss::bool_class<struct include_authorized_ops_tag>;
    std::vector<net::unresolved_address> initial_brokers;
    std::optional<tls_configuration> broker_tls;
    std::optional<sasl_configuration> sasl_cfg;
    std::optional<ss::sstring> client_id;
    std::chrono::milliseconds max_metadata_age{10000};
    std::chrono::milliseconds connection_timeout{1000};
    // If set to true, will request from the brokers the bitmask of authorized
    // operations the authenticated user is permitted to perform
    include_authorized_ops_t include_authorized_operations{
      include_authorized_ops_t::yes};

    ss::sstring get_client_id() const {
        return client_id.value_or("kafka-client");
    }
    static connection_configuration from_config_store(const configuration& cfg);

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool
    operator==(const connection_configuration&, const connection_configuration&)
      = default;
};

/**
 * Currently the `kafka::client::client is an aggregate of consumer and
 * producer. Client configuration holds all necessary properties.
 */
struct client_configuration {
    connection_configuration connection_cfg;
    producer_configuration producer_cfg;
    consumer_configuration consumer_cfg;
    retries_configuration retries_cfg;
    static client_configuration from_config_store(const configuration& cfg);
};

/// Pandaproxy client configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.
struct configuration final : public config::config_store {
    config::property<std::vector<net::unresolved_address>> brokers;
    config::property<config::tls_config> broker_tls;
    config::property<size_t> retries;
    config::property<std::chrono::milliseconds> retry_base_backoff;
    config::property<int32_t> produce_batch_record_count;
    config::property<int32_t> produce_batch_size_bytes;
    config::property<std::chrono::milliseconds> produce_batch_delay;
    config::property<ss::sstring> produce_compression_type;
    config::property<std::chrono::milliseconds> produce_shutdown_delay;
    config::property<int16_t> produce_ack_level;
    config::property<std::chrono::milliseconds> consumer_request_timeout;
    config::bounded_property<int32_t> consumer_request_min_bytes;
    config::bounded_property<int32_t> consumer_request_max_bytes;
    config::property<std::chrono::milliseconds> consumer_session_timeout;
    config::property<std::chrono::milliseconds> consumer_rebalance_timeout;
    config::property<std::chrono::milliseconds> consumer_heartbeat_interval;

    config::property<ss::sstring> sasl_mechanism;
    config::property<ss::sstring> scram_username;
    config::property<ss::sstring> scram_password;

    config::property<std::optional<ss::sstring>> client_identifier;

    configuration();
    explicit configuration(const YAML::Node& cfg);
};

} // namespace kafka::client
