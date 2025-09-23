/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/utils.h"

#include "cluster_link/model/types.h"
#include "kafka/client/configuration.h"

namespace cluster_link {
namespace {

struct tls_visitor {
    net::key_store operator()(
      const model::tls_file_path& key, const model::tls_file_path& cert) {
        return net::key_cert_path{
          .key = std::filesystem::path{key()},
          .cert = std::filesystem::path{cert()}};
    }
    net::key_store
    operator()(const model::tls_value& key, const model::tls_value& cert) {
        return net::key_cert{.key = key(), .cert = cert()};
    }

    template<typename T1, typename T2>
    requires(!std::is_same_v<T1, T2>)
    net::key_store operator()(const T1&, const T2&) {
        vassert(false, "TLS key and cert must be of the same type");
    }
};

kafka::client::tls_configuration
create_tls_configuration(const model::connection_config& link) {
    kafka::client::tls_configuration tls_cfg;

    if (link.ca.has_value()) {
        tls_cfg.truststore = ss::visit(
          link.ca.value(),
          [](const model::tls_file_path& path) {
              return net::certificate{std::filesystem::path{path()}};
          },
          [](const model::tls_value& value) {
              return net::certificate{value()};
          });
    }

    if (link.key.has_value() && link.cert.has_value()) {
        tls_cfg.k_store = std::visit(
          tls_visitor{}, link.key.value(), link.cert.value());
    }

    return tls_cfg;
}

kafka::client::sasl_configuration
create_sasl_config(const model::connection_config::authn_variant& authn) {
    return ss::visit(
      authn,
      [](const model::scram_credentials& scram)
        -> kafka::client::sasl_configuration {
          return {
            .mechanism = scram.mechanism,
            .username = scram.username,
            .password = scram.password};
      });
}
} // namespace
kafka::client::connection_configuration
metadata_to_kafka_config(const model::metadata& md) {
    kafka::client::connection_configuration config;
    const auto& link_connection = md.connection;

    config.initial_brokers = link_connection.bootstrap_servers;

    if (
      link_connection.cert.has_value() || link_connection.key.has_value()
      || link_connection.ca.has_value()) {
        config.broker_tls = create_tls_configuration(link_connection);
    }

    if (link_connection.authn_config.has_value()) {
        config.sasl_cfg = create_sasl_config(
          link_connection.authn_config.value());
    }

    config.client_id = link_connection.client_id;

    return config;
}
} // namespace cluster_link
