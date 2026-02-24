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

#pragma once

#include "kafka/client/configuration.h"
#include "kafka/client/fwd.h"
#include "pandaproxy/schema_registry/transport.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace cluster {
class controller;
class security_frontend;
} // namespace cluster

namespace pandaproxy::schema_registry {

/// Transport implementation that wraps kafka::client::client for schema
/// registry internal topic I/O. This is the legacy/fallback transport.
class kafka_client_transport final : public transport {
public:
    kafka_client_transport(
      ss::sharded<kafka::client::client>& client,
      const YAML::Node& client_config,
      std::unique_ptr<cluster::controller>& controller);

    ss::future<produce_result> produce(model::record_batch batch) override;
    ss::future<model::offset> get_high_watermark() override;
    ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<ss::future<>(model::record_batch)> consumer)
      override;

    ss::future<> configure() override;
    ss::future<> mitigate_error(std::exception_ptr eptr) override;
    ss::future<>
    validate_topic_creation_authorization(int16_t replication_factor) override;
    bool has_ephemeral_credentials() const override;

private:
    ss::future<> inform(model::node_id);
    ss::future<> do_inform(model::node_id);
    bool shadow_linking_active() const;

    ss::sharded<kafka::client::client>& _client;
    kafka::client::configuration _client_config;
    std::unique_ptr<cluster::controller>& _controller;
    bool _has_ephemeral_credentials{false};
};

} // namespace pandaproxy::schema_registry
