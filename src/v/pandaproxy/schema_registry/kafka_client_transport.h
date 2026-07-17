/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fwd.h"
#include "pandaproxy/schema_registry/transport.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace kafka::data::rpc {
class topic_creator;
} // namespace kafka::data::rpc

namespace pandaproxy::schema_registry {

/// Transport implementation that wraps kafka::client::client for schema
/// registry internal topic I/O. This is the legacy/fallback transport.
class kafka_client_transport final : public transport {
public:
    kafka_client_transport(
      kafka::client::configuration& client_config,
      cluster::controller& controller,
      std::unique_ptr<kafka::data::rpc::topic_creator> topic_creator);
    ~kafka_client_transport() final;

    ss::future<> stop() final;

    ss::future<produce_result> produce(model::record_batch batch) override;
    ss::future<model::offset> get_high_watermark() override;
    ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<
        ss::future<ss::stop_iteration>(model::record_batch)> consumer) override;

    ss::future<> configure();
    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      int32_t partition_count,
      cluster::topic_properties,
      int16_t replication_factor) override;
    bool has_ephemeral_credentials() const;

private:
    ss::future<> mitigate_error(std::exception_ptr eptr);
    ss::future<> inform(model::node_id);
    ss::future<> do_inform(model::node_id);
    ss::future<>
    validate_topic_creation_authorization(int16_t replication_factor);
    bool shadow_linking_active() const;

    kafka::client::configuration& _client_config;
    cluster::controller& _controller;
    std::unique_ptr<kafka::client::client> _client;
    std::unique_ptr<kafka::data::rpc::topic_creator> _topic_creator;
    bool _has_ephemeral_credentials{false};
    ss::abort_source _as;
};

} // namespace pandaproxy::schema_registry
