/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/metrics_reporter.h"
#include "kafka/client/configuration.h"
#include "model/metadata.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "security/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace YAML {
class Node;
}

namespace cluster {
class controller;
class metadata_cache;
} // namespace cluster

namespace schema {
class registry;
}

namespace pandaproxy::schema_registry {

class api {
public:
    api(
      model::node_id node_id,
      ss::smp_service_group sg,
      size_t max_memory,
      kafka::client::configuration& client_cfg,
      configuration& cfg,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      std::unique_ptr<cluster::controller>&,
      ss::sharded<security::audit::audit_log_manager>&) noexcept;
    ~api() noexcept;

    ss::future<> start();
    ss::future<> stop();
    ss::future<> restart();

    const configuration& get_config() const;
    const kafka::client::configuration& get_client_config() const;

    bool has_ephemeral_credentials() const;

    /// Contributes Schema Registry metrics to the metrics snapshot.
    ss::future<>
    contribute_metrics(cluster::metrics_reporter::metrics_snapshot&) const;

private:
    friend class schema_id_validator;
    friend class schema::registry;
    model::node_id _node_id;
    ss::smp_service_group _sg;
    size_t _max_memory;
    kafka::client::configuration& _client_cfg;
    configuration& _cfg;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;

    ss::sharded<kafka_client_transport> _transport;
    std::unique_ptr<pandaproxy::schema_registry::sharded_store> _store;
    ss::sharded<schema_id_validation_probe> _schema_id_validation_probe;
    ss::sharded<schema_id_cache> _schema_id_cache;
    ss::sharded<pandaproxy::schema_registry::service> _service;
    ss::sharded<pandaproxy::schema_registry::seq_writer> _sequencer;
    ss::sharded<security::audit::audit_log_manager>& _audit_mgr;

    // Metrics telemetry support - only used on shard 0
    ss::gate _metrics_gate{};
    std::optional<cluster::metrics_reporter::metrics_contributor_id>
      _metrics_contributor_id{};
};

} // namespace pandaproxy::schema_registry

namespace google::protobuf {

class FileDescriptor;

}
