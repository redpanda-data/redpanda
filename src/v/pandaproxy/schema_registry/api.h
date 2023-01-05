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

#include "cluster/controller_api.h"
#include "kafka/client/fwd.h"
#include "model/metadata.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace YAML {
class Node;
}

namespace cluster {
class controller;
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
      std::unique_ptr<cluster::controller>&) noexcept;
    ~api() noexcept;

    ss::future<> start();
    ss::future<> stop();
    ss::future<> restart();

private:
    model::node_id _node_id;
    ss::smp_service_group _sg;
    size_t _max_memory;
    kafka::client::configuration& _client_cfg;
    configuration& _cfg;
    std::unique_ptr<cluster::controller>& _controller;

    ss::sharded<kafka::client::client> _client;
    std::unique_ptr<pandaproxy::schema_registry::sharded_store> _store;
    ss::sharded<pandaproxy::schema_registry::service> _service;
    ss::sharded<pandaproxy::schema_registry::seq_writer> _sequencer;
};

} // namespace pandaproxy::schema_registry

namespace google::protobuf {

class FileDescriptor;

}
