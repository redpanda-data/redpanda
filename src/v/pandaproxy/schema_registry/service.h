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

#include "kafka/client/client.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/server.h"
#include "pandaproxy/util.h"
#include "seastarx.h"
#include "utils/request_auth.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>

#include <vector>

namespace cluster {
class controller;
}

namespace pandaproxy::schema_registry {

class service : public ss::peering_sharded_service<service> {
public:
    service(
      const YAML::Node& config,
      ss::smp_service_group smp_sg,
      size_t max_memory,
      ss::sharded<kafka::client::client>& client,
      sharded_store& store,
      ss::sharded<seq_writer>& sequencer,
      std::unique_ptr<cluster::controller>&);

    ss::future<> start();
    ss::future<> stop();

    configuration& config();
    kafka::client::configuration& client_config();
    ss::sharded<kafka::client::client>& client() { return _client; }
    seq_writer& writer() { return _writer.local(); }
    sharded_store& schema_store() { return _store; }
    request_authenticator& authenticator() { return _auth; }
    ss::future<> mitigate_error(std::exception_ptr);

private:
    ss::future<> do_start();
    ss::future<> configure();
    ss::future<> inform(model::node_id);
    ss::future<> do_inform(model::node_id);
    ss::future<> create_internal_topic();
    ss::future<> fetch_internal_topic();
    configuration _config;
    ssx::semaphore _mem_sem;
    ss::gate _gate;
    ss::sharded<kafka::client::client>& _client;
    ctx_server<service>::context_t _ctx;
    ctx_server<service> _server;
    sharded_store& _store;
    ss::sharded<seq_writer>& _writer;
    std::unique_ptr<cluster::controller>& _controller;

    one_shot _ensure_started;
    request_authenticator _auth;
    bool _has_ephemeral_credentials{false};
    bool _is_started{false};
};

} // namespace pandaproxy::schema_registry
