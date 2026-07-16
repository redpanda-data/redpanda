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
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/server.h"
#include "pandaproxy/util.h"
#include "security/fwd.h"
#include "security/request_auth.h"
#include "utils/adjustable_semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>

#include <memory>

namespace cluster {
class controller;
}
namespace kafka::data::rpc {
class topic_metadata_cache;
} // namespace kafka::data::rpc
namespace pandaproxy::schema_registry {

class service : public ss::peering_sharded_service<service> {
public:
    service(
      const YAML::Node& config,
      ss::smp_service_group smp_sg,
      size_t max_memory,
      transport& transport,
      sharded_store& store,
      ss::sharded<seq_writer>& sequencer,
      std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
        topic_metadata_cache,
      std::unique_ptr<cluster::controller>&,
      ss::sharded<security::audit::audit_log_manager>& audit_mgr);

    ss::future<> start();
    ss::future<> stop();

    configuration& config();
    seq_writer& writer() { return _writer.local(); }
    sharded_store& schema_store() { return _store; }
    request_authenticator& authenticator() { return _auth; }
    security::authorizer& authorizor();
    ss::future<> ensure_started() { return _ensure_started(); }
    /// Creates the internal `_schemas` topic if it does not exist, without
    /// running the full start-up (no store load). Idempotent.
    ss::future<> ensure_internal_topic();
    security::audit::audit_log_manager& audit_mgr() {
        return _audit_mgr.local();
    }

    std::unique_ptr<cluster::controller>& controller() { return _controller; }

private:
    ss::future<> do_start();
    /// Route every shard's start-up through a single one_shot on the reader
    /// shard, so the `_schemas` topic is replayed exactly once regardless of
    /// how many shards receive their first request concurrently. See the
    /// definition for why the per-shard one_shots would otherwise race.
    ss::future<> ensure_topic_loaded();
    ss::future<> create_internal_topic();
    ss::future<> fetch_internal_topic();
    configuration _config;
    ssx::semaphore _mem_sem;
    adjustable_semaphore _inflight_sem;
    config::binding<size_t> _inflight_config_binding;
    ss::gate _gate;
    transport* _transport;
    ctx_server<service>::context_t _ctx;
    ctx_server<service> _server;
    sharded_store& _store;
    ss::sharded<seq_writer>& _writer;
    std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
      _topic_metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;
    ss::sharded<security::audit::audit_log_manager>& _audit_mgr;
    ss::abort_source _as;

    // Per-shard: caches that start-up has completed on this shard, giving a
    // cheap fast path for subsequent requests. Its action delegates to
    // `_load_once` on the reader shard.
    one_shot _ensure_started;
    // Reader shard only: the single authority that runs `do_start()` exactly
    // once, no matter how many shards enter start-up concurrently.
    one_shot _load_once;
    request_authenticator _auth;
    bool _is_started{false};
};

} // namespace pandaproxy::schema_registry
