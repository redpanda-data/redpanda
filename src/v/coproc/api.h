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

#include "cluster/fwd.h"
#include "coproc/fwd.h"
#include "coproc/sys_refs.h"
#include "net/unresolved_address.h"

#include <seastar/core/abort_source.hh>

namespace coproc {
class api {
public:
    api(
      net::unresolved_address,
      ss::sharded<storage::api>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coproc::partition_manager>&) noexcept;

    ~api();

    ss::future<> start();

    ss::future<> stop();

    ss::sharded<pacemaker>& get_pacemaker() { return _pacemaker; }

private:
    net::unresolved_address _engine_addr;

    ss::sharded<wasm::script_database> _sdb; // one instance
    ss::sharded<pacemaker> _pacemaker;       /// one per core
    ss::sharded<cluster::non_replicable_topics_frontend>
      _mt_frontend; /// one instance
    ss::sharded<reconciliation_backend>
      _reconciliation_backend; /// one per core

    sys_refs _rs;
    ss::abort_source _as;

    std::unique_ptr<wasm::script_dispatcher> _dispatcher;
    std::unique_ptr<wasm::event_listener> _listener;
    std::unique_ptr<wasm::async_event_handler> _wasm_async_handler;
};

} // namespace coproc
