/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/api.h"

#include "cluster/non_replicable_topics_frontend.h"
#include "coproc/event_handler.h"
#include "coproc/event_listener.h"
#include "coproc/pacemaker.h"
#include "coproc/partition_manager.h"
#include "coproc/reconciliation_backend.h"
#include "coproc/script_database.h"
#include "coproc/script_dispatcher.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

api::api(
  unresolved_address addr,
  ss::sharded<storage::api>& storage,
  ss::sharded<cluster::topic_table>& topic_table,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<coproc::partition_manager>& cp_partition_manager) noexcept
  : _engine_addr(std::move(addr))
  , _rs(sys_refs{
      .storage = storage,
      .topic_table = topic_table,
      .shard_table = shard_table,
      .mt_frontend = _mt_frontend,
      .topics_frontend = topics_frontend,
      .metadata_cache = metadata_cache,
      .partition_manager = partition_manager,
      .cp_partition_manager = cp_partition_manager}) {}

api::~api() = default;

ss::future<> api::start() {
    co_await _reconciliation_backend.start(
      std::ref(_rs.topic_table),
      std::ref(_rs.shard_table),
      std::ref(_rs.partition_manager),
      std::ref(_rs.cp_partition_manager),
      std::ref(_pacemaker),
      std::ref(_sdb));

    co_await _reconciliation_backend.invoke_on_all(
      &coproc::reconciliation_backend::start);

    co_await _sdb.start_single();
    co_await _mt_frontend.start_single(std::ref(_rs.topics_frontend));
    co_await _pacemaker.start(_engine_addr, std::ref(_rs));
    co_await _pacemaker.invoke_on_all(&coproc::pacemaker::start);

    vassert(!_listener, "nullptr expected");
    vassert(!_dispatcher, "nullptr expected");
    vassert(!_wasm_async_handler, "nullptr expected");
    _listener = std::make_unique<wasm::event_listener>(_as);
    _dispatcher = std::make_unique<wasm::script_dispatcher>(
      _pacemaker, _sdb, _as);
    _wasm_async_handler = std::make_unique<coproc::wasm::async_event_handler>(
      std::ref(*_dispatcher));
    _listener->register_handler(
      coproc::wasm::event_type::async, _wasm_async_handler.get());
    co_await _listener->start();
}

ss::future<> api::stop() {
    co_await _reconciliation_backend.stop();
    co_await _listener->stop();
    co_await _pacemaker.stop();
    co_await _mt_frontend.stop();
    co_await _sdb.stop();
}

} // namespace coproc
