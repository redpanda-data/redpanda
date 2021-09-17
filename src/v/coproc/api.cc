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

#include "coproc/event_listener.h"
#include "coproc/pacemaker.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

api::api(
  unresolved_address addr,
  ss::sharded<storage::api>& storage,
  ss::sharded<cluster::partition_manager>& partition_manager) noexcept
  : _engine_addr(std::move(addr))
  , _rs(sys_refs{.storage = storage, .partition_manager = partition_manager}) {}

api::~api() = default;

ss::future<> api::start() {
    co_await _pacemaker.start(_engine_addr, std::ref(_rs));
    co_await _pacemaker.invoke_on_all(&coproc::pacemaker::start);
    _listener = std::make_unique<wasm::event_listener>(_pacemaker);
    co_await _listener->start();
}

ss::future<> api::stop() {
    auto f = ss::now();
    if (_listener) {
        f = _listener->stop();
    }
    return f.then([this] { return _pacemaker.stop(); });
}

} // namespace coproc
