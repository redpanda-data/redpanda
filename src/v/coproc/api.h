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

#include "coproc/fwd.h"
#include "coproc/sys_refs.h"
#include "utils/unresolved_address.h"
namespace coproc {

class api {
public:
    api(
      unresolved_address,
      ss::sharded<storage::api>&,
      ss::sharded<cluster::partition_manager>&) noexcept;

    ~api();

    ss::future<> start();

    ss::future<> stop();

    ss::sharded<pacemaker>& get_pacemaker() { return _pacemaker; }

private:
    unresolved_address _engine_addr;
    sys_refs _rs;
    std::unique_ptr<wasm::event_listener> _listener; /// one instance
    ss::sharded<pacemaker> _pacemaker;               /// one per core
};

} // namespace coproc
