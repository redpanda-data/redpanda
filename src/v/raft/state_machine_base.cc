// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/state_machine_base.h"

#include "base/vassert.h"
#include "raft/consensus.h"

#include <seastar/core/coroutine.hh>

namespace raft {

ss::future<> state_machine_base::apply(const model::record_batch& b) {
    auto units = co_await _apply_lock.get_units();
    co_await apply(b, units);
    set_next(model::next_offset(b.last_offset()));
}

void state_machine_base::set_next(model::offset offset) {
    vassert(
      offset >= _next,
      "can not move next offset backward, current: {}, requested: {}",
      _next,
      offset);
    _next = offset;
    _waiters.notify(model::prev_offset(offset));
}

ss::future<> state_machine_base::stop() {
    _apply_lock.broken();
    _waiters.stop();
    co_return;
}

ss::future<> state_machine_base::wait(
  model::offset offset,
  model::timeout_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    co_await _waiters.wait(offset, timeout, as);
}

} // namespace raft
