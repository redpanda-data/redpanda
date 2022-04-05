// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/event_manager.h"

#include "raft/consensus.h"

namespace raft {

ss::future<> event_manager::start() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this] {
              // we won't miss an update because notify is synchronous. If there
              // is a new commit index getting ready to be set, it won't signal
              // until after we've waited.
              _commit_index.notify(_consensus->committed_offset());
              return _cond.wait();
          });
    });
    return ss::now();
}

ss::future<> event_manager::stop() {
    auto f = _gate.close();
    _cond.signal();
    _commit_index.stop();
    return f;
}

ss::future<> event_manager::wait(
  model::offset offset,
  model::timeout_clock::time_point timeout,
  ss::abort_source& as) {
    return _commit_index.wait(offset, timeout, as);
}

void event_manager::notify_commit_index() { _cond.signal(); }

} // namespace raft
