/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "raft/recovery_memory_quota.h"

#include "raft/logger.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/semaphore.h"
#include "vlog.h"

#include <seastar/core/memory.hh>

namespace raft {

recovery_memory_quota::recovery_memory_quota(
  recovery_memory_quota::config_provider_fn config_provider)
  : _cfg(config_provider())
  , _current_max_recovery_mem(
      _cfg.max_recovery_memory().value_or(memory_groups::recovery_max_memory()))
  , _memory(_current_max_recovery_mem, "raft/recovery-quota") {
    _cfg.max_recovery_memory.watch([this] { on_max_memory_changed(); });
}

ss::future<ssx::semaphore_units> recovery_memory_quota::acquire_read_memory() {
    return ss::get_units(
      _memory,
      std::min(_current_max_recovery_mem, _cfg.default_read_buffer_size()));
}

void recovery_memory_quota::on_max_memory_changed() {
    int64_t new_size = _cfg.max_recovery_memory().value_or(
      memory_groups::recovery_max_memory());

    vlog(raftlog.info, "max recovery memory changed to {} bytes", new_size);

    int64_t diff = new_size - static_cast<int64_t>(_current_max_recovery_mem);
    if (diff < 0) {
        _memory.consume(-diff);
    } else if (diff > 0) {
        _memory.signal(diff);
    }
    _current_max_recovery_mem = new_size;
}

} // namespace raft
