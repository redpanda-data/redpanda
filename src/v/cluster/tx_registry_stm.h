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

#pragma once

#include "cluster/fwd.h"
#include "cluster/persisted_stm.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace config {
struct configuration;
}

namespace cluster {

class tx_registry_stm final : public persisted_stm<> {
public:
    explicit tx_registry_stm(ss::logger&, raft::consensus*);

    explicit tx_registry_stm(
      ss::logger&, raft::consensus*, config::configuration&);

    ss::gate& gate() { return _gate; }

    ss::future<ss::basic_rwlock<>::holder> read_lock() {
        return _lock.hold_read_lock();
    }

    ss::future<ss::basic_rwlock<>::holder> write_lock() {
        return _lock.hold_write_lock();
    }

    ss::future<checked<model::term_id, errc>> sync();

private:
    ss::future<checked<model::term_id, errc>>
      do_sync(model::timeout_clock::duration);

    ss::future<> apply(model::record_batch) override;

    ss::future<> truncate_log_prefix();
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;
    ss::future<> handle_raft_snapshot() override;

    config::binding<std::chrono::milliseconds> _sync_timeout;
    ss::basic_rwlock<> _lock;

    // Unlike the data partitions tx_registry_stm doesn't rely on the eviction
    // stm and manages log truncations on its own. STM counts the number of
    // applied commands and when it (`_processed`) surpasses
    // `_log_capacity` tx_registry_stm trucates the prefix.
    int64_t _processed{0};
    config::binding<int16_t> _log_capacity;
    model::offset _next_snapshot{-1};

    bool _is_truncating{false};
};

} // namespace cluster
