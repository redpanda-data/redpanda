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

#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct stm_snapshot_header {
    int8_t version{0};
    int32_t snapshot_size{0};

    static constexpr const size_t ondisk_size = sizeof(version)
                                                + sizeof(snapshot_size);
};

struct stm_snapshot {
    stm_snapshot_header header;
    model::offset offset;
    iobuf data;
};

/**
 * persisted_stm is a base class for building ingestion time (*) state
 * machines. Ingestion time means a state machine doesn't need to
 * replicate a command before start processing which allows to:
 *
 *   - reject commands without wasting resources on replication
 *   - be sure that the state can't be changed between a command
 *     received and its replication finished so we it's possible
 *     to ack commands after they're replicated but before they're
 *     applied
 *
 * How does it work?
 *
 * When persisted_stm becomes a leader it syncs its state with the tip
 * of the log (by writing a checkout batch checkpoint_batch_type and
 * reading/executing the commands up to its offset) and caches the
 * current raft's term. Then it uses the cached term for conditional
 * replication so in a case when its state becomes stale (a new leader
 * with higher term has been chosen) the conditional replication fails
 * preventing an operation on the stale state.
 *
 * To speed up the catch up process persisted_stm snapshots the state
 * and uses it as a base for replaying the commands.
 */

class persisted_stm
  : public raft::state_machine
  , public storage::snapshotable_stm {
public:
    static constexpr const int8_t snapshot_version = 0;
    explicit persisted_stm(ss::sstring, ss::logger&, raft::consensus*);

    ss::future<> make_snapshot() final;
    ss::future<> ensure_snapshot_exists(model::offset) final;

    /*
     * Usually start() acts as a barrier and we don't call any methods on the
     * object before start returns control flow.
     *
     * With snapshot-enabled stm we have the following workflow around
     * partition.h/.cc:
     *
     * 1. create consensus
     * 2. create partition
     * 3. pass consensus to partition
     * 4. inside partition:
     *    - create stm and pass consensus to constructor
     *    - pass stm to consensus via stm_manager
     *    - start consensus
     *    - start stm
     *
     * We can't start stm before starting consensus but once consensus has
     * started it may get a chance to invoke make_snapshot or
     * ensure_snapshot_exists on stm before it's started.
     *
     * `wait_for_snapshot_hydrated` inside those methods protects from this
     * scenario.
     */
    ss::future<> start() override;

    ss::future<bool>
    wait_no_throw(model::offset offset, model::timeout_clock::duration);

protected:
    virtual ss::future<> load_snapshot(stm_snapshot_header, iobuf&&) = 0;
    virtual ss::future<stm_snapshot> take_snapshot() = 0;
    ss::future<> hydrate_snapshot(storage::snapshot_reader&);
    ss::future<> wait_for_snapshot_hydrated();
    ss::future<> persist_snapshot(stm_snapshot&&);
    ss::future<> do_make_snapshot();

    ss::future<bool> sync(model::timeout_clock::duration);

    mutex _op_lock;
    std::vector<ss::lw_shared_ptr<expiring_promise<bool>>> _sync_waiters;
    ss::shared_promise<> _resolved_when_snapshot_hydrated;
    model::offset _last_snapshot_offset;
    bool _is_catching_up{false};
    model::term_id _insync_term;
    model::offset _insync_offset;
    raft::consensus* _c;
    storage::snapshot_manager _snapshot_mgr;
    ss::logger& _log;
    model::violation_recovery_policy _snapshot_recovery_policy;
};

} // namespace cluster
