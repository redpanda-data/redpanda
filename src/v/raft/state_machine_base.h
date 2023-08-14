/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "outcome.h"
#include "raft/fwd.h"
#include "raft/offset_monitor.h"
#include "raft/types.h"
#include "seastarx.h"

namespace raft {

class consensus;
/**
 * State machine interface. The class provides an interface that must be
 * implemented to build state machine that can be registered in
 * state_machine_manager.
 */
class state_machine_base {
public:
    state_machine_base() = default;
    state_machine_base(state_machine_base&&) = default;
    state_machine_base(const state_machine_base&) = delete;
    state_machine_base& operator=(state_machine_base&&) = default;
    state_machine_base& operator=(const state_machine_base&) = delete;
    virtual ~state_machine_base() = default;

    // wait until at least offset is applied to state machine
    ss::future<> wait(
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    /**
     * This function accepts a batch reference, an implementer may copy a batch
     * with `model::record_batch::copy()` method, the interface design is a
     * consequence of having single apply fiber for all state machines built on
     * top of the same partition. Apply is assumed to be atomic i.e. either
     * whole record is applied or not. When exception is thrown from apply
     * method it will be retried with the same record batch.
     */
    virtual ss::future<> apply(const model::record_batch&) = 0;
    /**
     * This function will be called every time a snapshot is applied in apply
     * fiber. Snapshot will contain only a data specific for this state machine
     * returned in previous `state_machine_base::take_snapshot()` calls
     */
    virtual ss::future<> apply_raft_snapshot(const iobuf&) = 0;

    /**
     * Returns a unique identifier of this state machine. Each stm built on top
     * of the same Raft group must have different id.
     * Id is going to be used when logging and to mark parts of the snapshots.
     */
    virtual std::string_view get_name() const = 0;

    /**
     * Returns a snapshot of an STM state with requested last included offset
     */
    virtual ss::future<iobuf>
    take_snapshot(model::offset last_included_offset) = 0;

    /**
     * Last successfully applied offset
     */
    model::offset last_applied_offset() const {
        return model::prev_offset(_next);
    }

protected:
    /**
     *  Lifecycle is managed by state_machine_manager
     */
    virtual ss::future<> start() = 0;
    /**
     * Stops the state machine (managed by `state_machine_manager`) when
     * overriding remember to call the `state_machine_base::stop()` first.
     */
    virtual ss::future<> stop();

    model::offset next() const { return _next; }
    void set_next(model::offset offset);

    friend class batch_applicator;
    friend class state_machine_manager;

private:
    offset_monitor _waiters;
    model::offset _next{0};
};

} // namespace raft
