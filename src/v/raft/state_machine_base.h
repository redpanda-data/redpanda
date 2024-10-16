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
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/fwd.h"
#include "raft/offset_monitor.h"
#include "utils/mutex.h"

namespace raft {
using snapshot_at_offset_supported
  = ss::bool_class<struct snapshot_at_offset_supported_tag>;
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
     * Applies the batch and updates the next offset to be applied.
     *
     * This function accepts a batch reference, an implementer may copy a batch
     * with `model::record_batch::copy()` method, the interface design is a
     * consequence of having single apply fiber for all state machines built on
     * top of the same partition. Apply is assumed to be atomic i.e. either
     * whole record is applied or not. When exception is thrown from apply
     * method it will be retried with the same record batch.
     */
    ss::future<> apply(const model::record_batch&);

    /**
     * This function will be called every time a snapshot is applied in apply
     * fiber. Snapshot will contain only a data specific for this state machine
     * returned in previous `state_machine_base::take_snapshot()` calls
     */
    virtual ss::future<> apply_raft_snapshot(const iobuf&) = 0;

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

    /**
     * Some of the state machines may persist additional local state, to account
     * for it we provide a method that should return size of local state in
     * bytes.
     */
    virtual size_t get_local_state_size() const = 0;
    /**
     * Some of the state machines may persist additional local state. This
     * method is used by stm manager to clean the persistent state of the stm
     */
    virtual ss::future<> remove_local_state() = 0;

    /**
     * Returns true if a state machine supports taking a snapshot at any offset.
     * It the state machine allows taking snapshot at any applied offset the
     * partition will support fast reconfigurations.
     */
    virtual snapshot_at_offset_supported supports_snapshot_at_offset() const {
        return snapshot_at_offset_supported::yes;
    }

protected:
    /**
     * Must always be called under apply mutex scope and apply_units argument
     * is in place to enforce that. It is const qualified to ensure the apply
     * impelementors do not control their lifetime.
     */
    virtual ss::future<>
    apply(const model::record_batch&, const ssx::semaphore_units& apply_units)
      = 0;

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

    mutex _apply_lock{"state_machine_base::apply_lock"};

    friend class batch_applicator;
    friend class state_machine_manager;

private:
    offset_monitor _waiters;
    model::offset _next{0};
};

/**
 * This flavor of state machine base allows implementer to opt out from taking
 * snapshot at arbitrary offset. This way a partition that the STM is based on
 * will not support the fast partition movement mechanism. When implementing
 * this state machine make sure to provide an external mechanism that will drive
 * the snapshot creation.
 */
class no_at_offset_snapshot_stm_base : public state_machine_base {
    /**
     * Method that will be called whenever a raft snapshot is required
     */
    virtual ss::future<iobuf> take_snapshot() = 0;

    snapshot_at_offset_supported supports_snapshot_at_offset() const final {
        return snapshot_at_offset_supported::no;
    }

    ss::future<iobuf> take_snapshot(model::offset offset) final {
        if (offset != last_applied_offset()) {
            throw std::logic_error(fmt::format(
              "State machine that do not support taking snapshot at arbitrary "
              "offset can to take snapshot at requested offset: {}, current "
              "last applied offset: {}",
              offset,
              last_applied_offset()));
        }
        return take_snapshot();
    }
};

} // namespace raft
