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

#include "config/property.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/fwd.h"
#include "raft/logger.h"
#include "raft/state_machine_base.h"
#include "serde/envelope.h"
#include "storage/snapshot.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <concepts>
#include <utility>
#include <vector>

namespace raft {

template<typename T>
concept ManagableStateMachine = std::derived_from<T, state_machine_base>;
template<typename Func>
concept StateMachineIterateFunc = requires(
  Func f, const ss::sstring& name, const state_machine_base& stm) {
    { f(name, stm) } -> std::convertible_to<void>;
};
/**
 * State machine manager is an entry point for registering state machines
 * built on top of replicated log. State machine managers uses a single
 * fiber to read and apply record batches to all managed state machines.
 *
 * When a machine throws an exception or timeouts when applying batches to
 * its state subsequent applies are executed in the separate apply fiber
 * specific for that STM.
 *
 * State machine manager also takes care of the snapshot consistency. It
 * wraps state machine snapshots in its own snapshot format which is a map
 * where each individual STM snapshot is stored separately. When snapshot is
 * taken the manager guarantees that all the STMs have the same offset
 * applied so that the snapshot is consistent across the state machines.
 *
 */
class state_machine_manager final {
public:
    // wait until at least offset is applied to all the state machines
    ss::future<> wait(
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);
    /**
     * In classic Raft protocol a snapshot is always taken from the current STM
     * state i.e last snapshot index is derived from last_applied_offset. In
     * Redpanda we use different approach. Data eviction policy forces us to
     * allow state machines to take snapshot at arbitrary offsets.
     */
    ss::future<iobuf> take_snapshot(model::offset);

    ss::future<> start();
    ss::future<> stop();

    model::offset last_applied() const { return model::prev_offset(_next); }

    template<StateMachineIterateFunc Func>
    void for_each_stm(Func&& func) const {
        for (const auto& [name, entry] : _machines) {
            func(name, *entry->stm);
        }
    }

private:
    using stm_ptr = ss::shared_ptr<state_machine_base>;

    state_machine_manager(
      consensus* raft,
      std::vector<stm_ptr> stms_to_manage,
      ss::scheduling_group apply_sg);

    friend class batch_applicator;
    friend class state_machine_manager_builder;
    static constexpr const char* default_ctx = "default";
    static constexpr const char* background_ctx = "background";

    struct state_machine_entry {
        explicit state_machine_entry(ss::shared_ptr<state_machine_base> stm)
          : stm(std::move(stm)) {}
        state_machine_entry(state_machine_entry&&) noexcept = default;
        state_machine_entry(const state_machine_entry&) noexcept = delete;
        state_machine_entry& operator=(state_machine_entry&&) noexcept = delete;
        state_machine_entry& operator=(const state_machine_entry&) noexcept
          = delete;
        ~state_machine_entry() = default;

        ss::shared_ptr<state_machine_base> stm;
        mutex background_apply_mutex;
    };
    using entry_ptr = ss::lw_shared_ptr<state_machine_entry>;
    using state_machines_t = absl::flat_hash_map<ss::sstring, entry_ptr>;

    void maybe_start_background_apply(const entry_ptr&);
    ss::future<> background_apply_fiber(entry_ptr);

    ss::future<> apply_raft_snapshot();
    ss::future<> do_apply_raft_snapshot(
      raft::snapshot_metadata metadata, storage::snapshot_reader& reader);
    ss::future<> apply();

    ss::future<std::vector<ssx::semaphore_units>>
    acquire_background_apply_mutexes();
    /**
     * Simple data structure allowing manager to store independent snapshot
     * for each of the STMs
     */
    struct managed_snapshot
      : serde::checksum_envelope<
          managed_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        absl::flat_hash_map<ss::sstring, iobuf> snapshot_map;

        friend bool operator==(const managed_snapshot&, const managed_snapshot&)
          = default;

        auto serde_fields() { return std::tie(snapshot_map); }
    };

    consensus* _raft;
    ctx_log _log;
    mutex _apply_mutex;
    state_machines_t _machines;
    model::offset _next{0};
    ss::gate _gate;
    ss::abort_source _as;
    ss::scheduling_group _apply_sg;
};

/**
 * Helper class to create state machine manager and manage its lifecycle
 */
class state_machine_manager_builder {
public:
    template<ManagableStateMachine T, typename... Args>
    ss::shared_ptr<T> create_stm(Args&&... args) {
        auto machine = ss::make_shared<T>(std::forward<Args>(args)...);
        _stms.push_back(machine);

        return machine;
    }

    void with_scheduing_group(ss::scheduling_group sg) { _sg = sg; }

    state_machine_manager build(raft::consensus* raft) && {
        return {raft, std::move(_stms), _sg};
    }

private:
    std::vector<state_machine_manager::stm_ptr> _stms;
    ss::scheduling_group _sg = ss::default_scheduling_group();
};

} // namespace raft
