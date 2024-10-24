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

#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/logger.h"
#include "raft/state_machine_base.h"
#include "raft/types.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "storage/snapshot.h"
#include "utils/absl_sstring_hash.h"
#include "utils/mutex.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <concepts>
#include <string_view>
#include <utility>
#include <vector>

namespace raft {

template<typename T>
concept ManagableStateMachine = requires(T stm) {
    std::derived_from<T, state_machine_base>;
    { T::name } -> std::convertible_to<std::string_view>;
};
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
    /**
     * A result returned after taking a snapshot it contains a serde serialized
     * snapshot data and last offset included into the snapshot.
     */
    struct snapshot_result {
        iobuf data;
        model::offset last_included_offset;
    };

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
     *
     * IMPORTANT: This API is only supported if all state machines support
     * taking snapshots at arbitrary offset.
     */
    ss::future<snapshot_result> take_snapshot(model::offset);

    /**
     * If any of the state machines in the manager doesn't support fast
     * reconfigurations this is the only API that the user is allowed to call,
     * the take snapshot with offset other than _last_applied_offset will fail.
     */
    ss::future<snapshot_result> take_snapshot();

    ss::future<> start();
    ss::future<> stop();

    model::offset last_applied() const { return model::prev_offset(_next); }

    snapshot_at_offset_supported supports_snapshot_at_offset() const {
        return _supports_snapshot_at_offset;
    }

    /**
     * Returns a pointer to specific type of state machine.
     *
     * This API provides basic runtime validation verifying if requested STM
     * type matches the name passed. It returns a nullptr if state machine with
     * requested name is not registered in manager.
     */
    template<ManagableStateMachine T>
    ss::shared_ptr<T> get() {
        auto it = _machines.find(T::name);
        if (it == _machines.end()) {
            return nullptr;
        }
        auto ptr = ss::dynamic_pointer_cast<T>(it->second->stm);
        vassert(
          ptr != nullptr, "Incorrect STM type requested for STM {}", T::name);
        return ptr;
    }

    template<StateMachineIterateFunc Func>
    void for_each_stm(Func&& func) const {
        for (const auto& [name, entry] : _machines) {
            func(name, *entry->stm);
        }
    }

    ss::future<> remove_local_state();

private:
    using stm_ptr = ss::shared_ptr<state_machine_base>;
    struct named_stm {
        named_stm(ss::sstring, stm_ptr);
        ss::sstring name;
        stm_ptr stm;
    };

    state_machine_manager(
      consensus* raft,
      std::vector<named_stm> stms_to_manage,
      ss::scheduling_group apply_sg);

    friend class batch_applicator;
    friend class state_machine_manager_builder;
    static constexpr const char* default_ctx = "default";
    static constexpr const char* background_ctx = "background";

    struct state_machine_entry {
        explicit state_machine_entry(
          ss::sstring name, ss::shared_ptr<state_machine_base> stm)
          : name(std::move(name))
          , stm(std::move(stm)) {}
        state_machine_entry(state_machine_entry&&) noexcept = default;
        state_machine_entry(const state_machine_entry&) noexcept = delete;
        state_machine_entry& operator=(state_machine_entry&&) noexcept = delete;
        state_machine_entry& operator=(const state_machine_entry&) noexcept
          = delete;
        ~state_machine_entry() = default;

        ss::sstring name;
        ss::shared_ptr<state_machine_base> stm;
        mutex background_apply_mutex{
          "state_machine_manager::background_apply_mutex"};
    };
    using entry_ptr = ss::lw_shared_ptr<state_machine_entry>;
    using state_machines_t
      = absl::flat_hash_map<ss::sstring, entry_ptr, sstring_hash, sstring_eq>;

    void maybe_start_background_apply(const entry_ptr&);
    ss::future<> background_apply_fiber(entry_ptr, ssx::semaphore_units);

    ss::future<> apply_raft_snapshot();
    ss::future<> do_apply_raft_snapshot(
      raft::snapshot_metadata metadata,
      storage::snapshot_reader& reader,
      std::vector<ssx::semaphore_units> background_apply_units);
    ss::future<> apply();
    ss::future<> try_apply_in_foreground();

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

    ss::future<> apply_snapshot_to_stm(
      ss::lw_shared_ptr<state_machine_entry> stm_entry,
      const managed_snapshot& snapshot,
      model::offset last_included_offset);

    consensus* _raft;
    ctx_log _log;
    mutex _apply_mutex{"stm_manager::apply"};
    state_machines_t _machines;
    model::offset _next{0};
    ss::gate _gate;
    ss::abort_source _as;
    ss::scheduling_group _apply_sg;
    snapshot_at_offset_supported _supports_snapshot_at_offset{true};
};

/**
 * Helper class to create state machine manager and manage its lifecycle
 */
class state_machine_manager_builder {
public:
    template<ManagableStateMachine T, typename... Args>
    ss::shared_ptr<T> create_stm(Args&&... args) {
        auto machine = ss::make_shared<T>(std::forward<Args>(args)...);
        _stms.emplace_back(ss::sstring(T::name), machine);

        return machine;
    }

    void with_scheduing_group(ss::scheduling_group sg) { _sg = sg; }

    state_machine_manager build(raft::consensus* raft) && {
        return {raft, std::move(_stms), _sg};
    }

private:
    std::vector<state_machine_manager::named_stm> _stms;
    ss::scheduling_group _sg = ss::default_scheduling_group();
};

} // namespace raft
