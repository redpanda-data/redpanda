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

#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/state_machine_base.h"
#include "storage/snapshot.h"
#include "storage/types.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/sharded.hh>

namespace raft {

inline constexpr const int8_t stm_snapshot_version_v0 = 0;
inline constexpr const int8_t stm_snapshot_version = 1;

struct stm_snapshot_header {
    int8_t version{0};
    int32_t snapshot_size{0};
    model::offset offset;
};

struct stm_snapshot {
    stm_snapshot_header header;
    iobuf data;

    static stm_snapshot
    create(int8_t version, model::offset offset, iobuf data) {
        stm_snapshot_header header;
        header.version = version;
        header.snapshot_size = data.size_bytes();
        header.offset = offset;

        stm_snapshot snapshot;
        snapshot.header = header;
        snapshot.data = std::move(data);

        return snapshot;
    }
};

// stm_snapshots powered by a seperate file on disk.
//
// This is the default backend for stm_snapshots and works well when
// there are very few partitions (ie for internal topics).
class file_backed_stm_snapshot {
public:
    file_backed_stm_snapshot(
      ss::sstring snapshot_name, prefix_logger& log, raft::consensus* c);
    ss::future<> perform_initial_cleanup();
    ss::future<std::optional<stm_snapshot>> load_snapshot();
    ss::future<> persist_local_snapshot(stm_snapshot&&);
    const ss::sstring& name();
    ss::sstring store_path() const;
    ss::future<> remove_persistent_state();
    size_t get_snapshot_size() const;

    static ss::future<>
    persist_local_snapshot(storage::simple_snapshot_manager&, stm_snapshot&&);

private:
    model::ntp _ntp;
    prefix_logger& _log;
    storage::simple_snapshot_manager _snapshot_mgr;
    size_t _snapshot_size{0};
};

// stm_snapshots powered by the kvstore.
//
// This backend is recommended when it's possible there will be many partitions
// for a given stm, as the alternative (files) does not scale well with many
// partitions.
class kvstore_backed_stm_snapshot {
public:
    kvstore_backed_stm_snapshot(
      ss::sstring snapshot_name,
      prefix_logger& log,
      raft::consensus* c,
      storage::kvstore& kvstore);

    /// For testing
    kvstore_backed_stm_snapshot(
      ss::sstring snapshot_name,
      prefix_logger& log,
      model::ntp ntp,
      storage::kvstore& kvstore);

    ss::future<> perform_initial_cleanup();
    ss::future<std::optional<stm_snapshot>> load_snapshot();
    ss::future<> persist_local_snapshot(stm_snapshot&&);
    const ss::sstring& name();
    ss::sstring store_path() const;
    ss::future<> remove_persistent_state();
    size_t get_snapshot_size() const;
    /// Returns a string used as a key for a snapshot with given name and ntp
    static ss::sstring
    snapshot_key(const ss::sstring& snapshot_name, const model::ntp& ntp);

private:
    bytes snapshot_key() const;

    model::ntp _ntp;
    ss::sstring _name;
    ss::sstring _snapshot_key;
    prefix_logger& _log;
    storage::kvstore& _kvstore;
};

template<typename T>
concept supported_stm_snapshot = requires(T s, stm_snapshot&& snapshot) {
    { s.perform_initial_cleanup() } -> std::same_as<ss::future<>>;
    {
        s.load_snapshot()
    } -> std::same_as<ss::future<std::optional<stm_snapshot>>>;
    {
        s.persist_local_snapshot(std::move(snapshot))
    } -> std::same_as<ss::future<>>;
    { s.remove_persistent_state() } -> std::same_as<ss::future<>>;
    { s.get_snapshot_size() } -> std::convertible_to<size_t>;
    { s.name() } -> std::convertible_to<const ss::sstring&>;
    { s.store_path() } -> std::convertible_to<ss::sstring>;
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
 * When persisted_stm becomes a leader it replicates the configuration batch and
 * uses its offset `last_term_start_offset` as a limit, up to which we read
 * and execute the commands. It caches the current raft term, and uses that for
 * conditional replication: In a case when its state becomes stale (a new leader
 * with higher term has been chosen) the conditional replication fails
 * preventing an operation on the stale state.
 *
 * To speed up the catch up process persisted_stm snapshots the state
 * and uses it as a base for replaying the commands.
 */

template<typename BaseT, supported_stm_snapshot T = file_backed_stm_snapshot>
class persisted_stm_base
  : public BaseT
  , public storage::snapshotable_stm {
public:
    template<typename... Args>
    explicit persisted_stm_base(
      ss::sstring, ss::logger&, raft::consensus*, Args&&...);

    /**
     * Takes and persists a local persisted_stm snapshot. This snapshot isn't
     * replicated and it does not prefix truncate the log
     */
    ss::future<> write_local_snapshot();
    /**
     * Takes and persists local persisted_stm snapshot in background fiber
     */
    void write_local_snapshot_in_background() final;
    /**
     * Called by storage::stm_manager to make sure that stm snapshot exists
     * before evicting log.
     */
    ss::future<> ensure_local_snapshot_exists(model::offset) override;
    virtual uint64_t get_local_snapshot_size() const;
    virtual ss::future<> remove_persistent_state();

    const ss::sstring& name() override { return _snapshot_backend.name(); }

    model::offset last_applied() const final {
        return raft::state_machine_base::last_applied_offset();
    }

    size_t get_local_state_size() const final {
        return get_local_snapshot_size();
    }

    ss::future<> remove_local_state() final {
        return remove_persistent_state();
    }

    ss::future<bool> wait_no_throw(
      model::offset offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>
      = std::nullopt) noexcept;

    model::offset max_collectible_offset() override;
    ss::future<fragmented_vector<model::tx_range>>
      aborted_tx_ranges(model::offset, model::offset) override;

    ss::future<> apply(
      const model::record_batch&,
      const ssx::semaphore_units& apply_units) final;

protected:
    ss::future<> start() override;

    ss::future<> stop() override;

    virtual ss::future<> do_apply(const model::record_batch& b) = 0;

    /**
     * Called when local snapshot is applied to the state machine
     */
    virtual ss::future<> apply_local_snapshot(stm_snapshot_header, iobuf&&) = 0;

    /**
     * Called when a local snapshot is taken. Apply fiber is stalled until
     * apply_units are alive for a consistent snapshot of the state machine.
     */
    virtual ss::future<stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) = 0;

    /*
     * `sync` checks that current node is a leader and if `sync` wasn't
     * called within its term it waits until the state machine is caught
     * up with all the events written by the previous leaders
     */
    ss::future<bool> sync(model::timeout_clock::duration);

    bool _is_catching_up{false};
    model::term_id _insync_term;
    raft::consensus* _raft;
    prefix_logger _log;
    ss::gate _gate;

private:
    ss::future<> wait_offset_committed(
      model::timeout_clock::duration, model::offset, model::term_id);
    ss::future<bool>
      do_sync(model::timeout_clock::duration, model::offset, model::term_id);
    ss::future<std::optional<stm_snapshot>> load_local_snapshot();
    ss::future<> wait_for_snapshot_hydrated();

    ss::future<> do_write_local_snapshot();
    mutex _op_lock{"persisted_stm::op_lock"};
    std::vector<ss::lw_shared_ptr<expiring_promise<bool>>> _sync_waiters;
    ss::condition_variable _on_snapshot_hydrated;
    bool _snapshot_hydrated{false};
    T _snapshot_backend;
    model::offset _last_snapshot_offset;
};

template<supported_stm_snapshot T = file_backed_stm_snapshot>
using persisted_stm = persisted_stm_base<state_machine_base, T>;
/**
 * Helper to copy persisted_stm kvstore snapshot from the source kvstore to
 * target shard
 */
ss::future<> do_copy_persistent_stm_state(
  ss::sstring snapshot_name,
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api);
/**
 * Helper to remove the persisted stm kvstore state
 */
ss::future<> do_remove_persistent_stm_state(
  ss::sstring snapshot_name, model::ntp ntp, storage::kvstore& kvs);

} // namespace raft
