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

#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/state_machine.h"
#include "raft/types.h"
#include "storage/snapshot.h"
#include "storage/types.h"
#include "utils/expiring_promise.h"
#include "utils/fragmented_vector.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

static constexpr const int8_t stm_snapshot_version_v0 = 0;
static constexpr const int8_t stm_snapshot_version = 1;

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

class file_backed_stm_snapshot {
public:
    file_backed_stm_snapshot(
      ss::sstring snapshot_name, prefix_logger& log, raft::consensus* c);
    ss::future<std::optional<stm_snapshot>> load_snapshot();
    ss::future<> persist_snapshot(stm_snapshot&&);
    const ss::sstring& name();
    ss::sstring store_path() const;
    ss::future<> remove_persistent_state();
    size_t get_snapshot_size() const;

    static ss::future<>
    persist_snapshot(storage::simple_snapshot_manager&, stm_snapshot&&);

private:
    model::ntp _ntp;
    prefix_logger& _log;
    storage::simple_snapshot_manager _snapshot_mgr;
    size_t _snapshot_size{0};
};

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

    ss::future<std::optional<stm_snapshot>> load_snapshot();
    ss::future<> persist_snapshot(stm_snapshot&&);
    const ss::sstring& name();
    ss::sstring store_path() const;
    ss::future<> remove_persistent_state();
    size_t get_snapshot_size() const;

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
    {
        s.load_snapshot()
    } -> std::same_as<ss::future<std::optional<stm_snapshot>>>;
    { s.persist_snapshot(std::move(snapshot)) } -> std::same_as<ss::future<>>;
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

template<supported_stm_snapshot T = file_backed_stm_snapshot>
class persisted_stm
  : public raft::state_machine
  , public storage::snapshotable_stm {
public:
    template<typename... Args>
    explicit persisted_stm(
      ss::sstring, ss::logger&, raft::consensus*, Args&&...);

    void make_snapshot_in_background() final;
    ss::future<> ensure_snapshot_exists(model::offset) final;
    model::offset max_collectible_offset() override;
    ss::future<fragmented_vector<model::tx_range>>
      aborted_tx_ranges(model::offset, model::offset) override;

    virtual ss::future<> remove_persistent_state();
    const ss::sstring& name() override { return _snapshot_backend.name(); }

    ss::future<> make_snapshot();
    virtual uint64_t get_snapshot_size() const;
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

    ss::future<bool> wait_no_throw(
      model::offset offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

private:
    ss::future<> wait_offset_committed(
      model::timeout_clock::duration, model::offset, model::term_id);
    ss::future<bool>
      do_sync(model::timeout_clock::duration, model::offset, model::term_id);

protected:
    virtual ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) = 0;
    virtual ss::future<stm_snapshot> take_snapshot() = 0;
    ss::future<std::optional<stm_snapshot>> load_snapshot();
    ss::future<> wait_for_snapshot_hydrated();
    ss::future<> persist_snapshot(stm_snapshot&&);
    static ss::future<>
    persist_snapshot(storage::simple_snapshot_manager&, stm_snapshot&&);
    ss::future<> do_make_snapshot();

    /*
     * `sync` checks that current node is a leader and if `sync` wasn't
     * called within its term it waits until the state machine is caught
     * up with all the events written by the previous leaders
     */
    ss::future<bool> sync(model::timeout_clock::duration);

    mutex _op_lock;
    std::vector<ss::lw_shared_ptr<expiring_promise<bool>>> _sync_waiters;
    ss::shared_promise<> _resolved_when_snapshot_hydrated;
    model::offset _last_snapshot_offset;
    bool _is_catching_up{false};
    model::term_id _insync_term;
    model::offset _insync_offset;
    raft::consensus* _c;
    prefix_logger _log;
    T _snapshot_backend;
};

} // namespace cluster
