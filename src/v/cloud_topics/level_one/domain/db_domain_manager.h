/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/rwlock.hh>

namespace cloud_topics::l1 {
class io;
class state_reader;

// Database-backed implementation of domain_manager.
// Expected to be running on the leader replicas of the partition that backs
// the STM.
class db_domain_manager final : public domain_manager {
public:
    explicit db_domain_manager(
      model::term_id expected_term,
      ss::shared_ptr<stm> stm,
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket);

    void start() override;
    ss::future<> stop_and_wait() override;

    ss::future<rpc::add_objects_reply>
      add_objects(rpc::add_objects_request) override;

    ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request) override;

    ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request) override;

    ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request) override;

    ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(
        rpc::get_first_offset_for_bytes_request) override;

    ss::future<rpc::get_offsets_reply>
      get_offsets(rpc::get_offsets_request) override;

    ss::future<rpc::get_compaction_info_reply>
      get_compaction_info(rpc::get_compaction_info_request) override;

    ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request) override;

    ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request) override;

    ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request) override;

    ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request) override;

    ss::future<rpc::get_compaction_infos_reply>
      get_compaction_infos(rpc::get_compaction_infos_request) override;

    ss::future<rpc::get_extent_metadata_reply>
      get_extent_metadata(rpc::get_extent_metadata_request) override;

private:
    // Initializes the underlying database for the current term, potentially
    // reopening it if needed (e.g. the underlying Raft term has changed since
    // the last open).
    //
    // Once called, callers should expect that db_ is at least set, though it
    // is possible that it will still need to be reopened (e.g. because of a
    // domain restore).
    ss::future<std::expected<void, rpc::errc>> maybe_open_db();

    // Should be called and held when resetting the database instance to ensure
    // there is no on-going access to the database.
    ss::future<std::expected<ss::rwlock::holder, rpc::errc>>
    exclusive_db_lock();

    std::optional<ss::gate::holder> maybe_gate();

    struct gate_read_lock {
        ss::gate::holder gate;
        ss::rwlock::holder db_lock;
    };
    // Holds the gate, opens the database, and takes the db lock in shared
    // mode, preventing other fibers from reopening the database while the
    // resulting lock is alive (e.g. during async work on the database).
    //
    // Should be called before reading from the database.
    ss::future<std::expected<gate_read_lock, rpc::errc>> gate_and_open_reads();

    struct gate_writer_locks {
        gate_read_lock gate_read_lock;
        // TODO: this may need to be more fine grained (e.g. row locks).
        ssx::checkpoint_mutex_units writer_lock;
    };
    // Similar to gate_and_open_reads(), but also acquires the writer lock,
    // ensuring serialized updates to the database.
    //
    // Should be called before checking invariants and writing to the database.
    ss::future<std::expected<gate_writer_locks, rpc::errc>>
    gate_and_open_writes();

    // Writes the given rows to the underlying database. If there is an issue
    // writing that implies that the underlying database state may not be safe
    // to continue writing to (e.g. because replication timed out and we can't
    // guarantee that it won't eventually succeed), steps down as leader to
    // prevent further updates from succeeding.
    ss::future<std::expected<void, rpc::errc>>
    write_rows(const gate_writer_locks&, chunked_vector<write_batch_row>);

    ss::future<rpc::get_compaction_info_reply> do_get_compaction_info(
      const gate_read_lock&, state_reader&, rpc::get_compaction_info_request);

    ss::gate gate_;
    ss::abort_source as_;
    model::term_id expected_term_;
    std::filesystem::path staging_dir_;
    cloud_io::remote* remote_;
    cloud_storage_clients::bucket_name bucket_;

    ss::shared_ptr<stm> stm_;

    // Hold in write mode when changing the db instance.
    // Hold in read mode for other access to the db that doesn't reopen the db.
    ss::rwlock db_instance_lock_;

    // Lock taken to serialize updates to the database, to ensure invariants
    // are checked and writes are applied atomically with respect to one
    // another. The db_instance_lock_ should be taken before taking this lock.
    //
    // TODO: make this more fine-grained, e.g. by doing per-partition locking;
    // note though that finer-grained locking will need to consider concurrent
    // updates to the same object entry from multiple partitions, so maybe
    // there'd need to be some form of object locking as well.
    ssx::checkpoint_mutex writer_lock_{"l1/domain/writer"};

    // Database backed by cloud IO and a replicated STM.
    // Operations will only succeed with this db when the underlying Raft
    // partition is leader of the expected term.
    std::unique_ptr<replicated_database> db_;
};

} // namespace cloud_topics::l1
