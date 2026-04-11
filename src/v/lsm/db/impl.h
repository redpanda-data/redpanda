// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/gc_actor.h"
#include "lsm/db/memtable.h"
#include "lsm/db/snapshot.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"
#include "lsm/stats.h"
#include "ssx/condition_variable.h"
#include "ssx/mutex.h"
#include "ssx/time.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm::db {

// The implementation of the database.
class impl {
    // The impl constructor is private.
    struct ctor {};

public:
    explicit impl(ctor, io::persistence, ss::lw_shared_ptr<internal::options>);
    impl(impl&&) = delete;
    impl(const impl&) = delete;
    impl& operator=(impl&&) = delete;
    impl& operator=(const impl&) = delete;
    ~impl() = default;

    // the maximum sequence number that has been applied to in memory state or
    // durable storage.
    std::optional<internal::sequence_number> max_applied_seqno() const;

    // The maximum sequence number that has been persisted to durable storage.
    std::optional<internal::sequence_number> max_persisted_seqno() const;

    // Open the database
    static ss::future<std::unique_ptr<impl>>
      open(ss::lw_shared_ptr<internal::options>, io::persistence);

    // Apply a batch of writes to the database atomically.
    ss::future<> apply(ss::lw_shared_ptr<memtable>);

    // Get a key from the database
    ss::future<lookup_result> get(internal::key_view);

    // Additional options to apply when creating an iterator.
    struct iterator_options {
        // The memtable to apply ontop of the changes in the database.
        ss::optimized_optional<ss::lw_shared_ptr<memtable>> memtable;
        // The snapshot to open the iterator at. If this is used
        // data after will not be seen.
        ss::optimized_optional<snapshot*> snapshot = nullptr;
    };

    // Create an interator over the database.
    //
    // If a non-null memtable is passed in, then a frozen state of the memtable
    // is applied ontop of the existing database.
    ss::future<std::unique_ptr<internal::iterator>>
      create_iterator(iterator_options);

    // Create a snapshot of the database contents. If the database is empty,
    // then this will return `nullptr` to signify the snapshot is to just return
    // nothing.
    ss::optimized_optional<std::unique_ptr<snapshot>> create_snapshot();

    // Flush any pending state in memtables to disk.
    ss::future<> flush(ssx::instant deadline);

    // Flush with no deadline.
    ss::future<> flush();

    // Reload the manifest from disk, adding a new version to the version set.
    // Only valid if in read-only mode.
    ss::future<bool> refresh();

    // Close the database, no more operations should happen to the database at
    // this point.
    //
    // This *must* be called before destroying the database.
    ss::future<> close();

    lsm::data_stats get_data_stats() const;

private:
    friend class snapshot;
    // Create an iterator over the database. Note that this iterator
    // results in ALL entries from the database, a deduplicating iterator
    // needs to be added on top to give a traditional iterator view.
    //
    // If a non-null memtable is passed in, then a frozen state of the memtable
    // is applied ontop of the existing database.
    ss::future<std::unique_ptr<internal::iterator>> create_internal_iterator();

    ss::future<> recover();

    ss::future<> make_room_for_write();

    void maybe_schedule_compaction();

    ss::future<> do_flush();
    ss::future<> do_compaction(std::unique_ptr<compaction>);
    ss::future<> apply_edits(ss::lw_shared_ptr<version_edit>);

    io::persistence _persistence;
    ss::lw_shared_ptr<internal::options> _opts;
    // The active in-memory memtable.
    ss::lw_shared_ptr<memtable> _mem;
    // The memtable being compacted
    ss::optimized_optional<ss::lw_shared_ptr<memtable>> _imm;
    std::unique_ptr<table_cache> _table_cache;
    std::unique_ptr<version_set> _versions;
    ssx::condition_variable _background_work_finished_signal;
    ss::abort_source _as;
    snapshot_list _snapshots;
    gc_actor _gc_actor;
    ssx::mutex _manifest_write_mu;
    bool _is_flushing = false;
    ss::gate _gate;
};

} // namespace lsm::db
