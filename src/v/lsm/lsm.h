// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "absl/time/time.h"
#include "base/seastarx.h"
#include "lsm/core/probe.h"
#include "lsm/io/persistence.h"
#include "lsm/stats.h"
#include "ssx/time.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>

#include <cstdint>
#include <memory>
#include <string>

namespace lsm {

namespace db {
class impl;
class memtable;
class snapshot;
} // namespace db

namespace internal {
class iterator;
} // namespace internal

using sequence_number = named_type<uint64_t, struct seqno_tag>;
class iterator;
class snapshot;

// Options for the database.
struct options {
    // Database epoch are assigned to a file such that on shared storage mediums
    // databases that share persistence locations that write with different
    // epochs will not collide with each other.
    //
    // For example, if replicating a WAL using raft, then writing the WAL to
    // object storage, each raft term could become it's own database_epoch,
    // which would mean that old leaders cannot clobber a new leader's files
    // while shutting down.
    //
    // This value should never decrease.
    uint64_t database_epoch = 0;

    // If the database is opened in readonly mode. Readonly mode causes all
    // write operations to fail. However, read operations can be performed.
    bool readonly = false;

    // The scheduling group for background operations in the LSM tree, such as
    // memtable flushing and compaction. Note that this is unused if the
    // database is in readonly mode. Defaults to the scheduling group that
    // opened the database.
    std::optional<ss::scheduling_group> compaction_scheduling_group;

    // The number of levels in the LSM tree. More levels allow more smaller and
    // faster compactions, but causes more read amplication.
    //
    // There must be at least 2 levels in the database:
    // * one for unsorted data (where memtables get flushed to)
    // * one for sorted data
    //
    // Currently, it is only valid for this value to be increased each time the
    // database is opened.
    constexpr static uint8_t default_num_levels = 7;
    uint8_t num_levels = default_num_levels;

    // At what point do we start throttling writes in terms of the number of L0
    // files
    constexpr static size_t default_level_zero_slowdown_writes_trigger = 8;
    size_t level_zero_slowdown_writes_trigger
      = default_level_zero_slowdown_writes_trigger;

    // At what point do we halt writes in terms of number of L0 files
    constexpr static size_t default_level_zero_stop_writes_trigger = 12;
    size_t level_zero_stop_writes_trigger
      = default_level_zero_stop_writes_trigger;

    // How big to let memtable accumulate in bytes before flushing.
    constexpr static size_t default_write_buffer_size = 16_MiB;
    size_t write_buffer_size = default_write_buffer_size;

    // When do we trigger compaction into L1 in terms of number of L0 files
    constexpr static size_t default_level_one_compaction_trigger = 4;
    size_t level_one_compaction_trigger = default_level_one_compaction_trigger;

    // The approximate max number of SST files that should be opened at one
    // time.
    constexpr static size_t default_max_open_files = 1000;
    size_t max_open_files = default_max_open_files;

    // If non-zero, the number of fibers to use to pre-open all the files in the
    // database, which populates the table cache.
    //
    // This allows ensuring files are all present and readable at database open
    // time.
    uint32_t max_pre_open_fibers = 0;

    // The size of the cache that stores uncompressed blocks.
    constexpr static size_t default_block_cache_size = 10_MiB;
    size_t block_cache_size = default_block_cache_size;

    // The size of a single block within an SST file.
    constexpr static size_t default_sst_block_size = 8_KiB;
    size_t sst_block_size = default_sst_block_size;

    // The frequency at which to generate a new bloom filter.
    //
    // If set to 0 then no bloom filters will be generated.
    //
    // This value may be changed between different opens of the database.
    //
    // REQUIRED: this value must be a power of two
    constexpr static size_t default_sst_filter_period = 2_KiB;
    size_t sst_filter_period = default_sst_filter_period;

    // The supported compression algorithms
    enum class compression_type : uint8_t {
        none = 0,
        zstd = 1,
    };
    // The compression to use for SST blocks.
    //
    // This value may be changed between different opens of the database (old
    // data is left as is).
    //
    // More elements in this vector are ignored, and if there are more levels
    // than elements, the last element is repeated for the rest of the levels.
    //
    // If empty all levels are uncompressed.
    std::vector<compression_type> compression_by_level;

    // The delay at which to wait until actually deleting files.
    //
    // If set to zero, then files are immediately GC'd after new manifest files
    // are written.
    absl::Duration file_deletion_delay;

    // The probe to use for recording statistics.
    //
    // Use this to specify the probe and grab a reference to expose the metrics
    // in our internal prometheus stack for the appropriate subsystem.
    ss::lw_shared_ptr<probe> probe = ss::make_lw_shared<lsm::probe>();
};

class write_batch;

// A LSM tree database. Note that this database does *not* have a WAL.
class database {
public:
    explicit database(std::unique_ptr<db::impl> impl);
    database(const database&) = delete;
    database(database&&) noexcept;
    database& operator=(const database&) = delete;
    database& operator=(database&&) noexcept;
    ~database() noexcept;

    // Open the database.
    static ss::future<database> open(options, io::persistence);

    // Close the database, no more operations should happen to the database at
    // this point, and all iterators should be closed before calling this
    // method.
    //
    // This *must* be called before destroying the database.
    ss::future<> close();

    // The maximum offset that has been persisted to durable storage.
    std::optional<sequence_number> max_persisted_seqno() const;

    // The maximum offset that has been applied to the database (persisted or
    // not).
    std::optional<sequence_number> max_applied_seqno() const;

    // Flush existing buffered data such that that `max_persisted_offset()`
    // becomes >= the current `max_applied_offset()`.
    ss::future<> flush(ssx::instant deadline);

    // Apply a batch of data atomically to the database.
    //
    // Any gets/iteration on the write batch must be finished before calling
    // this method.
    ss::future<> apply(write_batch);

    // Lookup a value in the database
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database.
    //
    // This iterator gives snapshot isolation, so writes applied after
    // this future completes will not be seen by the iterator.
    ss::future<iterator> create_iterator();

    // Create an explicit snapshot of the database.
    //
    // The snapshot must not outlive the database being closed.
    snapshot create_snapshot();

    // Create a write batch that can be applied to the the database.
    //
    // This write batch can only be used with this database. It's lifetime is
    // tied to this database as well, meaning the write batch must not outlive
    // the database being closed.
    write_batch create_write_batch();

    // Reload the manifest from disk, picking up changes made by other writers.
    //
    // Returns true if the manifest was updated, false if no change. Throws if
    // the database is not read-only, or if the downloaded manifest would
    // regress the file ID or sequence number.
    //
    // REQUIRES: Database must be opened in read-only mode.
    ss::future<bool> refresh();

    // Get data statistics for the database including memtable sizes, per-level
    // SST file sizes, and per-file information.
    data_stats get_data_stats() const;

private:
    std::unique_ptr<db::impl> _impl;
};

// An iterator over the contents of the database.
class iterator {
public:
    explicit iterator(std::unique_ptr<internal::iterator> impl);
    iterator(const iterator&) = delete;
    iterator(iterator&&) noexcept;
    iterator& operator=(const iterator&) = delete;
    iterator& operator=(iterator&&) noexcept;
    ~iterator() noexcept;

    // An iterator is either positioned at a key/value pair, or
    // not valid. This method returns true iff the iterator is valid.
    bool valid() const;

    // Position at the first key in the source. The iterator is valid()
    // after this call iff the source is not empty.
    ss::future<> seek_to_first();

    // Position at the last key in the source. The iterator is
    // valid() after this call iff the source is not empty.
    ss::future<> seek_to_last();

    // Position at the first key in the source that is at or past target.
    // The iterator is valid() after this call iff the source contains
    // an entry that comes at or past target.
    ss::future<> seek(std::string_view target);

    // Moves to the next entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: valid()
    ss::future<> next();

    // Moves to the previous entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: valid()
    ss::future<> prev();

    // Return the key for the current entry. The returned value is only valid
    // until the iterator is moved.
    // REQUIRES: valid()
    std::string_view key();

    // Return the sequence number for the current entry.
    // REQUIRES: valid()
    sequence_number seqno();

    // Return the value for the current entry.
    // REQUIRES: valid()
    iobuf value();

private:
    std::unique_ptr<internal::iterator> _impl;
};

// A batch of data that can be applied atomically to the database.
//
// In addition to being a staging ground for write operations,
// the write batch also supports reading the staged data as a view over the
// existing database so that you can Read-Your-Own-Writes before data is
// committed to the LSM tree.
class write_batch {
public:
    write_batch(const write_batch&) = delete;
    write_batch(write_batch&&) noexcept;
    write_batch& operator=(const write_batch&) = delete;
    write_batch& operator=(write_batch&&) noexcept;
    ~write_batch() noexcept;

    // Set the key in the database with the given value for this offset.
    //
    // REQUIRES: offsets must be monotonically increasing as added to the batch.
    void put(std::string_view key, iobuf value, sequence_number seqno);

    // Remove the key in the database at this offset.
    //
    // REQUIRES: offsets must be monotonically increasing as added to the batch.
    void remove(std::string_view key, sequence_number seqno);

    // Lookup a value in the database as if the current write batch was applied.
    //
    // The returned future must finish resolving before the write_batch is
    // applied to the database.
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database as if the current write batch was
    // applied.
    //
    // This iterator gives snapshot isolation, so  writes applied after
    // this future completes will not be seen by the iterator as long as
    // these writes do not have the same offset.
    //
    // The resulting iterator is safe to use concurrently with additional writes
    // being applied to the batch.
    //
    // The returned iterator must be destroyed before the write_batch is applied
    // to the database.
    ss::future<iterator> create_iterator();

private:
    explicit write_batch(db::impl*);

    friend class database;
    ss::lw_shared_ptr<db::memtable> _batch;
    db::impl* _db;
};

// A snapshot of the database.
class snapshot {
public:
    snapshot(const snapshot&) = delete;
    snapshot(snapshot&&) noexcept;
    snapshot& operator=(const snapshot&) = delete;
    snapshot& operator=(snapshot&&) noexcept;
    ~snapshot();
    // Lookup a value in the database snapshot.
    //
    // The returned future must finish resolving before the snapshot is
    // destroyed.
    ss::future<std::optional<iobuf>> get(std::string_view key);

    // Create an iterator over the database snapshot.
    //
    // The snapshot must outlive the returned iterator.
    ss::future<iterator> create_iterator();

private:
    snapshot(std::unique_ptr<db::snapshot> snap, db::impl* db);

    friend class database;
    // nullptr iff the database snapshot is for an empty database.
    std::unique_ptr<db::snapshot> _snap;
    db::impl* _db{};
};

} // namespace lsm
