// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "absl/container/fixed_array.h"
#include "base/format_to.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"

#include <seastar/core/shared_ptr.hh>

#include <cstdint>

namespace lsm::db {

// The default number of seeks before compaction is triggered.
constexpr static int32_t default_allowed_seeks = 1024 * 1024 * 1024;

// All the metadata for a single SST file.
struct file_meta_data {
    // The pointer to a file
    internal::file_handle handle;
    // Size of the file in bytes
    uint64_t file_size = 0;
    internal::key smallest; // smallest key in the table
    internal::key largest;  // largest key in the table
    internal::sequence_number oldest_seqno;
    internal::sequence_number newest_seqno;
    // Allowed seeks before compaction
    int32_t allowed_seeks = default_allowed_seeks;

    bool operator==(const file_meta_data&) const = default;
    fmt::iterator format_to(fmt::iterator it) const;
};

// This allocator can create new file ID for files being created in the LSM
// tree.
//
// The main purpose of this abstraction is to remove a circular dependency
// between a version_set and a version_edit.
class file_id_allocator {
public:
    file_id_allocator() = default;
    file_id_allocator(const file_id_allocator&) = delete;
    file_id_allocator(file_id_allocator&&) = delete;
    file_id_allocator& operator=(const file_id_allocator&) = delete;
    file_id_allocator& operator=(file_id_allocator&&) = delete;
    virtual ~file_id_allocator() = default;

    // Allocate a new ID that will not clash with any other files.
    virtual internal::file_id allocate_id() = 0;
};

// A class representing all the incremental changes needed to progress from one
// version to another version.
//
// Additionally, these track and book keep any IDs allocated while creating this
// version edit. All active edits are tracked in the version_set, which can
// ensure that any new files are not garbage collected.
class version_edit {
public:
    explicit version_edit(
      file_id_allocator* id_allocator, const internal::options& options)
      : _id_allocator(id_allocator)
      , _mutations_by_level(options.levels.size()) {}

    version_edit(const version_edit&) = delete;
    version_edit(version_edit&&) = delete;
    version_edit& operator=(const version_edit&) = delete;
    version_edit& operator=(version_edit&& o) = delete;
    ~version_edit() = default;

    // Set the compaction pointer, which is where the next compaction should
    // begin.
    void set_compact_pointer(internal::level level, internal::key key) {
        _mutations_by_level[level].compact_pointer = std::move(key);
    }

    // Set the latest seqno for the data written, this only needs to be set when
    // new data is added to the database which is only memtable flushes.
    void set_last_seqno(internal::sequence_number seqno) {
        _last_seqno = seqno;
    }

    // Allocate an ID for a file that will be added to the edit via `add_file`.
    internal::file_id allocate_id() {
        auto id = _id_allocator->allocate_id();
        _min_allocated_id = std::min(_min_allocated_id, id);
        return id;
    }

    // The parameters to `add_file`
    struct added_file {
        internal::level level;
        internal::file_handle file_handle;
        uint64_t file_size;
        internal::key smallest;
        internal::key largest;
        internal::sequence_number oldest_seqno;
        internal::sequence_number newest_seqno;
    };

    // Add a file to the new version
    void add_file(added_file params) {
        _mutations_by_level[params.level].added_files.push_back(
          ss::make_lw_shared<file_meta_data>({
            .handle = params.file_handle,
            .file_size = params.file_size,
            .smallest = std::move(params.smallest),
            .largest = std::move(params.largest),
            .oldest_seqno = params.oldest_seqno,
            .newest_seqno = params.newest_seqno,
          }));
    }

    // Remove a file from this version.
    void remove_file(internal::level level, internal::file_handle h) {
        _mutations_by_level[level].removed_files.insert(h);
    }

    fmt::iterator format_to(fmt::iterator it) const;

private:
    friend class version_set;
    struct mutation {
        chunked_hash_set<internal::file_handle> removed_files;
        chunked_vector<ss::lw_shared_ptr<file_meta_data>> added_files;
        std::optional<internal::key> compact_pointer;

        fmt::iterator format_to(fmt::iterator) const;
    };
    file_id_allocator* _id_allocator;
    absl::FixedArray<mutation> _mutations_by_level;
    // This is safe because it is applied idempotently.
    std::optional<internal::sequence_number> _last_seqno;
    internal::file_id _min_allocated_id = internal::file_id::max();
    intrusive_list_hook _list_hook;
};

} // namespace lsm::db
