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
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/core/lookup_result.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/weak_intrusive_list.h"
#include "lsm/io/persistence.h"

namespace lsm::db {

class version_set;
class compaction;

// A single immutable version of the database.
class version : public weak_intrusive_list<version> {
    // A private struct so that the constructor can only be used
    // internal to version (or friended classes).
    struct ctor {};

public:
    struct get_stats {
        ss::optimized_optional<ss::lw_shared_ptr<file_meta_data>> seek_file;
        internal::level seek_file_level;
    };

    version(ctor, version_set* vset);

    version() = delete;
    version(const version&) = delete;
    version(version&&) = delete;
    version& operator=(const version&) = delete;
    version& operator=(version&&) = delete;
    ~version() = default;

    // Append to *iters a sequence of iterators that will yield the contents of
    // this version when merged together.
    // REQUIRES: This version has been applied to a version_set.
    ss::future<>
    add_iterators(chunked_vector<std::unique_ptr<internal::iterator>>* iters);

    // Adds stats into the current state. Returns true if a new compaction may
    // need to be trigged, false otherwise.
    bool update_stats(const get_stats& stats);

    // Record a sample of bytes read at the specified internal key.
    // samples are take approximately once every read_bytes_period bytes.
    // Returns true if a new compaction may need to be trigged.
    ss::future<bool> record_read_sample(internal::key_view);

    // Return all files in level that overlap [begin,end].
    //
    // begin==nullptr, means before all keys.
    // end==nullptr, means after all keys.
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> get_overlapping_inputs(
      internal::level,
      std::optional<user_key_view> begin,
      std::optional<user_key_view> end);

    // Lookup the value for key.
    ss::future<lookup_result> get(internal::key_view target, get_stats*);

    // Returns true if some file in the specified level overlaps some part of
    // the specified key range.
    //
    // begin==nullopt, means before all keys.
    // end==nullopt, means after all keys.
    bool overlap_in_level(
      internal::level,
      std::optional<user_key_view> begin,
      std::optional<user_key_view> end);

    // Return the level at which we should place a new memtable compaction
    // result that covers the range [begin,end].
    internal::level
    pick_level_for_memtable_output(user_key_view begin, user_key_view end);

    size_t num_files(internal::level level) { return _files[level].size(); }

    // Call func(level_number, files) for every level in this version.
    void for_each_level(
      absl::FunctionRef<void(
        internal::level,
        const chunked_vector<ss::lw_shared_ptr<file_meta_data>>&)> func) const;

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class version_set;
    friend class compaction;

    std::unique_ptr<internal::iterator>
      create_concatenating_iterator(internal::level);

    // Call the function for every file that overlaps with the use in order from
    // newset to oldest. If an invocation of the function returns stop, no more
    // calls are made.
    ss::future<> for_each_overlapping(
      internal::key_view,
      absl::FunctionRef<ss::future<ss::stop_iteration>(
        internal::level, ss::lw_shared_ptr<file_meta_data>)>);

    version_set* _vset; // the set which this version belongs to
    // All the files in this version of the database.
    absl::FixedArray<chunked_vector<ss::lw_shared_ptr<file_meta_data>>> _files;

    // The next file to compact based on seek stats.
    ss::optimized_optional<ss::lw_shared_ptr<file_meta_data>> _file_to_compact;
    internal::level _file_to_compact_level;

    // The compaction score for each level.
    // Scores < 1 means that compaction is not strictly needed.
    absl::FixedArray<double> _compaction_scores;
};

// The representation of a database is a set of versions. The newest version is
// called "current". Older versions may be kept around to get a consistent view
// to live iterators.
//
// Each version keeps track of a set of table files per level. The entire set of
// versions is maintained in this data structure.
class version_set : public file_id_allocator {
    class builder;

public:
    version_set(
      io::metadata_persistence* persistence,
      table_cache* table_cache,
      ss::lw_shared_ptr<internal::options> options);

    // Return the current version of this set.
    ss::lw_shared_ptr<version> current() { return _current; }

    // Allocate a new file ID.
    internal::file_id allocate_id() override { return _next_file_id++; }

    // Create a new edit that can be applied to the version_set.
    //
    // These live edits are tracked so that we can ensure file GC does not
    // delete any of the files allocated for these edits.
    ss::lw_shared_ptr<version_edit> new_edit();

    // Apply the edit to form a new version of the database that is both saved
    // to persistence as well as set to be the current version.
    //
    // The edit is applied on top of the current version, and this method
    // requires external synchronization (it's not valid to call this method
    // concurrently).
    ss::future<> log_and_apply(ss::lw_shared_ptr<version_edit>);

    // Recover the last saved version of the database from the persistence
    // layer.
    ss::future<> recover();

    // Reload the manifest from disk. Returns true if the manifest was updated,
    // false if no change. Throws if the manifest would regress state.
    ss::future<bool> refresh();

    // The latest seqno applied to the LSM tree.
    std::optional<internal::sequence_number> last_seqno() const {
        return _last_seqno;
    }

    // Returns true iff some level needs compaction.
    bool needs_compaction() const;

    // Pick level and inputs for a new compaction run.
    // Returns std::nullopt if there is no compaction.
    ss::optimized_optional<std::unique_ptr<compaction>> pick_compaction();

    // Create an iterator that reads over the compaction inputs.
    ss::future<std::unique_ptr<internal::iterator>>
    make_input_iterator(compaction*, internal::iterator_options);

    // Get all the files that are currently being used by any live version.
    chunked_hash_set<internal::file_handle> get_live_files();

    // Return the minimum file ID that is not yet committed.
    internal::file_id min_uncommitted_file_id() const;

private:
    friend class version;
    friend class compaction;

    void set_current(ss::lw_shared_ptr<version>);
    void finalize(version*);

    struct manifest {
        ss::lw_shared_ptr<version> version;
        internal::file_id next_file_id;
        internal::sequence_number last_seqno;
        internal::database_epoch epoch;
    };
    // Write this version to a manifest file as a snapshot.
    ss::future<> write_manifest(manifest);
    ss::future<std::optional<manifest>> read_manifest();

    io::metadata_persistence* _persistence;
    table_cache* _table_cache;
    ss::lw_shared_ptr<internal::options> _options;
    ss::lw_shared_ptr<version> _current;
    intrusive_list<version_edit, &version_edit::_list_hook> _live_edits;
    absl::FixedArray<bool> _compacting_levels;
    internal::file_id _next_file_id = internal::file_id{2};
    internal::file_id _current_manifest_id;
    std::optional<internal::sequence_number> _last_seqno;
    absl::FixedArray<std::optional<internal::key>> _compact_pointer;
};

// Encapulate information about a compaction event.
class compaction {
    // A private struct so that the constructor can only be used
    // internal to version (or friended classes).
    struct ctor {};

public:
    compaction(
      ctor,
      ss::lw_shared_ptr<internal::options> options,
      ss::lw_shared_ptr<version> version,
      ss::lw_shared_ptr<version_edit> edit,
      internal::level level);

    ~compaction();

    // Return the level that is being compacted.  Inputs from "level"
    // and "level+1" will be merged to produce a set of "level+1" files.
    internal::level level() const { return _level; }
    // Return the object that holds the edits to the descriptor done
    // by this compaction.
    ss::lw_shared_ptr<version_edit> edit() { return _edit; }

    enum which : uint8_t {
        input_level = 0,
        output_level = 1,
    };

    // "which" must be either input_level or output_level
    size_t num_input_files(which w) const { return _inputs.at(w).size(); }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    ss::lw_shared_ptr<file_meta_data> input(which w, size_t i) const {
        return _inputs.at(w).at(i);
    }

    // Maximum size of files to build during this compaction.
    uint64_t max_output_file_size() const { return _max_output_file_size; }

    // Is this a trivial compaction that can be implemented by just
    // moving a single input file to the next level (no merging or splitting)
    bool is_trivial_move() const;

    // Add all inputs to this compaction as delete operations to *edit.
    void add_input_deletions(version_edit* edit);

    // Returns true if the information we have available guarantees that
    // the compaction is producing data in "level+1" for which no data exists
    // in levels greater than "level+1".
    bool is_base_level_for_key(internal::key_view key);

    // Returns true iff we should stop building the current output
    // before processing "internal_key".
    bool should_stop_before(internal::key_view key);

    fmt::iterator format_to(fmt::iterator) const;

private:
    friend class version;
    friend class version_set;

    internal::level _level;
    uint64_t _max_output_file_size = 0;
    ss::lw_shared_ptr<version> _input_version;
    ss::lw_shared_ptr<version_edit> _edit;
    // Each compaction reads inputs from "level_" and "level_+1"
    std::array<chunked_vector<ss::lw_shared_ptr<file_meta_data>>, 2>
      _inputs; // The two sets of inputs

    // State used to check for number of overlapping grandparent files
    // (parent == level_ + 1, grandparent == level_ + 2)
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> _grandparents;
    size_t _grandparent_index = 0;  // Index in grandparent_starts_
    bool _seen_key = false;         // Some output key has been seen
    uint64_t _overlapped_bytes = 0; // Bytes of overlap between current output
                                    // and grandparent files

    // State for implementing is_base_level_for_key

    // level_ptrs_ holds indices into input_version_->levels_: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level_ + 2).
    absl::FixedArray<size_t> _level_ptrs;
};

} // namespace lsm::db
