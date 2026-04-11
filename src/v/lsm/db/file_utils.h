// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/core/internal/files.h"
#include "lsm/db/version_edit.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::db {

struct by_smallest_key {
    using is_transparent = void;

    bool operator()(
      const ss::lw_shared_ptr<file_meta_data>& a,
      const ss::lw_shared_ptr<file_meta_data>& b) const {
        // Sort by smallest key, and break ties using file ID.
        return std::tie(a->smallest, a->handle.id)
               < std::tie(b->smallest, b->handle.id);
    }
};

size_t
total_file_size(const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files);

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view target);
size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  user_key_view target);

// Returns true iff some file in `files` overlaps the user key range
// [*smallest,*largest].
// smallest==nullopt represents a key smaller than all keys in the DB.
// largest==nullopt represents a key larger than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges in
// sorted order.
bool some_file_overlaps_range(
  bool disjoint_sorted_files,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  std::optional<user_key_view> smallest_key,
  std::optional<user_key_view> largest_key);

// Extracts the largest file b1 from `compaction_files` and then searches for a
// b2 in `level_files` for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
void add_boundary_inputs(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  chunked_vector<ss::lw_shared_ptr<file_meta_data>>* compaction_files);

// Return the minimal range that covers all entries in the inputs.
std::pair<internal::key, internal::key>
get_range(const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs);
std::pair<internal::key, internal::key> get_range(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs1,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs2);

} // namespace lsm::db
