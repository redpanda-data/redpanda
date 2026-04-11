// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/file_utils.h"

#include <boost/range/join.hpp>

#include <functional>

namespace lsm::db {

size_t total_file_size(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files) {
    size_t total = 0;
    for (const auto& file : files) {
        total += file->file_size;
    }
    return total;
}

namespace {

template<typename Key>
size_t find_file_impl(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files, Key target) {
    size_t left = 0;
    size_t right = files.size();
    while (left < right) {
        size_t mid = (left + right) / 2;
        const auto& f = files[mid];
        bool target_after_file = false;
        if constexpr (std::is_same_v<internal::key_view, Key>) {
            target_after_file = f->largest < target;
        } else if constexpr (std::is_same_v<user_key_view, Key>) {
            target_after_file = f->largest.user_key() < target;
        } else {
            static_assert(false, "unsupported type");
        }
        if (target_after_file) {
            // kkey at mid.largest is < target. Therefore all files at or before
            // mid are uninteresting.
            left = mid + 1;
        } else {
            // Key at mid.largest is >= target. Therefore all files after "mid"
            // are uninteresting.
            right = mid;
        }
    }
    return right;
}

} // namespace

size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  internal::key_view target) {
    return find_file_impl(files, target);
}

size_t find_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  user_key_view target) {
    return find_file_impl(files, target);
}

namespace {

bool after_file(
  const file_meta_data& file, const std::optional<user_key_view>& key) {
    return key && *key > file.largest.user_key();
}

bool before_file(
  const file_meta_data& file, const std::optional<user_key_view>& key) {
    return key && *key < file.smallest.user_key();
}

} // namespace

bool some_file_overlaps_range(
  bool disjoint_sorted_files,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  std::optional<user_key_view> smallest_key,
  std::optional<user_key_view> largest_key) {
    if (!disjoint_sorted_files) {
        // Need to check against all files
        for (const auto& file : files) {
            if (
              after_file(*file, smallest_key)
              || before_file(*file, largest_key)) {
                // no overlap
            } else {
                return true; // overlap
            }
        }
        return false;
    }
    size_t index = 0;
    if (smallest_key) {
        index = find_file(files, *smallest_key);
    }
    if (index >= files.size()) {
        // beginning of range is after all files, so no overlap.
        return false;
    }
    return !before_file(*files[index], largest_key);
}

namespace {

std::optional<internal::key_view> find_largest_key(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files) {
    auto it = std::ranges::max_element(
      files, std::less{}, [](const ss::lw_shared_ptr<file_meta_data>& file) {
          return internal::key_view{file->largest};
      });
    if (it == files.end()) {
        return std::nullopt;
    }
    return internal::key_view{(*it)->largest};
}

ss::lw_shared_ptr<file_meta_data> find_smallest_boundary_file(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& level_files,
  internal::key_view largest_key) {
    ss::lw_shared_ptr<file_meta_data> smallest_boundary_file = nullptr;
    for (const auto& file : level_files) {
        if (
          file->smallest > largest_key
          && file->smallest.user_key() == largest_key.user_key()) {
            if (
              !smallest_boundary_file
              || file->smallest < smallest_boundary_file->smallest) {
                smallest_boundary_file = file;
            }
        }
    }
    return smallest_boundary_file;
}

} // namespace

void add_boundary_inputs(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& files,
  chunked_vector<ss::lw_shared_ptr<file_meta_data>>* compaction_files) {
    auto largest_key = find_largest_key(*compaction_files);
    if (!largest_key) {
        return;
    }
    bool continue_searching = true;
    while (continue_searching) {
        auto smallest_boundary_file = find_smallest_boundary_file(
          files, *largest_key);
        if (smallest_boundary_file != nullptr) {
            largest_key = smallest_boundary_file->largest;
            compaction_files->push_back(std::move(smallest_boundary_file));
        } else {
            continue_searching = false;
        }
    }
}

namespace {

template<typename Range>
std::pair<internal::key, internal::key> get_key_range(const Range& r) {
    auto it = r.begin();
    auto end = r.end();
    vassert(it != end, "cannot get range for empty set of files");
    internal::key smallest = (*it)->smallest, largest = (*it)->largest;
    for (++it; it != end; ++it) {
        const auto& file = *it;
        if (file->smallest < smallest) {
            smallest = file->smallest;
        }
        if (file->largest > largest) {
            largest = file->largest;
        }
    }
    return std::make_pair(std::move(smallest), std::move(largest));
}

} // namespace

std::pair<internal::key, internal::key>
get_range(const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs) {
    return get_key_range(inputs);
}

std::pair<internal::key, internal::key> get_range(
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs1,
  const chunked_vector<ss::lw_shared_ptr<file_meta_data>>& inputs2) {
    return get_key_range(boost::join(inputs1, inputs2));
}

} // namespace lsm::db
