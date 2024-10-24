// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include <absl/container/btree_set.h>

#include <filesystem>
#include <vector>

namespace storage {

// RAII class used to keep track of files in case of an early return (e.g.
// keeping track of staging files during compaction, so they may be cleaned up
// if compaction is aborted).
//
// NOTE: scoped_file_tracker is additive to the input set_t -- calling clear()
// on a value will not remove that value from the tracked files if it already
// exists.
class scoped_file_tracker {
public:
    using set_t = absl::btree_set<std::filesystem::path>;
    scoped_file_tracker(
      set_t* tracked_files,
      std::vector<std::filesystem::path> may_need_tracking)
      : tracked_files_(tracked_files)
      , may_need_tracking_(std::move(may_need_tracking)) {}

    scoped_file_tracker(scoped_file_tracker&& other) noexcept
      : tracked_files_(std::exchange(other.tracked_files_, nullptr))
      , may_need_tracking_(std::move(other.may_need_tracking_)) {}

    scoped_file_tracker& operator=(scoped_file_tracker&& other) = delete;
    scoped_file_tracker(const scoped_file_tracker&) = delete;
    scoped_file_tracker& operator=(const scoped_file_tracker&) = delete;

    ~scoped_file_tracker() {
        if (!tracked_files_) {
            return;
        }
        // If we didn't clear(), add them to the tracked set.
        for (const auto& file : may_need_tracking_) {
            tracked_files_->emplace(file);
        }
    }

    // Once called, will not add to `tracked_files_` upon destruction.
    void clear() { may_need_tracking_.clear(); }

private:
    set_t* tracked_files_{nullptr};

    // Files that may end up being left in `_tracked_files` if the tracking
    // isn't canceled.
    std::vector<std::filesystem::path> may_need_tracking_{};
};

} // namespace storage
