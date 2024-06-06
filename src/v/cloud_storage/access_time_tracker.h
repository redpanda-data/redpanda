/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "hashing/xx.h"
#include "recursive_directory_walker.h"
#include "seastar/core/iostream.hh"
#include "serde/envelope.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <string_view>

namespace cloud_storage {

enum class tracker_version : uint8_t { v1, v2 };

struct file_metadata {
    uint32_t atime_sec;
    uint64_t size;
    std::chrono::system_clock::time_point time_point() const;
};

/// Access time tracker maps cache entry file paths to their last accessed
/// timestamp and file size.
class access_time_tracker {
    using timestamp_t = uint32_t;
    using table_t = absl::btree_map<ss::sstring, file_metadata>;

public:
    /// Add metadata to the container.
    void add(
      ss::sstring path,
      std::chrono::system_clock::time_point atime,
      size_t size);

    /// Remove key from the container.
    void remove(std::string_view) noexcept;

    /// Return file metadata for key.
    std::optional<file_metadata> get(const std::string& key) const;

    ss::future<> write(
      ss::output_stream<char>&, tracker_version version = tracker_version::v2);
    ss::future<> read(ss::input_stream<char>&);

    /// Returns true if tracker has new data which wasn't serialized
    /// to disk.
    bool is_dirty() const;

    using add_entries_t = ss::bool_class<struct trim_additive_tag>;
    /// Remove every key which isn't present in list of input files
    ss::future<> sync(
      const fragmented_vector<file_list_item>&,
      add_entries_t add_entries = add_entries_t::no);

    size_t size() const { return _table.size(); }

    fragmented_vector<file_list_item> lru_entries() const;

private:
    /// Returns true if the key's metadata should be tracked.
    /// We do not wish to track index files and transaction manifests
    /// as they are just an appendage to segment/chunk files and are
    /// purged along with them.
    bool should_track(std::string_view key) const;

    /// Drain _pending_upserts for any writes made while table lock was held
    void on_released_table_lock();

    table_t _table;

    // Lock taken during async loops over the table (ser/de and trim())
    // modifications may proceed without the lock if it is not taken.
    // When releasing lock, drain _pending_upserts.
    ss::semaphore _table_lock{1};

    // Calls into add/remove populate this if the _serialization_lock is
    // unavailable.  The serialization code is responsible for draining it upon
    // releasing the lock.
    absl::btree_map<ss::sstring, std::optional<file_metadata>> _pending_upserts;

    bool _dirty{false};
};

} // namespace cloud_storage
