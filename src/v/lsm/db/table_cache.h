/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/functional/function_ref.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::db {

// A table cache keeps a cache of open file handles to SST files, and supports
// accessing reads and iterators to these files.
class table_cache {
public:
    class impl;

    table_cache(
      io::data_persistence*,
      size_t max_entries,
      ss::lw_shared_ptr<sst::block_cache>);
    table_cache(const table_cache&) = delete;
    table_cache(table_cache&&) = default;
    table_cache& operator=(const table_cache&) = delete;
    table_cache& operator=(table_cache&&) = default;
    ~table_cache();

    // Create an iterator. All iterators returned from this method must
    // be destructed before the `close` method is called.
    ss::future<std::unique_ptr<internal::iterator>>
    create_iterator(internal::file_handle, uint64_t file_size);

    // Calls `fn` if the seek to `key` on this table would return a valid value.
    ss::future<> get(
      internal::file_handle,
      uint64_t file_size,
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn);

    // Manually evict this file from the cache. There must not be any
    // open iterators or concurrent calls to `get` for this file.
    ss::future<> evict(internal::file_handle);

    // Close the cache and any readers that are cached.
    //
    // This *must* be called before destructing the cache.
    //
    // All iterators from `create_iterator` *must* be destroyed before calling
    // this method.
    ss::future<> close();

    /**
     * Cache statistics.
     */
    struct stat {
        // Total number of open file handles.
        size_t open_file_handles;
        // Current size of the small queue.
        size_t small_queue_size;
        // Current size of the main queue.
        size_t main_queue_size;

        fmt::iterator format_to(fmt::iterator) const;
    };

    stat statistics() const;

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::db
