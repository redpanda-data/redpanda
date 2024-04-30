/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "io/cache.h"
#include "io/page_set.h"
#include "io/scheduler.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

namespace experimental::io {

class page_cache;

/**
 * The pager wraps a normal file and presents an abstraction of an append-only
 * file. All standard life cycle events of a file must be handled out-of-band,
 * such as file creation, deletion, and renaming.
 *
 * The pager is responsible for managing the index of in-memory pages and
 * offering write-back caching capabilities in conjunction with the page cache
 * and the I/O scheduler.
 */
class pager {
    static constexpr uint64_t page_size = 4096;

public:
    /**
     * Construct a new pager for the file identified by \p file with initial
     * size \size. Any data contained in the underlying file at offsets greather
     * than or equal to \p size will not be visible and will be overwritten when
     * new data is appended.
     */
    pager(
      std::filesystem::path file,
      size_t size,
      persistence*,
      page_cache*,
      scheduler*);

    pager(const pager&) = delete;
    pager& operator=(const pager&) = delete;
    pager(pager&&) noexcept = delete;
    pager& operator=(pager&&) noexcept = delete;
    ~pager() noexcept = default;

    /**
     * Shutdown the pager. After calling this do no submit reads or writes.
     * Inflight I/O operations will be completed.
     */
    seastar::future<> close() noexcept;

    /**
     * Writes data to the end of the file. The file size will be extended by the
     * number of bytes being appended. When the returned future completes the
     * file size will have been updated and the newly appended data will be
     * visible to readers. The data may not yet have been written to the
     * underlying file or persistent storage.
     */
    seastar::future<> append(seastar::temporary_buffer<char> data) noexcept;

    /**
     * Configuration object for read interface.
     */
    struct read_config {
        uint64_t offset;
        uint64_t length;
    };

    /**
     * Return pages that overlap the region defined by \p cfg. A subset of the
     * pages for the entire range may be returned, in which case the caller
     * should advance the starting offset to retrieve additional pages. Reads
     * past the end-of-file will return an empty result.
     *
     * Caller should ensure that the length of the read does not result in a
     * vector that would exceed recommended contiguous allocation limits.
     */
    seastar::future<std::vector<seastar::lw_shared_ptr<page>>>
    read(read_config cfg) noexcept;

    size_t size() const { return size_; }

private:
    static seastar::lw_shared_ptr<page> alloc_page(
      uint64_t offset, std::optional<cache_hook> hook = std::nullopt) noexcept;

    /*
     * Read a page from the underlying file.
     */
    seastar::future<seastar::lw_shared_ptr<page>>
    get_page(uint64_t offset) noexcept;

    static void handle_completion(page&) noexcept;

    /// The underlying file managed by this pager.
    std::filesystem::path file_;

    /// The readable size of the file.
    size_t size_;

    /// Global page cache eviction. Doesn't own the pages.
    page_cache* cache_;

    /// Pages that contain data for this file.
    page_set pages_;

    /// The IO scheduler and queue for IO operations on this file.
    scheduler* sched_;
    scheduler::queue queue_;
};

} // namespace experimental::io
