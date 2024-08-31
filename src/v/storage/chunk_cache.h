/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"
#include "ssx/semaphore.h"
#include "storage/segment_appender_chunk.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace storage::internal {

class chunk_cache {
    using chunk = segment_appender_chunk;
    using chunk_ptr = ss::lw_shared_ptr<chunk>;

public:
    /**
     * The chunk cache serves all segment files, which individually may have
     * different alignment requirements (e.g. different file systems or
     * devices). However current restrictions mean that all segments share the
     * same device and file system. For convenience we use the fail safe size
     * specified by seastar, and dynamically verify compatibility for each file.
     */
    static constexpr const alignment alignment{4_KiB};

    chunk_cache() noexcept;
    chunk_cache(chunk_cache&&) = delete;
    chunk_cache& operator=(chunk_cache&&) = delete;
    chunk_cache(const chunk_cache&) = delete;
    chunk_cache& operator=(const chunk_cache&) = delete;
    ~chunk_cache() noexcept = default;

    ss::future<> start();

    void add(const chunk_ptr& chunk);

    ss::future<chunk_ptr> get();

    size_t chunk_size() const { return _chunk_size; }

private:
    ss::future<chunk_ptr> do_get();

    chunk_ptr pop_or_allocate();

    ss::chunked_fifo<chunk_ptr> _chunks;
    ssx::semaphore _sem{0, "s/chunk-cache"};
    size_t _size_available{0};
    size_t _size_total{0};
    const size_t _size_target;
    const size_t _size_limit;

    const size_t _chunk_size{0};
};

chunk_cache& chunks();

} // namespace storage::internal
