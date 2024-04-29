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
#include "io/page.h"

namespace experimental::io {

/**
 * The page cache tracks pages and controls cache eviction.
 */
class page_cache {
    struct evict {
        bool operator()(page&) noexcept;
    };

    struct cost {
        size_t operator()(const page&) noexcept;
    };

    using cache_type = cache<page, &page::cache_hook, evict, cost>;

public:
    using config = cache_type::config;

    /**
     * Initialize with the given configuration.
     */
    explicit page_cache(config cfg)
      : cache_(cfg) {}

    /**
     * Insert @page into the cache.
     *
     * The page must not already be stored in the cache.
     */
    void insert(page& page) noexcept;

    /**
     * Remove @page from the cache.
     *
     * The page must currently be stored in the cache.
     */
    void remove(const page&) noexcept;

private:
    cache_type cache_;
};

} // namespace experimental::io
