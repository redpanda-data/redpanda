/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>

namespace cloud_topics {

/// Shard-local cache of parsed L1 object footers, keyed on object_id.
///
/// Backed by s3_fifo so single-scan footers are evicted without polluting the
/// cache for objects accessed repeatedly across fetches.
class l1_footer_cache {
public:
    /// Construct a footer cache with the given capacity.
    /// Pass max_entries=0 to disable caching entirely.
    explicit l1_footer_cache(size_t max_entries);

    /// Look up a cached footer. Returns nullptr on miss.
    ss::shared_ptr<const l1::footer> get(l1::object_id oid);

    /// Insert a footer, evicting an entry if the cache is full.
    void put(l1::object_id oid, ss::shared_ptr<const l1::footer> footer);

    /// No-op — required by the sharded service protocol.
    ss::future<> stop();

private:
    using kv_cache_t = utils::chunked_kv_cache<l1::object_id, const l1::footer>;
    std::unique_ptr<kv_cache_t> _cache;
};

} // namespace cloud_topics
