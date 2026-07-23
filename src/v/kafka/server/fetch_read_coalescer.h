/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "utils/chunked_kv_cache.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <optional>

namespace kafka {

// Defined in kafka/server/handlers/fetch.h.
struct read_result;

using shared_read = std::shared_ptr<const read_result>;

/// Same-key reads have byte-identical output. max_bytes is the requested
/// budget, sampled before allocation.
struct coalesce_key {
    // Build from the config's ktp_with_hash, not the sliced ktp(), to reuse
    // the planning-time hash.
    model::ktp_with_hash ktp;
    model::offset fetch_offset;
    model::isolation_level level;
    size_t max_bytes;
    // Obligatory (non-strict) reads guarantee >=1 batch; strict reads honor
    // max_bytes. Keyed apart so neither cross-serves the other, at the cost
    // of a duplicate read when one (ntp, offset) is fetched both ways at once.
    bool obligatory;

    friend bool operator==(const coalesce_key& a, const coalesce_key& b) {
        return a.fetch_offset == b.fetch_offset && a.level == b.level
               && a.max_bytes == b.max_bytes && a.obligatory == b.obligatory
               && a.ktp == b.ktp;
    }
};

struct coalesce_key_hash {
    // Reuses ktp's cached hash; mixes in the other fields.
    size_t operator()(const coalesce_key&) const;
};

/// Per-shard fetch read coalescer. Reads and serializes each unique key once,
/// then shares the result with concurrent and back-to-back readers of that key.
class fetch_read_coalescer {
    /// Coalescing state for one key: an in-flight read that concurrent readers
    /// await, plus a weakly retained completed read that later readers reuse.
    struct coalesce_entry {
        // Set while a read for this key is in flight; concurrent readers await
        // its future instead of starting their own. Reset once the read
        // resolves.
        std::optional<ss::shared_promise<shared_read>> inflight;
        // The last completed read, held weakly so the coalescer never pins it.
        std::weak_ptr<const read_result> ready;

        // A read is starting: install the promise waiters block on, and drop
        // any retained result since the fresh read supersedes it.
        void begin_read();
        // The read produced `result`: hand it to the waiters and retain it for
        // later readers.
        void finish_read(shared_read result);
        // The read failed: hand the exception to the waiters and retain
        // nothing.
        void fail_read(std::exception_ptr);
    };

public:
    using cache_t = utils::
      chunked_kv_cache<coalesce_key, coalesce_entry, coalesce_key_hash>;
    using cache_config = cache_t::config;

    /// Always constructed; disabling clears the cache via the binding watch.
    fetch_read_coalescer(cache_config, config::binding<bool> enabled);
    fetch_read_coalescer(const fetch_read_coalescer&) = delete;
    fetch_read_coalescer& operator=(const fetch_read_coalescer&) = delete;
    fetch_read_coalescer(fetch_read_coalescer&&) = delete;
    fetch_read_coalescer& operator=(fetch_read_coalescer&&) = delete;
    ~fetch_read_coalescer() = default;

    bool enabled() const { return _enabled(); }

    using read_fn = ss::noncopyable_function<ss::future<read_result>()>;

    /// Checks if there is a cached shared read for the provided `key`.
    /// One of the following actions is taken internally:
    ///   - If there is a ready read for the key and that read's end offset is
    ///   greater than or equal to `current_bound` then that read is returned.
    ///   - If there is an in-flight read for the key it is awaited and the
    ///   resulting read is returned.
    ///   - Otherwise read_fn runs inline and its result is shared and retained.
    /// Note that read_fn runs at most once, only on the miss path. If it throws
    /// the exception is propagated to all waiters.
    ///
    /// The returned shared_read is the only strong owner of the result. The
    /// cache keeps a weak handle so it never pins memory, so the caller must
    /// hold the returned ref for as long as the result is needed (in the fetch
    /// path this is until the response is sent to the client).
    ss::future<shared_read>
    get_or_insert(const coalesce_key&, model::offset current_bound, read_fn);

private:
    // Run read_fn inline on `entry` and share its result with the entry's
    // waiters, retaining it iff it is data-bearing and error-free.
    ss::future<shared_read>
      read_and_publish(ss::shared_ptr<coalesce_entry>, read_fn);

    void on_enabled_change();
    void setup_metrics();

    cache_config _cache_config;
    config::binding<bool> _enabled;
    // Held via optional because chunked_kv_cache has no clear() and deletes its
    // move; disable reset()s it.
    std::optional<cache_t> _cache;

    uint64_t _insertions{0};
    uint64_t _reinsertions{0};
    uint64_t _ready_hits{0};
    uint64_t _inflight_hits{0};
    metrics::internal_metric_groups _metrics;
};

} // namespace kafka
