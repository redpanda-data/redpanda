/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_storage/materialized_manifest_cache.h"
#include "cloud_storage/read_path_probes.h"
#include "config/property.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cloud_storage {

class async_manifest_materializer {
    using materialized_manifest_ptr = ss::shared_ptr<materialized_manifest>;

public:
    async_manifest_materializer(
      cloud_storage_clients::bucket_name,
      ss::sharded<remote>*,
      ss::sharded<cloud_io::cache>*,
      const remote_path_provider*,
      const partition_manifest*);

    async_manifest_materializer(const async_manifest_materializer&) = delete;
    async_manifest_materializer&
    operator=(const async_manifest_materializer&) = delete;

    // rtc node is not movable, so we can't move the materializer
    async_manifest_materializer(async_manifest_materializer&&) = delete;
    async_manifest_materializer&
    operator=(async_manifest_materializer&&) = delete;

    ~async_manifest_materializer() = default;

public:
    ss::future<> start();
    ss::future<> stop();

public:
    ss::future<result<materialized_manifest_ptr, error_outcome>>
    materialize_manifest(const segment_meta& search_vec);

private:
    ss::future<> run_bg_loop();

    /// Load manifest from the cache
    ///
    /// The method reads manifest from the cache or downloads from the cloud.
    /// Local state is not changed. The returned manifest has to be stored
    /// in the view after the call.
    /// \throws
    ///     - not_found if the manifest doesn't exist
    ///     - repeat if the manifest is being downloaded already
    ///     - TODO
    ss::future<result<spillover_manifest, error_outcome>>
    do_materialize_manifest(remote_manifest_path path) noexcept;

    /// Load manifest from the cloud
    ///
    /// On success put serialized copy into the cache. The method should only be
    /// called if the manifest is not available in the cache. The state of the
    /// view is not changed.
    ss::future<result<spillover_manifest, error_outcome>>
    hydrate_manifest(remote_manifest_path path) noexcept;

    /// Convert segment_meta to spillover manifest path
    remote_manifest_path
    get_spillover_manifest_path(const segment_meta& meta) const;

private:
    ss::condition_variable _cvar;
    ss::abort_source _as;
    ss::gate _gate;

    retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;

    cloud_storage_clients::bucket_name _bucket;
    ss::sharded<remote>* _remote;
    ss::sharded<cloud_io::cache>* _cache;
    const remote_path_provider* _remote_path_provider;
    const partition_manifest* _stm_manifest;

    config::binding<std::chrono::milliseconds> _timeout;
    config::binding<std::chrono::milliseconds> _backoff;
    config::binding<size_t> _read_buffer_size;
    config::binding<int16_t> _readahead_size;
    config::binding<std::chrono::milliseconds> _manifest_meta_ttl;

    materialized_manifest_cache* _manifest_cache;
    ts_read_path_probe* _ts_probe;

    /// Materialization request which is sent to background fiber
    struct materialization_request_t {
        segment_meta search_vec;
        ss::promise<result<materialized_manifest_ptr, error_outcome>> promise;
        std::unique_ptr<ts_read_path_probe::hist_t::measurement> _measurement;
    };
    std::deque<materialization_request_t> _requests;
};

} // namespace cloud_storage
