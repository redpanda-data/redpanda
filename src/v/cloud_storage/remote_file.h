/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/types.h"
#include "utils/retry_chain_node.h"

namespace cloud_storage {

// Encapsulates and exposes a file to be downloaded from remote storage and
// backed by the cloud cache.
class remote_file {
public:
    remote_file(
      remote& r,
      cache& cache,
      cloud_storage_clients::bucket_name bucket,
      remote_segment_path remote_path,
      retry_chain_node& retry_parent,
      ss::sstring log_prefix,
      remote::download_metrics metrics = {});
    remote_file(const remote_segment&) = delete;
    remote_file(remote_segment&&) = delete;
    remote_file& operator=(const remote_segment&) = delete;
    remote_file& operator=(remote_segment&&) = delete;
    ~remote_file() = default;

    // Hydrates the file of the remote path, fetching from the cache or
    // downloading from remote storage if it doesn't exist. Returns a file that
    // has been opened with read flags.
    //
    // Throws an exception if there was an issue downloading or caching.
    ss::future<ss::file> hydrate_readable_file();

    std::filesystem::path local_path() const {
        return _cache.get_local_path(_remote_path);
    }

private:
    // Puts the given stream into the cache. Expected to be used as a
    // remote::try_consume_stream.
    //
    // Throws an exception if there was a problem inserting into the cache.
    ss::future<uint64_t>
    put_in_cache(uint64_t size_bytes, ss::input_stream<char> s);

    remote& _remote;
    cache& _cache;
    cloud_storage_clients::bucket_name _bucket;
    remote_segment_path _remote_path;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    const remote::download_metrics _metrics;
    simple_time_jitter<ss::lowres_clock> _cache_backoff_jitter;

    ss::gate _gate;
};

} // namespace cloud_storage
