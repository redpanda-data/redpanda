/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/remote_file.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/download_exception.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/gate.hh>

namespace cloud_storage {

remote_file::remote_file(
  remote& r,
  cache& cache,
  cloud_storage_clients::bucket_name bucket,
  remote_segment_path remote_path,
  retry_chain_node& retry_parent,
  ss::sstring log_prefix,
  remote::download_metrics metrics)
  : _remote(r)
  , _cache(cache)
  , _bucket(std::move(bucket))
  , _remote_path(std::move(remote_path))
  , _rtc(&retry_parent)
  , _ctxlog(cst_log, _rtc, std::move(log_prefix))
  , _metrics(std::move(metrics))
  , _cache_backoff_jitter(cache_thrash_backoff) {};

ss::future<ss::file> remote_file::hydrate_readable_file() {
    ss::gate::holder g(_gate);
    while (!_gate.is_closed()) {
        // If the file is in cache, return immediately.
        auto maybe_file = co_await _cache.get(_remote_path);
        if (maybe_file.has_value()) {
            co_return maybe_file->body;
        }
        // Otherwise, go to remote storage and put it in the cache.
        auto res = co_await _remote.download_stream(
          _bucket,
          _remote_path,
          [this](uint64_t size_bytes, ss::input_stream<char> s) {
              return put_in_cache(size_bytes, std::move(s));
          },
          _rtc,
          "file",
          _metrics);

        if (res != download_result::success) {
            throw download_exception(res, _remote_path);
        }
        maybe_file = co_await _cache.get(_remote_path);
        if (!maybe_file.has_value()) {
            // The file was evicted immediately after the put. Try again.
            co_await ss::sleep(_cache_backoff_jitter.next_duration());
            continue;
        }
        co_return maybe_file->body;
    }
    throw ss::gate_closed_exception();
}

ss::future<uint64_t>
remote_file::put_in_cache(uint64_t size_bytes, ss::input_stream<char> s) {
    try {
        auto reservation = co_await _cache.reserve_space(size_bytes, 1);
        co_await _cache.put(_remote_path, s, reservation).finally([&s] {
            return s.close();
        });
    } catch (...) {
        auto put_exception = std::current_exception();
        vlog(
          _ctxlog.warn,
          "Failed to write a segment file to cache, error: {}",
          put_exception);
        std::rethrow_exception(put_exception);
    }
    co_return size_bytes;
}

} // namespace cloud_storage
