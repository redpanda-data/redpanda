/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_materializer.h"

#include "cloud_io/cache_service.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/remote.h"
#include "ssx/future-util.h"

#include <seastar/core/fstream.hh>

namespace cloud_storage {

async_manifest_materializer::async_manifest_materializer(
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<remote>* remote,
  ss::sharded<cloud_io::cache>* cache,
  const remote_path_provider* path_provider,
  const partition_manifest* stm_manifest)
  : _rtcnode(_as)
  , _ctxlog(cst_log, _rtcnode, stm_manifest->get_ntp().path())
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _cache(cache)
  , _remote_path_provider(path_provider)
  , _stm_manifest(stm_manifest)
  , _timeout(
      config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms.bind())
  , _backoff(config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
  , _read_buffer_size(config::shard_local_cfg().storage_read_buffer_size.bind())
  , _readahead_size(
      config::shard_local_cfg().storage_read_readahead_count.bind())
  , _manifest_meta_ttl(
      config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.bind())
  , _manifest_cache(
      &_remote->local().materialized().get_materialized_manifest_cache())
  , _ts_probe(&remote->local().materialized().get_read_path_probe())

{}

ss::future<> async_manifest_materializer::start() {
    ssx::spawn_with_gate(_gate, [this] { return run_bg_loop(); });
    return ss::now();
}

ss::future<> async_manifest_materializer::stop() {
    _as.request_abort();
    _cvar.broken();
    co_await _gate.close();
}

ss::future<
  result<async_manifest_materializer::materialized_manifest_ptr, error_outcome>>
async_manifest_materializer::materialize_manifest(const segment_meta& meta) {
    auto res = _manifest_cache->get(
      std::make_tuple(
        _stm_manifest->get_ntp(),
        _stm_manifest->get_revision_id(),
        meta.base_offset),
      _ctxlog);
    if (res) {
        return ss::make_ready_future<
          result<materialized_manifest_ptr, error_outcome>>(std::move(res));
    }
    // Send materialization request to background loop
    materialization_request_t request{
      .search_vec = meta,
      ._measurement = _ts_probe->spillover_manifest_latency(),
    };
    auto fut = request.promise.get_future();
    _requests.emplace_back(std::move(request));
    _cvar.signal();
    return fut;
}

ss::future<> async_manifest_materializer::run_bg_loop() {
    std::exception_ptr exc_ptr;
    bool shutting_down{false};

    try {
        while (!_as.abort_requested()) {
            co_await _cvar.when(
              [&] { return !_requests.empty() || _as.abort_requested(); });
            _as.check();
            if (_requests.empty()) {
                continue;
            }
            auto front = std::move(_requests.front());
            _requests.pop_front();
            try {
                auto path = get_spillover_manifest_path(front.search_vec);
                vlog(
                  _ctxlog.debug,
                  "Processing spillover manifest request {}, path: {}",
                  front.search_vec,
                  path);

                vassert(
                  front.search_vec.base_offset
                    < _stm_manifest->get_start_offset().value_or(
                      model::offset::max()),
                  "Request {} refers to STM manifest (start offset {})",
                  front.search_vec,
                  _stm_manifest->get_start_offset());

                if (!_manifest_cache->contains(
                      std::make_tuple(
                        _stm_manifest->get_ntp(),
                        _stm_manifest->get_revision_id(),
                        front.search_vec.base_offset))) {
                    // Manifest is not cached and has to be hydrated and/or
                    // materialized.
                    vlog(
                      _ctxlog.debug,
                      "Preparing cache for manifest with {} bytes, path {}",
                      front.search_vec.metadata_size_hint,
                      path);
                    // The timeout is TTL x2 because the cursor is allowed
                    // to hold on to the manifest for up to TTL ms. This
                    // means that waiting exactly TTL milliseconds is not
                    // enough because we need some time for cache to evict
                    // the item and then TTL milliseconds for the cursor
                    // timer to fire.
                    auto u = co_await _manifest_cache->prepare(
                      front.search_vec.metadata_size_hint,
                      _ctxlog,
                      _manifest_meta_ttl() * 2);
                    // At this point we have free memory to download the
                    // spillover manifest.
                    auto m_res = co_await do_materialize_manifest(path);
                    if (m_res.has_failure()) {
                        if (m_res.error() == error_outcome::shutting_down) {
                            vlog(
                              _ctxlog.info,
                              "Stopping manifest hydration background loop due "
                              "to shutdown");

                            front.promise.set_value(
                              error_outcome::shutting_down);

                            shutting_down = true;
                            break;
                        } else {
                            vlog(
                              _ctxlog.error,
                              "Failed to materialize manifest {}, vec: {}, "
                              "error: "
                              "{}",
                              path,
                              front.search_vec,
                              m_res.error());
                            front.promise.set_value(m_res.as_failure());
                        }
                        continue;
                    }
                    // Put newly materialized manifest into the cache
                    auto lso = m_res.value().get_start_offset();
                    vlog(
                      _ctxlog.debug,
                      "Manifest with LSO {} is materialized, using {} "
                      "units to put it into the cache {{cache size: "
                      "{}/{}}}",
                      lso,
                      u.count(),
                      _manifest_cache->size(),
                      _manifest_cache->size_bytes());
                    _manifest_cache->put(
                      std::move(u), std::move(m_res.value()), _ctxlog);
                    _ts_probe->set_spillover_manifest_bytes(
                      static_cast<int64_t>(_manifest_cache->size_bytes()));
                    _ts_probe->set_spillover_manifest_instances(
                      static_cast<int32_t>(_manifest_cache->size()));
                    vlog(
                      _ctxlog.debug,
                      "Manifest with LSO {} is cached {{cache size: "
                      "{}/{}}}",
                      lso,
                      _manifest_cache->size(),
                      _manifest_cache->size_bytes());
                } else {
                    vlog(_ctxlog.debug, "Manifest is already materialized");
                }
                auto cached = _manifest_cache->get(
                  std::make_tuple(
                    _stm_manifest->get_ntp(),
                    _stm_manifest->get_revision_id(),
                    front.search_vec.base_offset),
                  _ctxlog);
                front.promise.set_value(cached);
                vlog(
                  _ctxlog.debug,
                  "Spillover manifest request {} processed successfully, "
                  "found manifest that contains offset range [{}:{}]",
                  front.search_vec,
                  cached->manifest.get_start_offset(),
                  cached->manifest.get_last_offset());
            } catch (const std::system_error& err) {
                vlog(
                  _ctxlog.error,
                  "Failed processing request {}, exception: {} : {}",
                  front.search_vec,
                  err.code(),
                  err.what());
                front.promise.set_to_current_exception();
            } catch (...) {
                vlog(
                  _ctxlog.error,
                  "Failed processing request {}, exception: {}",
                  front.search_vec,
                  std::current_exception());
                front.promise.set_to_current_exception();
            }
        }
    } catch (...) {
        exc_ptr = std::current_exception();
        if (ssx::is_shutdown_exception(exc_ptr)) {
            vlog(
              _ctxlog.debug,
              "Shut down exception caught in manifest materialization loop: {}",
              exc_ptr);
        } else {
            vlog(_ctxlog.error, "Unexpected exception: {}", exc_ptr);
        }
    }
    if (exc_ptr) {
        // Unblock all readers in case of error
        for (auto& req : _requests) {
            req.promise.set_exception(exc_ptr);
        }
    } else if (shutting_down) {
        for (auto& req : _requests) {
            req.promise.set_value(error_outcome::shutting_down);
        }
    }
    co_return;
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_materializer::do_materialize_manifest(
  remote_manifest_path path) noexcept {
    try {
        auto h = _gate.hold();
        spillover_manifest manifest(
          _stm_manifest->get_ntp(), _stm_manifest->get_revision_id());
        // Perform simple scan of the manifest list
        // Probe cache. If not available or in case of race with cache eviction
        // hydrate manifest from the cloud.
        auto cache_status = co_await _cache->local().is_cached(path());
        switch (cache_status) {
        case cloud_io::cache_element_status::in_progress:
            vlog(_ctxlog.warn, "Concurrent manifest hydration, path {}", path);
            co_return error_outcome::repeat;
        case cloud_io::cache_element_status::not_available: {
            auto res = co_await hydrate_manifest(path);
            if (res.has_failure()) {
                if (res.error() == error_outcome::shutting_down) {
                    co_return res;
                }

                vlog(
                  _ctxlog.error,
                  "failed to download manifest, object key: {}, error: {}",
                  path,
                  res.error());
                co_return error_outcome::manifest_download_error;
            }
            manifest = std::move(res.value());
        } break;
        case cloud_io::cache_element_status::available: {
            auto res = co_await _cache->local().get(path());
            if (!res.has_value()) {
                vlog(
                  _ctxlog.warn,
                  "failed to read cached manifest, object key: {}",
                  path);
                // Cache race removed the file after `is_cached` check, the
                // upper layer is supposed to retry the call.
                co_return error_outcome::repeat;
            }
            std::exception_ptr update_err;
            try {
                ss::file_input_stream_options options{
                  .buffer_size = _read_buffer_size(),
                  .read_ahead = static_cast<uint32_t>(_readahead_size()),
                };
                auto data_stream = ss::make_file_input_stream(
                  res->body, 0, std::move(options));
                co_await manifest.update(std::move(data_stream));
                vlog(
                  _ctxlog.debug,
                  "Manifest is materialized, start offset {}, last offset {}",
                  manifest.get_start_offset(),
                  manifest.get_last_offset());
            } catch (...) {
                vlog(
                  _ctxlog.error,
                  "Error during manifest update: {}",
                  std::current_exception());
                update_err = std::current_exception();
            }
            co_await res->body.close();
            if (update_err) {
                std::rethrow_exception(update_err);
            }
        } break;
        }
        _ts_probe->on_spillover_manifest_materialization();
        co_return manifest;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_materializer::hydrate_manifest(
  remote_manifest_path path) noexcept {
    try {
        spillover_manifest manifest(
          _stm_manifest->get_ntp(), _stm_manifest->get_revision_id());
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        // Spillover manifests are always serde-encoded
        auto fk = std::make_pair(manifest_format::serde, path);
        auto res = co_await _remote->local().download_manifest(
          _bucket, fk, manifest, fib);
        if (res != download_result::success) {
            vlog(
              _ctxlog.error,
              "failed to download manifest {}, object key: {}",
              res,
              path);
            co_return error_outcome::manifest_download_error;
        }
        auto [str, len] = co_await manifest.serialize();
        auto reservation = co_await _cache->local().reserve_space(len, 1);
        co_await _cache->local().put(
          manifest.get_manifest_path(*_remote_path_provider)(),
          str,
          reservation);
        _ts_probe->on_spillover_manifest_hydration();
        vlog(
          _ctxlog.debug,
          "hydrated manifest {} with {} elements",
          path,
          manifest.size());
        co_return std::move(manifest);
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(
              _ctxlog.debug,
              "Shut down prevented manifest {} materialization: {}",
              path,
              ex);
            co_return error_outcome::shutting_down;
        } else {
            vlog(
              _ctxlog.error, "Failed to materialize manifest {}: {}", path, ex);
            co_return error_outcome::failure;
        }
    }
}

remote_manifest_path async_manifest_materializer::get_spillover_manifest_path(
  const segment_meta& meta) const {
    spillover_manifest_path_components comp{
      .base = meta.base_offset,
      .last = meta.committed_offset,
      .base_kafka = meta.base_kafka_offset(),
      .next_kafka = meta.next_kafka_offset(),
      .base_ts = meta.base_timestamp,
      .last_ts = meta.max_timestamp,
    };
    return remote_manifest_path{
      _remote_path_provider->spillover_manifest_path(*_stm_manifest, comp)};
}

} // namespace cloud_storage
