/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_view.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "fmt/chrono.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "utils/human.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/defer.hh>

#include <boost/lexical_cast.hpp>
#include <boost/outcome/success_failure.hpp>

#include <exception>
#include <functional>
#include <iterator>
#include <optional>
#include <system_error>
#include <variant>

namespace {
ss::log_level log_level_for_error(cloud_storage::error_outcome err) {
    if (err == cloud_storage::error_outcome::shutting_down) {
        return ss::log_level::debug;
    }
    return ss::log_level::error;
}
} // namespace

namespace cloud_storage {

static ss::sstring to_string(const async_view_search_query_t& t) {
    return ss::visit(
      t,
      [&](model::offset ro) { return ssx::sformat("[offset: {}]", ro); },
      [&](kafka::offset ko) { return ssx::sformat("[kafka offset: {}]", ko); },
      [&](const async_view_timestamp_query& ts) {
          return ssx::sformat("{}", ts);
      });
}

std::ostream& operator<<(std::ostream& s, const async_view_search_query_t& q) {
    s << to_string(q);
    return s;
}

static bool
contains(const partition_manifest& m, const async_view_search_query_t& query) {
    return ss::visit(
      query,
      [&](model::offset o) {
          return o >= m.get_start_offset().value_or(model::offset::max())
                 && o <= m.get_last_offset();
      },
      [&](kafka::offset k) {
          return k >= m.get_start_kafka_offset()
                 && k < m.get_next_kafka_offset();
      },
      [&](const async_view_timestamp_query& ts_query) {
          if (m.size() == 0) {
              return false;
          }

          auto kafka_start_offset = m.get_start_kafka_offset();
          if (!kafka_start_offset.has_value()) {
              return false;
          }

          auto kafka_last_offset = m.get_last_kafka_offset();
          if (!kafka_last_offset.has_value()) {
              return false;
          }

          auto range_overlaps = ts_query.min_offset <= kafka_last_offset.value()
                                && ts_query.max_offset
                                     >= kafka_start_offset.value();

          return range_overlaps && ts_query.ts >= m.begin()->base_timestamp
                 && ts_query.ts <= m.last_segment()->max_timestamp;
      });
}

std::ostream& operator<<(std::ostream& o, async_manifest_view_cursor_status s) {
    switch (s) {
    case async_manifest_view_cursor_status::empty:
        fmt::print(o, "empty");
        break;
    case async_manifest_view_cursor_status::evicted:
        fmt::print(o, "evicted");
        break;
    case async_manifest_view_cursor_status::materialized_stm:
        fmt::print(o, "materialized_stm");
        break;
    case async_manifest_view_cursor_status::materialized_spillover:
        fmt::print(o, "materialized_spillover");
        break;
    }
    return o;
}

async_manifest_view_cursor::async_manifest_view_cursor(
  async_manifest_view& view,
  model::offset begin,
  model::offset end_inclusive,
  ss::lowres_clock::duration timeout)
  : _view(view)
  , _current(std::monostate())
  , _idle_timeout(timeout)
  , _begin(begin)
  , _end(end_inclusive) {
    _timer.set_callback([this] { on_timeout(); });
}

async_manifest_view_cursor_status
async_manifest_view_cursor::get_status() const {
    return ss::visit(
      _current,
      [](std::monostate) { return async_manifest_view_cursor_status::empty; },
      [](stale_manifest) { return async_manifest_view_cursor_status::evicted; },
      [](std::reference_wrapper<const partition_manifest>) {
          return async_manifest_view_cursor_status::materialized_stm;
      },
      [](const ss::shared_ptr<materialized_manifest>&) {
          return async_manifest_view_cursor_status::materialized_spillover;
      });
}

ss::future<> async_manifest_view_cursor::maybe_sync_manifest() {
    if (manifest_needs_sync()) {
        auto res = co_await seek(_stm_start_offset.value());
        if (res.has_failure()) {
            throw std::system_error(res.error());
        }
        if (!res.value()) {
            vlog(_view._ctxlog.error, "Can't sync manifest");
            _current = stale_manifest();
        }
    }
}

bool async_manifest_view_cursor::manifest_needs_sync() const {
    if (std::holds_alternative<stm_manifest_t>(_current)) {
        // Invariant: if _current points to the STM manifest the
        //            _stm_start_offset is set
        vassert(_stm_start_offset.has_value(), "STM start offset is not set");
        const auto& m = std::get<stm_manifest_t>(_current).get();
        auto so = m.get_start_offset().value_or(model::offset{});
        return so != _stm_start_offset.value();
    }
    return false;
}

ss::future<result<bool, error_outcome>>
async_manifest_view_cursor::seek(async_view_search_query_t q) {
    if (std::holds_alternative<model::offset>(q)) {
        auto o = std::get<model::offset>(q);
        if (_begin > o || o > _end) {
            vlog(
              _view._ctxlog.debug,
              "Offset {} out of [{}-{}] range",
              o,
              _begin,
              _end);
            co_return false;
        }
    }
    auto satisfies_query = ss::visit(
      _current,
      [this](std::monostate) {
          vlog(_view._ctxlog.debug, "Manifest is not initialized");
          return false;
      },
      [this](stale_manifest) {
          vlog(_view._ctxlog.debug, "Manifest is stale");
          return false;
      },
      [this, q](std::reference_wrapper<const partition_manifest> p) {
          vlog(
            _view._ctxlog.debug,
            "Seeking STM manifest [{}-{}]",
            p.get().get_start_offset(),
            p.get().get_last_offset());
          return contains(p, q);
      },
      [this, q](const ss::shared_ptr<materialized_manifest>& m) {
          vlog(
            _view._ctxlog.debug,
            "Seeking spillover manifest [{}-{}]",
            m->manifest.get_start_offset(),
            m->manifest.get_last_offset());
          return contains(m->manifest, q);
      });
    if (satisfies_query) {
        // The seek is to the same manifest so no need to go through the churns
        // of hydrating/materializing/fetching the manifest
        co_return true;
    }
    auto start_offset = std::clamp(
      _view.stm_manifest().get_start_offset().value_or(model::offset{}),
      _begin,
      _end);
    auto res = co_await _view.get_materialized_manifest(q);
    if (res.has_failure()) {
        vlog(
          _view._ctxlog.debug,
          "Failed to seek async_manifest_view_cursor: {}",
          res.error());
        co_return res.as_failure();
    }
    // Check that the manifest fits inside the offset range
    // limit. The check has to be performed after the scheduling
    // point for the list of manifest to be up to date.
    if (unlikely(!manifest_in_range(res.value()))) {
        vlog(
          _view._ctxlog.debug,
          "Manifest is not in the specified range, range: [{}/{}]",
          _begin,
          _end);
        co_return false;
    }
    _current = res.value();
    if (std::holds_alternative<stm_manifest_t>(_current)) {
        // Invariant: if cursor points to the STM manifest _stm_start_offset is
        //            set to expected base offset
        _stm_start_offset = start_offset;
    } else {
        _stm_start_offset = std::nullopt;
    }
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return true;
}

bool async_manifest_view_cursor::manifest_in_range(
  const manifest_section_t& m) {
    return ss::visit(
      m,
      [](std::monostate) { return false; },
      [](stale_manifest) { return false; },
      [this](std::reference_wrapper<const partition_manifest> p) {
          auto so = p.get().get_start_offset().value_or(model::offset{});
          auto lo = p.get().get_last_offset();
          vlog(
            _view._ctxlog.debug,
            "STM manifest range: [{}/{}], cursor range: [{}/{}]",
            so,
            lo,
            _begin,
            _end);
          return !(_end < so || _begin > lo);
      },
      [this](const ss::shared_ptr<materialized_manifest>& m) {
          auto so = m->manifest.get_start_offset().value_or(model::offset{});
          auto lo = m->manifest.get_last_offset();
          vlog(
            _view._ctxlog.debug,
            "Spill manifest range: [{}/{}], cursor range: [{}/{}]",
            so,
            lo,
            _begin,
            _end);
          return !(_end < so || _begin > lo);
      });
}

ss::future<result<async_manifest_view_cursor::eof, error_outcome>>
async_manifest_view_cursor::next() {
    static constexpr auto EOS = model::offset{};
    auto next_base_offset = ss::visit(
      _current,
      [](std::monostate) { return EOS; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) { return EOS; },
      [](const ss::shared_ptr<materialized_manifest>& m) {
          return model::next_offset(m->manifest.get_last_offset());
      });

    if (next_base_offset == EOS || next_base_offset > _end) {
        co_return eof::yes;
    }
    auto manifest = co_await _view.get_materialized_manifest(next_base_offset);
    if (manifest.has_failure()) {
        co_return manifest.as_failure();
    }
    if (unlikely(!manifest_in_range(manifest.value()))) {
        co_return error_outcome::out_of_range;
    }
    _current = manifest.value();
    if (std::holds_alternative<stm_manifest_t>(_current)) {
        // Invariant: if cursor points to the STM manifest _stm_start_offset is
        //            set to expected base offset
        _stm_start_offset = std::clamp(next_base_offset, _begin, _end);
    }
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return eof::no;
}

ss::future<ss::stop_iteration> async_manifest_view_cursor::next_iter() {
    auto res = co_await next();
    if (res.has_failure()) {
        throw std::system_error(res.error());
    }
    co_return res.value() == eof::yes ? ss::stop_iteration::yes
                                      : ss::stop_iteration::no;
}

ssx::task_local_ptr<const partition_manifest>
async_manifest_view_cursor::manifest() const {
    using ret_t = ssx::task_local_ptr<const partition_manifest>;
    return ss::visit(
      _current,
      [](std::monostate) -> ret_t { return {}; },
      [this](stale_manifest) -> ret_t {
          auto errc = make_error_code(error_outcome::timed_out);
          throw std::system_error(
            errc,
            fmt_with_ctx(
              fmt::format,
              "{} manifest was evicted from the cache",
              _view.get_ntp()));
      },
      [](std::reference_wrapper<const partition_manifest> m) -> ret_t {
          return ret_t(&m.get());
      },
      [](const ss::shared_ptr<materialized_manifest>& m) -> ret_t {
          return ret_t(&m->manifest);
      });
}

void async_manifest_view_cursor::on_timeout() {
    auto next = ss::visit(
      _current,
      [](std::monostate) { return model::offset{}; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) {
          return model::offset{};
      },
      [this](const ss::shared_ptr<materialized_manifest>& m) {
          if (m->evicted) {
              vlog(
                _view._ctxlog.debug,
                "Spillover manifest {} is being evicted, last offset: {}",
                m->manifest.get_manifest_filename(),
                m->manifest.get_last_offset());
              return model::next_offset(m->manifest.get_last_offset());
          } else {
              vlog(
                _view._ctxlog.debug,
                "Spillover manifest {} is not evicted, rearming",
                m->manifest.get_manifest_filename());
              return model::offset{};
          }
      });
    if (next != model::offset{}) {
        _current = stale_manifest{.next_offset = next};
    } else {
        _timer.arm(_idle_timeout);
    }
}

async_manifest_view::async_manifest_view(
  ss::sharded<remote>& remote,
  ss::sharded<cache>& cache,
  const partition_manifest& stm_manifest,
  cloud_storage_clients::bucket_name bucket,
  const remote_path_provider& path_provider)
  : _bucket(bucket)
  , _remote_path_provider(path_provider)
  , _remote(remote)
  , _cache(cache)
  , _ts_probe(remote.local().materialized().get_read_path_probe())
  , _stm_manifest(stm_manifest)
  , _rtcnode(_as)
  , _ctxlog(cst_log, _rtcnode, _stm_manifest.get_ntp().path())
  , _timeout(
      config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms.bind())
  , _backoff(config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
  , _read_buffer_size(config::shard_local_cfg().storage_read_buffer_size.bind())
  , _readahead_size(
      config::shard_local_cfg().storage_read_readahead_count.bind())
  , _manifest_meta_ttl(
      config::shard_local_cfg().cloud_storage_manifest_cache_ttl_ms.bind())
  , _manifest_cache(
      _remote.local().materialized().get_materialized_manifest_cache()) {}

ss::future<> async_manifest_view::start() {
    ssx::spawn_with_gate(_gate, [this] { return run_bg_loop(); });
    co_return;
}

ss::future<> async_manifest_view::stop() {
    _as.request_abort();
    _cvar.broken();
    co_await _gate.close();
}

ss::future<> async_manifest_view::run_bg_loop() {
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
                if (in_stm(front.search_vec.base_offset)) {
                    vlog(
                      _ctxlog.warn,
                      "Request {} refers to STM manifest",
                      front.search_vec);
                    // Normally, the request shouldn't contain the STM
                    // request but nothing prevents us from handling this
                    // just in case.
                    front.promise.set_value(std::ref(_stm_manifest));
                    continue;
                }
                if (!_manifest_cache.contains(std::make_tuple(
                      get_ntp(), front.search_vec.base_offset))) {
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
                    auto u = co_await _manifest_cache.prepare(
                      front.search_vec.metadata_size_hint,
                      _ctxlog,
                      _manifest_meta_ttl() * 2);
                    // At this point we have free memory to download the
                    // spillover manifest.
                    auto m_res = co_await materialize_manifest(path);
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
                      _manifest_cache.size(),
                      _manifest_cache.size_bytes());
                    _manifest_cache.put(
                      std::move(u), std::move(m_res.value()), _ctxlog);
                    _ts_probe.set_spillover_manifest_bytes(
                      static_cast<int64_t>(_manifest_cache.size_bytes()));
                    _ts_probe.set_spillover_manifest_instances(
                      static_cast<int32_t>(_manifest_cache.size()));
                    vlog(
                      _ctxlog.debug,
                      "Manifest with LSO {} is cached {{cache size: "
                      "{}/{}}}",
                      lso,
                      _manifest_cache.size(),
                      _manifest_cache.size_bytes());
                } else {
                    vlog(_ctxlog.debug, "Manifest is already materialized");
                }
                auto cached = _manifest_cache.get(
                  std::make_tuple(get_ntp(), front.search_vec.base_offset),
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

ss::future<result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
async_manifest_view::get_cursor(
  async_view_search_query_t query,
  std::optional<model::offset> end_inclusive,
  cursor_base_t cursor_base) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (!in_archive(query) && !in_stm(query)) {
            co_return error_outcome::out_of_range;
        }
        model::offset begin;
        model::offset end = end_inclusive.value_or(
          _stm_manifest.get_last_offset());
        if (_stm_manifest.get_archive_start_offset() == model::offset{}) {
            begin = _stm_manifest.get_start_offset().value_or(begin);
        } else {
            switch (cursor_base) {
            case cursor_base_t::archive_start_offset:
                begin = _stm_manifest.get_archive_start_offset();
                break;
            case cursor_base_t::archive_clean_offset:
                begin = _stm_manifest.get_archive_clean_offset();
                break;
            }
        }

        if (end < begin) {
            vlog(
              _ctxlog.debug,
              "invalid end offset: stm_manifest_begin={} stm_manifest_end={} "
              "end_inclusive_override={}",
              begin,
              _stm_manifest.get_last_offset(),
              end_inclusive);
            co_return error_outcome::out_of_range;
        }
        vlog(
          _ctxlog.debug,
          "creating_cursor: begin: {}, end: {}, stm_range[{}/{}]",
          begin,
          end,
          _stm_manifest.get_start_offset(),
          _stm_manifest.get_last_offset());
        auto cursor = std::make_unique<async_manifest_view_cursor>(
          *this, begin, end, _manifest_meta_ttl());
        // This calls 'get_materialized_manifest' internally which
        // could potentially schedule manifest hydration/materialization
        // in the background fiber.
        int retry_quota = 4;
        while (retry_quota-- > 0) {
            auto result = co_await cursor->seek(query);
            if (result.has_error()) {
                if (result.error() == error_outcome::repeat) {
                    vlog(_ctxlog.debug, "seek to {} need to be retried", query);
                    continue;
                } else if (result.error() == error_outcome::out_of_range) {
                    vlog(
                      _ctxlog.debug,
                      "failed to seek to {}, out-of-range",
                      query);
                } else {
                    vlogl(
                      _ctxlog,
                      log_level_for_error(result.error()),
                      "failed to seek to {}, error: {}",
                      query,
                      result.error());
                }
                co_return result.as_failure();
            }
            if (!result.value()) {
                vlog(
                  _ctxlog.debug,
                  "failed to seek to {}, offset out of valid range",
                  query);
                co_return error_outcome::out_of_range;
            }
            co_return cursor;
        }
        vlog(_ctxlog.debug, "can't seek to {} retry quota exceeded", query);
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to create a cursor: {}",
          std::current_exception());
    }
    co_return error_outcome::failure;
}

ss::future<result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
async_manifest_view::get_retention_backlog() noexcept {
    try {
        ss::gate::holder h(_gate);
        auto cursor = std::make_unique<async_manifest_view_cursor>(
          *this,
          _stm_manifest.get_archive_clean_offset(),
          model::prev_offset(_stm_manifest.get_archive_start_offset()),
          _manifest_meta_ttl());
        // Query the beginning of the backlog. This will fail if for some reason
        // the spillover manifest doesn't exist in the cloud. To avoid this we
        // should never delete spillover manifests above the
        auto q = _stm_manifest.get_archive_clean_offset();
        int retry_quota = 4;
        while (retry_quota-- > 0) {
            auto result = co_await cursor->seek(q);
            if (result.has_error()) {
                if (result.error() == error_outcome::repeat) {
                    vlog(_ctxlog.debug, "seek to {} need to be retried", q);
                    continue;
                }

                vlogl(
                  _ctxlog,
                  log_level_for_error(result.error()),
                  "failed to seek to {} in the retention backlog, "
                  "error: {}",
                  q,
                  result.error());
                co_return result.as_failure();
            }
            co_return cursor;
        }
        vlog(_ctxlog.debug, "can't seek to {} retry quota exceeded", q);
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to create a cursor: {}",
          std::current_exception());
    }
    co_return error_outcome::failure;
}

ss::future<result<std::optional<kafka::offset>, error_outcome>>
async_manifest_view::get_term_last_offset(model::term_id term) noexcept {
    const auto& stmm = stm_manifest();
    vassert(
      stmm.size() > 0,
      "The manifest for {} is not expected to be empty",
      get_ntp());

    if (stmm.begin()->segment_term <= term) {
        // if last segment term is equal to the one we look for return it
        auto last = stmm.last_segment();
        vassert(
          last.has_value(),
          "The manifest for {} is not expected to be empty",
          get_ntp());

        if (last->segment_term == term) {
            // Fast path, most requests should query the last term
            co_return last->next_kafka_offset() - kafka::offset(1);
        } else {
            // look for first segment in next term, segments are sorted by
            // base_offset and term
            for (const auto& p : stmm) {
                if (p.segment_term > term) {
                    co_return p.base_kafka_offset() - kafka::offset(1);
                }
            }
        }
    } else if (stmm.get_archive_start_offset() != model::offset{}) {
        const auto spill_index = get_spillover_upper_bound_by_term(term);
        if (!spill_index.has_value()) {
            co_return std::nullopt;
        }

        vlog(
          _ctxlog.debug,
          "Picked spill manifest at index {} for last offest in term {}",
          *spill_index,
          term());

        auto spill = stm_manifest().get_spillover_map().at_index(*spill_index);
        if (spill.is_end()) {
            vlog(
              _ctxlog.error,
              "Failed to find spillover manifest at index: {}",
              *spill_index);
            co_return error_outcome::failure;
        }

        auto cursor_start = spill->base_offset;
        auto archive_start = stm_manifest().get_archive_start_offset();
        if (cursor_start < archive_start) {
            // The start offset of the selected spill manifest
            // may be below the start offset of the archive.
            // If that's the case, point the cursor to the start of the archive
            // if that lies within the selected spill manifest.
            if (archive_start <= spill->committed_offset) {
                cursor_start = archive_start;
            } else {
                co_return std::nullopt;
            }
        }

        auto res = co_await get_cursor(cursor_start);
        if (res.has_error()) {
            vlogl(
              _ctxlog,
              log_level_for_error(res.error()),
              "Failed to scan metadata: {}",
              res.error());
            co_return res.as_failure();
        }

        try {
            std::optional<kafka::offset> res_offset;
            co_await ss::repeat(
              [this, &res_offset, term, cursor = std::move(res.value())] {
                  const auto& manifest = *cursor->manifest();
                  vlog(
                    _ctxlog.debug,
                    "Scanning manifest {} for term {}",
                    manifest.get_manifest_path(path_provider()),
                    term);
                  for (auto meta : manifest) {
                      if (meta.segment_term > term) {
                          res_offset = meta.base_kafka_offset()
                                       - kafka::offset(1);
                          vlog(
                            _ctxlog.debug,
                            "Scan found offset {} at term {}",
                            res_offset.value(),
                            meta.segment_term);
                          return ss::make_ready_future<ss::stop_iteration>(
                            ss::stop_iteration::yes);
                      }
                  }
                  return cursor->next_iter();
              });

            co_return res_offset;
        } catch (const std::system_error& e) {
            // thrown by `async_manifest_view::next_iter`
            if (e.code().category() != error_category()) {
                vlog(
                  _ctxlog.error,
                  "Exception with unexpected error category caught: {}",
                  e);

                co_return error_outcome::failure;
            }

            const auto err_outcome = static_cast<error_outcome>(
              e.code().value());

            vlogl(
              _ctxlog,
              log_level_for_error(err_outcome),
              "Failed to get last offset from term {}: {}",
              term,
              err_outcome);

            co_return err_outcome;
        } catch (...) {
            vlog(
              _ctxlog.error,
              "Failed to get last offest for term {}: {}",
              term,
              std::current_exception());
            co_return error_outcome::failure;
        }
    }
    co_return std::nullopt;
}

bool async_manifest_view::is_empty() const noexcept {
    return _stm_manifest.size() == 0;
}

bool async_manifest_view::in_archive(async_view_search_query_t o) {
    if (_stm_manifest.get_archive_start_offset() == model::offset{}) {
        return false;
    }
    return ss::visit(
      o,
      [this](model::offset ro) {
          return ro >= _stm_manifest.get_archive_clean_offset()
                 && ro < _stm_manifest.get_start_offset().value_or(
                      model::offset::min());
      },
      [this](kafka::offset ko) {
          return ko >= _stm_manifest.get_archive_start_kafka_offset()
                 && ko < _stm_manifest.get_start_kafka_offset().value_or(
                      kafka::offset::min());
      },
      [this](async_view_timestamp_query ts_query) {
          // For a query to be satisfiable by the archive the min offset must be
          // in the archive. The same condition can be stated as: min offset
          // must be before the start of the STM manifest.
          //
          // Otherwise, even though the last timestamp in the archive could
          // satisfy the query, it can't be used because offset-wise it is
          // outside of the queried range.
          kafka::offset archive_end_offset = kafka::prev_offset(
            _stm_manifest.get_start_kafka_offset().value_or(
              kafka::offset::min()));

          bool range_overlaps
            = ts_query.min_offset <= archive_end_offset
              && ts_query.max_offset
                   >= _stm_manifest.get_archive_start_kafka_offset();

          // The condition for timequery is tricky. With offsets there is a
          // clear pivot point. The start_offset of the STM manifest separates
          // the STM region from the archive. With timestamps it's not as
          // simple.There could be a gap between the last segment in the archive
          // and the first segment in the STM manifest. We need in_stm and
          // in_archive to be consistent with each other. To do this we can use
          // last timestamp in the archive as a pivot point.
          return range_overlaps
                 && _stm_manifest.get_spillover_map()
                        .last_segment()
                        ->max_timestamp
                      >= ts_query.ts;
      });
}

bool async_manifest_view::in_stm(async_view_search_query_t o) {
    return ss::visit(
      o,
      [this](model::offset ro) {
          auto so = _stm_manifest.get_start_offset().value_or(
            model::offset::max());
          return ro >= so;
      },
      [this](kafka::offset ko) {
          auto sko = _stm_manifest.get_start_kafka_offset().value_or(
            kafka::offset::max());
          return ko >= sko;
      },
      [this](async_view_timestamp_query ts_query) {
          vlog(
            _ctxlog.debug, "Checking timestamp {} using timequery", ts_query);
          if (_stm_manifest.get_spillover_map().empty()) {
              // The spillover manifest is empty, so the timestamp query has to
              // be directed to the STM manifest. Otherwise, we can safely
              // direct the query either to spillover or stm because the
              // STM manifest with spillover segments is never empty.
              return true;
          }

          bool range_overlaps
            = ts_query.min_offset
                <= _stm_manifest.get_last_kafka_offset().value_or(
                  kafka::offset::min())
              && ts_query.max_offset
                   >= _stm_manifest.get_start_kafka_offset().value_or(
                     kafka::offset::max());

          // The last timestamp in the archive is used as a pivot point. See
          // description in in_archive.
          return range_overlaps
                 && _stm_manifest.get_spillover_map()
                        .last_segment()
                        ->max_timestamp
                      < ts_query.ts;
      });
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::compute_retention(
  std::optional<size_t> size_limit,
  std::optional<std::chrono::milliseconds> time_limit) noexcept {
    archive_start_offset_advance time_result;
    archive_start_offset_advance size_result;
    if (time_limit.has_value()) {
        auto res = co_await time_based_retention(time_limit.value());
        if (res.has_value()) {
            time_result = res.value();
        } else {
            vlogl(
              _ctxlog,
              log_level_for_error(res.error()),
              "Failed to compute time-based retention: {}",
              res.error());

            co_return res;
        }
    }
    if (size_limit.has_value()) {
        auto res = co_await size_based_retention(size_limit.value());
        if (res.has_value()) {
            size_result = res.value();
        } else {
            vlogl(
              _ctxlog,
              log_level_for_error(res.error()),
              "Failed to compute size-based retention: {}",
              res.error());

            co_return res;
        }
    }
    archive_start_offset_advance result;
    if (size_result.offset > time_result.offset) {
        result = size_result;
    } else {
        result = time_result;
    }
    if (
      _stm_manifest.get_start_kafka_offset_override() != kafka::offset{}
      && _stm_manifest.get_start_kafka_offset_override()
           > result.offset - result.delta) {
        // The start kafka offset is placed above the retention boundary. We
        // need to adjust retention boundary to remove all data up to start
        // kafka offset.
        vlog(
          _ctxlog.debug,
          "Start kafka offset override {} exceeds computed retention {}",
          _stm_manifest.get_start_kafka_offset_override(),
          result.offset);
        auto r = co_await offset_based_retention();
        if (r.has_error()) {
            co_return r;
        }
        result = r.value();
        vlog(
          _ctxlog.debug,
          "Found offset {} to advance start offset to",
          result.offset);
    }
    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::offset_based_retention() noexcept {
    archive_start_offset_advance result;
    try {
        auto boundary = _stm_manifest.get_start_kafka_offset_override();
        auto res = co_await get_cursor(
          boundary, std::nullopt, cursor_base_t::archive_clean_offset);
        if (res.has_failure()) {
            if (res.error() == error_outcome::out_of_range) {
                vlog(
                  _ctxlog.debug,
                  "There is no segment old enough to be removed by retention");
                co_return result;
            } else {
                vlogl(
                  _ctxlog,
                  log_level_for_error(res.error()),
                  "Failed to compute offset-based retention {}",
                  res.error());
                co_return res.as_failure();
            }
        }

        const auto& manifest = *res.value()->manifest();
        vassert(
          !manifest.empty(), "{} Spillover manifest can't be empty", get_ntp());
        result.offset = manifest.begin()->base_offset;
        result.delta = manifest.begin()->delta_offset;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::time_based_retention(
  std::chrono::milliseconds time_limit) noexcept {
    archive_start_offset_advance result;

    try {
        const auto now = model::timestamp_clock::now();
        const auto delta
          = std::chrono::duration_cast<model::timestamp_clock::duration>(
            time_limit);
        const auto boundary = model::to_timestamp(now - delta);
        vlog(
          _ctxlog.debug,
          "Computing time-based retention, boundary: {}, now: {}",
          now - delta,
          now);

        if (!_stm_manifest.get_start_offset().has_value()) {
            vlog(
              _ctxlog.error,
              "Empty STM manifest with archive in place: "
              "stm_start_offset={}, archive_start_offset={}, "
              "archive_clean_offset={}",
              _stm_manifest.get_start_offset(),
              _stm_manifest.get_archive_start_offset(),
              _stm_manifest.get_archive_clean_offset());
            co_return error_outcome::failure;
        }

        auto res = co_await get_cursor(
          _stm_manifest.get_archive_start_offset(),
          model::prev_offset(_stm_manifest.get_start_offset().value()),
          cursor_base_t::archive_clean_offset);
        if (res.has_failure()) {
            if (res.error() == error_outcome::out_of_range) {
                // The cutoff point is outside of the offset range, no need to
                // do anything
                vlog(
                  _ctxlog.debug,
                  "There is no segment old enough to be removed by retention");
                co_return result;
            } else {
                vlogl(
                  _ctxlog,
                  log_level_for_error(res.error()),
                  "Failed to compute time-based retention {}",
                  res.error());
                co_return res.as_failure();
            }
        }
        auto cursor = std::move(res.value());
        while (cursor->get_status()
               == async_manifest_view_cursor_status::materialized_spillover) {
            auto eof = co_await cursor->with_manifest(
              [boundary, &result](const partition_manifest& manifest) {
                  for (const auto& meta : manifest) {
                      if (meta.max_timestamp > boundary) {
                          return true;
                      }
                      result.offset = model::next_offset(meta.committed_offset);
                      result.delta = meta.delta_offset;
                  }
                  return false;
              });
            vlog(
              _ctxlog.debug,
              "Updated last offset to {}, delta {}",
              result.offset,
              result.delta);

            if (!eof) {
                auto r = co_await cursor->next();
                if (
                  r.has_value()
                  && r.value() == async_manifest_view_cursor::eof::yes) {
                    vlog(
                      _ctxlog.info,
                      "Entire archive is removed by the time-based "
                      "retention");
                    break;
                } else if (r.has_failure()) {
                    vlogl(
                      _ctxlog,
                      log_level_for_error(r.error()),
                      "Failed to scan manifest while computing retention "
                      "{}",
                      r.error());
                    co_return r.as_failure();
                }
            } else {
                vlog(
                  _ctxlog.debug,
                  "Retention found offset {} with delta {}",
                  result.offset,
                  result.delta);
                break;
            }
        }
        if (result.offset == model::offset{}) {
            vlog(
              _ctxlog.debug,
              "Failed to find the retention boundary, the manifest {} "
              "doesn't "
              "have any matching segment",
              cursor->manifest()->get_manifest_path(path_provider()));
        }
    } catch (const std::system_error& err) {
        // Thrown by `async_manifest_view::maybe_sync_manifest`
        const auto shutdown_errc = make_error_code(
          error_outcome::shutting_down);
        if (err.code() == shutdown_errc) {
            co_return error_outcome::shutting_down;
        }

        vlog(_ctxlog.error, "Failed to compute retention err: {}", err.code());
        co_return error_outcome::failure;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }

    co_return result;
}

ss::future<
  result<async_manifest_view::archive_start_offset_advance, error_outcome>>
async_manifest_view::size_based_retention(size_t size_limit) noexcept {
    archive_start_offset_advance result;
    try {
        const auto cloud_log_size = _stm_manifest.cloud_log_size();
        const auto clean_offset = _stm_manifest.get_archive_clean_offset();
        if (cloud_log_size > size_limit) {
            auto to_remove = cloud_log_size - size_limit;
            vlog(
              _ctxlog.debug,
              "Computing size-based retention, log size: {}, limit: {}, {} "
              "bytes will be removed",
              cloud_log_size,
              size_limit,
              to_remove);

            if (!_stm_manifest.get_start_offset().has_value()) {
                vlog(
                  _ctxlog.error,
                  "Empty STM manifest with archive in place: "
                  "stm_start_offset={}, archive_start_offset={}, "
                  "archive_clean_offset={}",
                  _stm_manifest.get_start_offset(),
                  _stm_manifest.get_archive_start_offset(),
                  _stm_manifest.get_archive_clean_offset());
                co_return error_outcome::failure;
            }

            auto res = co_await get_cursor(
              _stm_manifest.get_archive_clean_offset(),
              model::prev_offset(_stm_manifest.get_start_offset().value()),
              cursor_base_t::archive_clean_offset);
            if (res.has_failure()) {
                vlogl(
                  _ctxlog,
                  log_level_for_error(res.error()),
                  "Failed to compute size-based retention {}",
                  res.error());
                co_return res.as_failure();
            }

            auto cursor = std::move(res.value());
            while (to_remove != 0
                   && cursor->get_status()
                        == async_manifest_view_cursor_status::
                          materialized_spillover) {
                // We are reading from the spillover manifests until
                // the 'to_remove' value is zero. Every time we read
                // we're advancing the last_* values. The scan shouldn't
                // go to the STM manifest and should only include archive.
                // The end condition is the lambda returned true, otherwise
                // we should keep scanning.
                auto eof = co_await cursor->with_manifest(
                  [this, &to_remove, &result, clean_offset](
                    const partition_manifest& manifest) mutable {
                      for (const auto& meta : manifest) {
                          // Skip segments below the clean offset as they're
                          // already eligible for GC. The reason why we are
                          // using the clean offset and not the start offset
                          // here is that the archive size (used above in
                          // `partition_manifest::cloud_log_size` is updated
                          // with the clean offset.
                          if (meta.base_offset < clean_offset) {
                              vlog(
                                _ctxlog.debug,
                                "Retention skip {}, as it's below the clean "
                                "offset {}",
                                meta,
                                clean_offset);
                              continue;
                          }

                          result.offset = meta.base_offset;
                          result.delta = meta.delta_offset;

                          if (meta.size_bytes > to_remove) {
                              vlog(_ctxlog.debug, "Retention stop at {}", meta);
                              to_remove = 0;
                              return true;
                          } else {
                              to_remove -= meta.size_bytes;
                              vlog(
                                _ctxlog.debug,
                                "Retention consume {}, remaining bytes: {}",
                                meta,
                                to_remove);
                          }
                      }
                      return false;
                  });
                vlog(
                  _ctxlog.debug,
                  "Updated last offset to {}, delta {}",
                  result.offset,
                  result.delta);
                if (!eof) {
                    auto r = co_await cursor->next();
                    if (
                      r.has_value()
                      && r.value() == async_manifest_view_cursor::eof::yes) {
                        // If the retention policy requires us to remove
                        // segments from the STM manifest, or if the entire
                        // archive was removed, the archive start offset should
                        // be advanced to match that of the STM region.
                        if (!_stm_manifest.empty()) {
                            // The STM manifest should never be empty here since
                            // we have an archive in place.
                            result.offset = _stm_manifest.begin()->base_offset;
                            result.delta = _stm_manifest.begin()->delta_offset;
                        } else {
                            vlog(
                              _ctxlog.error,
                              "Empty STM manifest with archive in place: "
                              "stm_start_offset={}, archive_start_offset={}, "
                              "archive_clean_offset={}",
                              _stm_manifest.get_start_offset(),
                              _stm_manifest.get_archive_start_offset(),
                              _stm_manifest.get_archive_clean_offset());
                        }
                        vlog(
                          _ctxlog.info,
                          "Entire archive is removed by the size-based "
                          "retention");
                        break;
                    } else if (r.has_failure()) {
                        vlogl(
                          _ctxlog,
                          log_level_for_error(r.error()),
                          "Failed to scan manifest while computing "
                          "retention "
                          "{}",
                          r.error());
                        co_return r.as_failure();
                    }
                } else {
                    vlog(
                      _ctxlog.debug,
                      "Retention found offset {} with delta {}",
                      result.offset,
                      result.delta);
                    break;
                }
            }
        } else {
            vlog(
              _ctxlog.debug,
              "Log size ({}) is withing the limit ({})",
              cloud_log_size,
              size_limit);
        }
    } catch (const std::system_error& err) {
        // Thrown by `async_manifest_view::maybe_sync_manifest`
        const auto shutdown_errc = make_error_code(
          error_outcome::shutting_down);
        if (err.code() == shutdown_errc) {
            co_return error_outcome::shutting_down;
        }

        vlog(_ctxlog.error, "Failed to compute retention err: {}", err.code());
        co_return error_outcome::failure;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to compute retention {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return result;
}

ss::future<result<manifest_section_t, error_outcome>>
async_manifest_view::get_materialized_manifest(
  async_view_search_query_t q) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (in_stm(q)) {
            vlog(_ctxlog.debug, "Query {} matches with STM manifest", q);
            // Fast path for STM reads
            co_return std::ref(_stm_manifest);
        }
        // query in not in the stm region
        if (
          std::holds_alternative<async_view_timestamp_query>(q)
          && _stm_manifest.get_archive_start_offset() == model::offset{}) {
            vlog(_ctxlog.debug, "Using STM manifest for timequery {}", q);
            co_return std::ref(_stm_manifest);
        }
        auto meta = search_spillover_manifests(q);
        if (!meta.has_value()) {
            vlog(_ctxlog.debug, "Can't find requested manifest, {}", q);
            co_return error_outcome::out_of_range;
        }
        vlog(_ctxlog.debug, "Found spillover manifest meta: {}", meta);
        auto res = _manifest_cache.get(
          std::make_tuple(get_ntp(), meta->base_offset), _ctxlog);
        if (res) {
            co_return res;
        }
        // Send materialization request to background loop
        materialization_request_t request{
          .search_vec = *meta,
          ._measurement = _ts_probe.spillover_manifest_latency(),
        };
        auto fut = request.promise.get_future();
        _requests.emplace_back(std::move(request));
        _cvar.signal();
        auto m = co_await std::move(fut);
        if (m.has_failure()) {
            vlogl(
              _ctxlog,
              log_level_for_error(m.error()),
              "Failed to materialize spillover manifest: {}",
              m.error());
            co_return m.as_failure();
        }
        co_return m.value();
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::hydrate_manifest(
  remote_manifest_path path) const noexcept {
    try {
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        // Spillover manifests are always serde-encoded
        auto fk = std::make_pair(manifest_format::serde, path);
        auto res = co_await _remote.local().download_manifest(
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
        auto reservation = co_await _cache.local().reserve_space(len, 1);
        co_await _cache.local().put(
          manifest.get_manifest_path(path_provider())(),
          str,
          reservation,
          priority_manager::local().shadow_indexing_priority());
        _ts_probe.on_spillover_manifest_hydration();
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

std::optional<segment_meta> async_manifest_view::search_spillover_manifests(
  async_view_search_query_t query) const {
    const auto& manifests = _stm_manifest.get_spillover_map();
    auto ix = ss::visit(
      query,
      [&](model::offset o) {
          vlog(
            _ctxlog.debug,
            "search_spillover_manifest query: {}, num manifests: {}, first: "
            "{}, last: {}",
            query,
            manifests.size(),
            manifests.empty() ? model::offset{}
                              : manifests.begin()->base_offset,
            manifests.empty() ? model::offset{}
                              : manifests.last_segment()->committed_offset);
          const auto& bo_col = manifests.get_base_offset_column();
          const auto& co_col = manifests.get_committed_offset_column();
          auto co_it = co_col.lower_bound(o);
          if (co_it.is_end()) {
              return -1;
          }
          auto bo_it = bo_col.at_index(co_it.index());
          while (!bo_it.is_end()) {
              if (o >= *bo_it && o <= *co_it) {
                  return static_cast<int>(bo_it.index());
              }
              ++bo_it;
              ++co_it;
          }
          return -1;
      },
      [&](kafka::offset k) {
          vlog(
            _ctxlog.debug,
            "search_spillover_manifest query: {}, num manifests: {}, first: "
            "{}, last: {}",
            query,
            manifests.size(),
            manifests.empty() ? kafka::offset{}
                              : manifests.begin()->base_kafka_offset(),
            manifests.empty() ? kafka::offset{}
                              : manifests.last_segment()->next_kafka_offset());
          const auto& bo_col = manifests.get_base_offset_column();
          const auto& co_col = manifests.get_committed_offset_column();
          const auto& do_col = manifests.get_delta_offset_column();
          const auto& de_col = manifests.get_delta_offset_end_column();
          auto bo_it = bo_col.begin();
          auto co_it = co_col.begin();
          auto do_it = do_col.begin();
          auto de_it = de_col.begin();
          while (!bo_it.is_end()) {
              static constexpr int64_t min_delta = model::offset::min()();
              auto d_begin = *do_it == min_delta ? 0 : *do_it;
              auto d_end = *de_it == min_delta ? d_begin : *de_it;
              auto bko = kafka::offset(*bo_it - d_begin);
              auto nko = kafka::offset(*co_it - d_end);
              if (k >= bko && k <= nko) {
                  return static_cast<int>(bo_it.index());
              }
              ++bo_it;
              ++co_it;
              ++do_it;
              ++de_it;
          }
          return -1;
      },
      [&](const async_view_timestamp_query& ts_query) {
          if (manifests.empty()) {
              return -1;
          }
          vlog(
            _ctxlog.debug,
            "search_spillover_manifest query: {}, num manifests: {}, first: "
            "{}, last: {}",
            query,
            manifests.size(),
            *manifests.begin(),
            *manifests.last_segment());

          auto max_t = manifests.last_segment()->max_timestamp;

          // Edge cases
          if (ts_query.ts > max_t) {
              return -1;
          }

          const auto& bo_col = manifests.get_base_offset_column();
          const auto& co_col = manifests.get_committed_offset_column();
          const auto& do_col = manifests.get_delta_offset_column();
          const auto& de_col = manifests.get_delta_offset_end_column();
          const auto& bt_col = manifests.get_base_timestamp_column();
          const auto& mt_col = manifests.get_max_timestamp_column();

          auto bo_it = bo_col.begin();
          auto co_it = co_col.begin();
          auto do_it = do_col.begin();
          auto de_it = de_col.begin();
          auto max_ts_it = mt_col.begin();
          auto base_ts_it = bt_col.begin();

          int target_ix = -1;
          while (!base_ts_it.is_end()) {
              static constexpr int64_t min_delta = model::offset::min()();
              auto d_begin = *do_it == min_delta ? 0 : *do_it;
              auto d_end = *de_it == min_delta ? d_begin : *de_it;
              auto bko = kafka::offset(*bo_it - d_begin);
              auto cko = kafka::offset(*co_it - d_end);

              auto range_overlaps = ts_query.min_offset <= cko
                                    && ts_query.max_offset >= bko;

              if (
                range_overlaps
                && (*max_ts_it >= ts_query.ts() || *base_ts_it > ts_query.ts())) {
                  // Handle case when we're overshooting the target
                  // (base_timestamp > t) or the case when the target is in the
                  // middle of the manifest (max_timestamp >= t)
                  target_ix = static_cast<int>(base_ts_it.index());
                  break;
              }
              ++bo_it;
              ++co_it;
              ++do_it;
              ++de_it;
              ++base_ts_it;
              ++max_ts_it;
          }
          return target_ix;
      });

    if (ix < 0) {
        return std::nullopt;
    }
    auto res = manifests.at_index(ix);
    if (res.is_end()) {
        return std::nullopt;
    }
    return *res;
}

remote_manifest_path async_manifest_view::get_spillover_manifest_path(
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
      path_provider().spillover_manifest_path(_stm_manifest, comp)};
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::materialize_manifest(
  remote_manifest_path path) const noexcept {
    try {
        auto h = _gate.hold();
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        // Perform simple scan of the manifest list
        // Probe cache. If not available or in case of race with cache eviction
        // hydrate manifest from the cloud.
        auto cache_status = co_await _cache.local().is_cached(path());
        switch (cache_status) {
        case cache_element_status::in_progress:
            vlog(_ctxlog.warn, "Concurrent manifest hydration, path {}", path);
            co_return error_outcome::repeat;
        case cache_element_status::not_available: {
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
        case cache_element_status::available: {
            auto res = co_await _cache.local().get(path());
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
                  .io_priority_class
                  = priority_manager::local().shadow_indexing_priority()};
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
        _ts_probe.on_spillover_manifest_materialization();
        co_return manifest;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

std::optional<size_t> async_manifest_view::get_spillover_upper_bound_by_term(
  model::term_id term) noexcept {
    // Use column-store that contains list of spillover manifests to
    // find a starting point for the search.
    const auto& spillover_map = stm_manifest().get_spillover_map();
    // This column contains the term of the last segment in the spill manifest
    const auto& last_term_col = spillover_map.get_segment_term_column();
    size_t sp_index = 0;
    for (auto last_term : last_term_col) {
        if (last_term > term()) {
            break;
        }
        sp_index++;
    }

    if (sp_index == last_term_col.size()) {
        return std::nullopt;
    }

    return sp_index;
}

} // namespace cloud_storage
