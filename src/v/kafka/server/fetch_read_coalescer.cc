/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 */
#include "kafka/server/fetch_read_coalescer.h"

#include "base/vassert.h"
#include "config/configuration.h"
#include "hashing/combine.h"
#include "kafka/server/handlers/fetch.h"
#include "metrics/prometheus_sanitize.h"

namespace kafka {

namespace {

// Serve a retained result only while the visible end has not advanced past the
// point it was read at; otherwise a re-read must pick up the new data.
bool is_fresh(
  const read_result& r,
  model::isolation_level level,
  model::offset current_bound) {
    auto bound = level == model::isolation_level::read_committed
                   ? r.last_stable_offset
                   : r.high_watermark;
    return current_bound <= bound;
}

} // namespace

size_t coalesce_key_hash::operator()(const coalesce_key& k) const {
    size_t h = std::hash<model::ktp_with_hash>{}(k.ktp);
    hash::combine(
      h,
      k.fetch_offset(),
      static_cast<int8_t>(k.level),
      k.max_bytes,
      k.obligatory);
    return h;
}

fetch_read_coalescer::fetch_read_coalescer(
  cache_config cfg, config::binding<bool> enabled)
  : _cache_config(cfg)
  , _enabled(std::move(enabled)) {
    if (_enabled()) {
        _cache.emplace(_cache_config);
    }
    _enabled.watch([this] { on_enabled_change(); });
    setup_metrics();
}

void fetch_read_coalescer::on_enabled_change() {
    if (_enabled() && !_cache) {
        _cache.emplace(_cache_config);
    } else if (!_enabled() && _cache) {
        _cache.reset();
    }
}

ss::future<shared_read> fetch_read_coalescer::get_or_insert(
  const coalesce_key& key, model::offset current_bound, read_fn fn) {
    vassert(_cache.has_value(), "coalescer used while disabled");
    ss::shared_ptr<coalesce_entry> entry;
    if (auto existing = _cache->get_value(key)) {
        entry = *existing;
        if (entry->inflight) {
            ++_inflight_hits;
            return entry->inflight->get_shared_future();
        }
        if (
          auto r = entry->ready.lock();
          r && is_fresh(*r, key.level, current_bound)) {
            ++_ready_hits;
            return ss::make_ready_future<shared_read>(std::move(r));
        }
        ++_reinsertions;
    } else {
        entry = ss::make_shared<coalesce_entry>();
        _cache->try_insert(key, entry);
        ++_insertions;
    }
    return read_and_publish(std::move(entry), std::move(fn));
}

void fetch_read_coalescer::coalesce_entry::begin_read() {
    inflight.emplace();
    ready.reset();
}

void fetch_read_coalescer::coalesce_entry::finish_read(shared_read result) {
    // Retain only data-bearing reads. An empty read drains instantly, so a
    // retained empty would expire before reuse; concurrent empty reads still
    // coalesce via the in-flight promise.
    if (result->error == error_code::none && result->has_data()) {
        ready = result;
    }
    inflight->set_value(std::move(result));
    inflight.reset();
}

void fetch_read_coalescer::coalesce_entry::fail_read(std::exception_ptr e) {
    inflight->set_exception(std::move(e));
    inflight.reset();
}

ss::future<shared_read> fetch_read_coalescer::read_and_publish(
  ss::shared_ptr<coalesce_entry> entry, read_fn fn) {
    entry->begin_read();
    try {
        auto shared = std::make_shared<const read_result>(co_await fn());
        entry->finish_read(shared);
        co_return std::move(shared);
    } catch (...) {
        entry->fail_read(std::current_exception());
        throw;
    }
}

void fetch_read_coalescer::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:fetch_read_coalescer"),
      {
        sm::make_counter(
          "insertions_total",
          [this] { return _insertions; },
          sm::description("Fetch reads that inserted a new cache entry.")),
        sm::make_counter(
          "reinsertions_total",
          [this] { return _reinsertions; },
          sm::description(
            "Fetch reads that re-read an existing (stale or expired) entry.")),
        sm::make_counter(
          "ready_hits_total",
          [this] { return _ready_hits; },
          sm::description("Fetch reads served from a retained result.")),
        sm::make_counter(
          "inflight_hits_total",
          [this] { return _inflight_hits; },
          sm::description("Fetch reads served by awaiting an in-flight read.")),
      },
      {},
      {sm::shard_label});
}

} // namespace kafka
