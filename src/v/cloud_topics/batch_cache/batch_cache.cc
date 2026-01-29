/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/batch_cache.h"

#include "config/configuration.h"
#include "model/record.h"
#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"

#include <seastar/core/preempt.hh>
#include <seastar/core/shard_id.hh>

#include <chrono>

namespace cloud_topics {

batch_cache::batch_cache(
  storage::log_manager* log_manager, std::chrono::milliseconds gc_interval)
  : _gc_interval(gc_interval)
  , _lm(log_manager)
  , _probe(config::shard_local_cfg().disable_metrics()) {}

batch_cache::batch_cache(
  ss::sharded<storage::api>& log_manager, std::chrono::milliseconds gc_interval)
  : batch_cache(&log_manager.local().log_mgr(), gc_interval) {}

ss::future<> batch_cache::start() {
    // Setup materialized index cleanup timer
    _cleanup_timer.set_callback([this] {
        auto gh = _gate.hold();
        ssx::spawn_with_gate(_gate, [this] { return cleanup_index_entries(); });
    });
    _cleanup_timer.arm(_gc_interval);

    return ss::now();
}

ss::future<> batch_cache::stop() {
    _cleanup_timer.cancel();
    co_await _gate.close();
}

void batch_cache::put(
  const model::topic_id_partition& tidp, const model::record_batch& b) {
    vassert(
      b.term() > model::term_id{-1},
      "Batch without term in the cache: {}",
      b.header());
    if (_lm == nullptr) {
        return;
    }
    _gate.check();
    auto it = _index.find(tidp);
    if (it == _index.end()) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        auto [new_it, ok] = _index.insert(
          std::make_pair(
            tidp,
            std::make_unique<storage::batch_cache_index>(
              std::move(*cache_ix))));
        if (ok) {
            it = new_it;
        } else {
            return;
        }
    }
    it->second->put(b, storage::batch_cache::is_dirty_entry::no);
    _probe.register_materialized_put(b.size_bytes());
}

std::optional<model::record_batch>
batch_cache::get(const model::topic_id_partition& tidp, model::offset o) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    _gate.check();
    if (auto it = _index.find(tidp); it != _index.end()) {
        auto rb = it->second->get(o);
        if (rb.has_value()) {
            vassert(
              rb->term() > model::term_id{-1},
              "Batch without term in the cache: {}",
              rb->header());
            vassert(
              rb->base_offset() <= o && o <= rb->last_offset(),
              "Unexpected batch for {}, got range: [{},{}] for offset {}",
              tidp,
              rb->base_offset(),
              rb->last_offset(),
              o);
            _probe.register_materialized_get(rb->size_bytes());
        } else {
            _probe.register_materialized_miss();
        }
        return rb;
    }
    _probe.register_materialized_miss();
    return std::nullopt;
}

ss::future<> batch_cache::cleanup_index_entries() {
    // NOTE: the memory is reclaimed asynchronously.  In some cases
    // the index may no longer reference any live entries.  If this
    // is the case we need to delete the batch_cache_index from the
    // '_index'  collection to avoid accumulating orphaned entries.

    // Clean up materialized batch index
    auto it = _index.begin();
    while (it != _index.end()) {
        if (it->second->empty()) {
            it = _index.erase(it);
        } else {
            ++it;
        }
        if (ss::need_preempt() && it != _index.end()) {
            model::topic_id_partition next = it->first;
            co_await ss::yield();
            it = _index.lower_bound(next);
        }
    }

    // Clean up empty per-partition hydrated indices
    auto hydrated_it = _partition_hydrated.begin();
    while (hydrated_it != _partition_hydrated.end()) {
        bool should_remove = false;
        if (
          hydrated_it->second.translation_map
          && hydrated_it->second.translation_map->extent_count() == 0
          && hydrated_it->second.translation_map->epoch_count() == 0) {
            should_remove = true;
        }
        if (should_remove) {
            hydrated_it = _partition_hydrated.erase(hydrated_it);
        } else {
            ++hydrated_it;
        }
        if (ss::need_preempt() && hydrated_it != _partition_hydrated.end()) {
            model::topic_id_partition next = hydrated_it->first;
            co_await ss::yield();
            hydrated_it = _partition_hydrated.lower_bound(next);
        }
    }

    _cleanup_timer.arm(_gc_interval);
}

// partition_hydrated_cache implementation

partition_hydrated_cache::partition_hydrated_cache(batch_cache& cache)
  : _cache(cache) {}

bool partition_hydrated_cache::is_cached(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  byte_range_size_t size) const {
    return _cache.is_cached_hydrated_internal(tidp, id, byte_offset, size);
}

std::optional<iobuf> partition_hydrated_cache::get(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  byte_range_size_t size) {
    return _cache.get_hydrated_internal(tidp, id, byte_offset, size);
}

void partition_hydrated_cache::put(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  iobuf payload) {
    _cache.put_hydrated_internal(tidp, id, byte_offset, std::move(payload));
}

void partition_hydrated_cache::truncate_hydrated(
  const model::topic_id_partition& tidp, cluster_epoch invalidated_epoch) {
    _cache.truncate_hydrated_internal(tidp, invalidated_epoch);
}

// batch_cache per-partition hydrated cache methods

partition_hydrated_cache_api* batch_cache::get_partition_hydrated_cache() {
    if (!_partition_hydrated_cache) {
        _partition_hydrated_cache = std::make_unique<partition_hydrated_cache>(
          *this);
    }
    return _partition_hydrated_cache.get();
}

partition_hydrated_state& batch_cache::ensure_partition_hydrated_state(
  const model::topic_id_partition& tidp) {
    auto it = _partition_hydrated.find(tidp);
    if (it != _partition_hydrated.end()) {
        return it->second;
    }

    partition_hydrated_state state;

    if (_lm != nullptr) {
        // Create a factory function that creates batch_cache_index instances
        batch_cache_index_factory factory = [this]() {
            auto cache_ix = _lm->create_cache(storage::with_cache::yes);
            if (cache_ix.has_value()) {
                return std::make_unique<storage::batch_cache_index>(
                  std::move(*cache_ix));
            }
            return storage::batch_cache_index_ptr{};
        };
        state.translation_map = std::make_unique<partition_hydrated_index>(
          std::move(factory));
    }

    auto [new_it, _] = _partition_hydrated.emplace(tidp, std::move(state));
    return new_it->second;
}

bool batch_cache::is_cached_hydrated_internal(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  byte_range_size_t size) const {
    auto it = _partition_hydrated.find(tidp);
    if (it == _partition_hydrated.end() || !it->second.translation_map) {
        return false;
    }
    return it->second.translation_map->has_extent(id, byte_offset, size());
}

std::optional<iobuf> batch_cache::get_hydrated_internal(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  byte_range_size_t size) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    _gate.check();

    auto it = _partition_hydrated.find(tidp);
    if (it == _partition_hydrated.end() || !it->second.translation_map) {
        _probe.register_hydrated_miss();
        return std::nullopt;
    }

    auto& state = it->second;

    // Use get_extent which handles subset queries and data retrieval
    auto data = state.translation_map->get_extent(id, byte_offset, size());
    if (!data.has_value()) {
        _probe.register_hydrated_miss();
        return std::nullopt;
    }

    _probe.register_hydrated_get(data->size_bytes());
    return data;
}

void batch_cache::put_hydrated_internal(
  const model::topic_id_partition& tidp,
  const object_id& id,
  first_byte_offset_t byte_offset,
  iobuf payload) {
    if (_lm == nullptr) {
        return;
    }
    _gate.check();

    auto& state = ensure_partition_hydrated_state(tidp);
    if (!state.translation_map) {
        return;
    }

    // put_extent allocates a synthetic offset and creates epoch index if needed
    auto synthetic = state.translation_map->put_extent(
      id, byte_offset, payload.size_bytes());

    if (!synthetic.has_value()) {
        // Caching is disabled or failed to create index
        return;
    }

    // Get the batch_cache_index for this epoch
    auto* index = state.translation_map->get_batch_cache_index(id.epoch);
    if (index == nullptr) {
        return;
    }

    // Check if already stored in the index
    if (index->get(*synthetic).has_value()) {
        return;
    }

    // Create a fake record batch to store the payload
    // We use the synthetic offset as the base_offset
    // The term is set to a placeholder value since this is hydrated (not
    // materialized) data
    model::record_batch_header header{
      .size_bytes = static_cast<int32_t>(
        model::packed_record_batch_header_size + payload.size_bytes()),
      .base_offset = *synthetic,
      .type = model::record_batch_type::raft_data,
      .crc = 0,
      .attrs = model::record_batch_attributes{},
      .last_offset_delta = 0,
      .first_timestamp = model::timestamp::now(),
      .max_timestamp = model::timestamp::now(),
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = 1,
      .ctx = model::record_batch_header::context(
        model::term_id{0}, ss::this_shard_id()),
    };

    auto batch = model::record_batch(header, std::move(payload));
    index->put(batch, storage::batch_cache::is_dirty_entry::no);
    _probe.register_hydrated_put(batch.size_bytes());
}

void batch_cache::truncate_hydrated_internal(
  const model::topic_id_partition& tidp, cluster_epoch invalidated_epoch) {
    auto it = _partition_hydrated.find(tidp);
    if (it == _partition_hydrated.end() || !it->second.translation_map) {
        return;
    }

    auto& state = it->second;

    // Truncate entries from epochs older than the invalidated epoch.
    // This removes the per-epoch batch_cache_index entries and truncates
    // their underlying batch_cache data.
    state.translation_map->truncate_epoch(invalidated_epoch);
}

} // namespace cloud_topics
