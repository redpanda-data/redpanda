/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/frontend_reader/reader.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "model/timeout_clock.h"

#include <chrono>
#include <exception>
#include <iterator>
#include <utility>
#include <variant>

namespace cloud_topics {

// TODO: add config
static constexpr size_t L0_max_bytes_per_metadata_fetch = 4_KiB;

level_zero_log_reader_impl::level_zero_log_reader_impl(
  cloud_topic_log_reader_config& cfg,
  ss::lw_shared_ptr<cluster::partition> underlying,
  data_plane_api* ct_api)
  : _config(cfg)
  , _underlying(std::move(underlying))
  , _ct_api(ct_api) {}

ss::future<model::record_batch_reader::storage_t>
level_zero_log_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    // We're only fetching from the record batch cache if the reader is in
    // the 'empty' state. It doesn't make any difference if the reader is in
    // the 'materialized' state. If we're in 'ready' state we risk to go out
    // of sync with cached metadata so it's safer to hydrate.
    if (cache_enabled()) {
        if (auto cached = maybe_load_slices_from_cache()) {
            co_return std::move(cached.value());
        }
    }

    chunked_circular_buffer<model::record_batch> res;
    switch (_current) {
    case state::empty_state:
        co_await fetch_metadata(deadline);
        [[fallthrough]];
    case state::ready_state:
        co_await materialize_batches(deadline);
        [[fallthrough]];
    case state::materialized_state:
    case state::end_of_stream_state:
        // Handled in the next switch statement
        break;
    }

    // Invariant: in case of success the state will be either materialized
    // or end_of_stream. In case of error the state will be end_of_stream.
    switch (_current) {
    case state::empty_state:
    case state::ready_state:
        _current = state::end_of_stream_state;
        throw std::runtime_error("Invalid reader state (ready/empty)");
    case state::materialized_state:
        consume_materialized_batches(&res);
        [[fallthrough]];
    case state::end_of_stream_state:
        break;
    }
    co_return res;
}

std::optional<chunked_circular_buffer<model::record_batch>>
level_zero_log_reader_impl::maybe_load_slices_from_cache() {
    chunked_circular_buffer<model::record_batch> ret;
    size_t materialized_bytes = 0;
    auto current = _config.start_offset;
    while (materialized_bytes < _config.max_bytes
           && current <= _config.max_offset) {
        auto batch = _ct_api->cache_get(
          _underlying->ntp(), kafka::offset_cast(current));
        size_t batch_size = 0;
        if (!batch.has_value()) {
            // We hit a gap in the cache and have to download objects
            // from S3.
            //
            // NOTE: this can also happen when transactions are used and
            // we would encounter reading a control batch from the local log.
            break;
        }
        vlog(
          cd_log.trace,
          "Loaded batch from cache for {}: {} @ term {}",
          current,
          batch.value().base_offset(),
          batch.value().term());
        vassert(
          batch.value().term() > model::term_id{-1},
          "Batch without term in the cache: {}",
          batch.value().header());
        batch_size = batch.value().size_bytes();
        if (
          !ret.empty() && batch_size + materialized_bytes > _config.max_bytes) {
            // The batch will cause over the limit.
            // We want to accept the oversized batch if the res is empty to
            // avoid stalling the reader.
            break;
        }
        vassert(
          batch->base_offset() <= kafka::offset_cast(current)
            && kafka::offset_cast(current) <= batch->last_offset(),
          "Unexpected batch for {}, got range: [{},{}] for offset {}",
          _underlying->ntp(),
          batch->base_offset(),
          batch->last_offset(),
          current);
        ret.push_back(std::move(batch.value()));
        materialized_bytes += batch_size;
        // Invariant: it's guaranteed that the 'ret' is not empty.
        current = model::offset_cast(
          model::next_offset(ret.back().last_offset()));
    }
    _config.start_offset = current;
    if (_config.start_offset > _config.max_offset) {
        vlog(
          cd_log.debug,
          "reached end of stream, start offset: {}, max offset: {}, "
          "current: {}",
          _config.start_offset,
          _config.max_offset,
          current);
        _current = state::end_of_stream_state;
    }
    if (!ret.empty()) {
        return ret;
    }
    return std::nullopt;
}

ss::future<> level_zero_log_reader_impl::fetch_metadata(
  model::timeout_clock::time_point deadline) {
    vassert(
      _current == state::empty_state || _current == state::materialized_state,
      "Invalid state transition, unexpected current state: {}",
      std::to_underlying(_current));
    if (_unhydrated.size() > 0) {
        // If we already have metadata, we can skip fetching it again.
        _current = state::ready_state;
        co_return;
    }
    try {
        // Fetch metadata from the _underlying
        auto ot_state = _underlying->get_offset_translator_state();
        auto start_offset = ot_state->to_log_offset(
          kafka::offset_cast(_config.start_offset));
        auto max_offset = ot_state->to_log_offset(
          kafka::offset_cast(_config.max_offset));
        storage::local_log_reader_config cfg(
          start_offset,
          max_offset,
          _config.min_bytes,
          _config.max_bytes,
          // We need to fetch both raft data batches for transaction control
          // markers as well as placeholder batches to hydrate from object
          // storage, so we don't include a typefilter and instead postfilter
          // here.
          /*type_filter=*/std::nullopt,
          _config.first_timestamp,
          _config.abort_source,
          _config.client_address);
        cfg.translate_offsets = model::translate_offsets::yes;
        // This parameter defines how many bytes we want to fetch
        // from the underlying partition in one go.
        // The L0 meta batches are small, so we can fetch a lot of them in a
        // single request and then gradually materialize them.
        // The 'cfg.max_bytes' doesn't limit the size of the materialized
        // batches, because it is fetching L0 meta batches, which have different
        // size. In order to know the size of the materialized batches we need
        // to fetch L0 meta batches first and then parse them.
        cfg.max_bytes = L0_max_bytes_per_metadata_fetch;

        auto reader = co_await _underlying->make_local_reader(cfg);
        auto placeholders = co_await model::consume_reader_to_chunked_vector(
          std::move(reader), deadline);

        // Convert L0 meta batches to extent_meta structures.
        for (auto&& batch : placeholders) {
            auto& header = batch.header();
            if (header.type == model::record_batch_type::raft_data) {
                local_log_batch local_batch{.header = header};
                local_batch.data = std::move(batch).release_data();
                _unhydrated.push_back(std::move(local_batch));
                continue;
            }
            if (header.type != model::record_batch_type::dl_placeholder) {
                continue;
            }
            cloud_topics::extent_meta e{
              .base_offset = model::offset_cast(batch.base_offset()),
              .last_offset = model::offset_cast(batch.last_offset()),
            };
            iobuf payload = std::move(batch).release_data();
            iobuf_parser parser(std::move(payload));
            auto record = model::parse_one_record_from_buffer(parser);
            iobuf value = std::move(record).release_value();
            auto placeholder = serde::from_iobuf<cloud_topics::dl_placeholder>(
              std::move(value));
            e.id = placeholder.id;
            e.first_byte_offset = placeholder.offset;
            e.byte_range_size = placeholder.size_bytes;
            _unhydrated.push_back(local_log_batch{.header = header, .data = e});
        }
        if (!_unhydrated.empty()) {
            vlog(
              cd_log.debug,
              "Fetched {} L0 meta batches from the underlying "
              "partition, first offset: {}, last offset: {}",
              _unhydrated.size(),
              _unhydrated.front().header.base_offset,
              _unhydrated.back().header.last_offset());
        } else {
            vlog(
              cd_log.debug,
              "No L0 meta batches fetched from the underlying partition, "
              "start offset: {}, max offset: {}",
              cfg.start_offset,
              cfg.max_offset);
        }
    } catch (...) {
        vlog(
          cd_log.info,
          "Failed to fetch metadata from the underlying partition: {}",
          std::current_exception());
        _hydrated.clear();
        _unhydrated.clear();
        _current = state::end_of_stream_state;
        throw;
    }
    _current = _unhydrated.empty() ? state::end_of_stream_state
                                   : state::ready_state;
}

ss::future<> level_zero_log_reader_impl::materialize_batches(
  model::timeout_clock::time_point deadline) {
    if (_current == state::end_of_stream_state) {
        _current = state::end_of_stream_state;
        vlog(cd_log.trace, "Materialize batches called while EOS");
        co_return;
    }
    vassert(
      _current == state::ready_state || _current == state::materialized_state,
      "Invalid state transition, unexpected current state: {}",
      std::to_underlying(_current));

    if (_hydrated.size() > 0) {
        // We're already materialized.
        _current = state::materialized_state;
        vlog(
          cd_log.trace,
          "Materialize batches call redundant, already materialized");
        co_return;
    }

    if (_unhydrated.empty()) {
        // Nothing to materialize.
        _current = state::end_of_stream_state;
        vlog(cd_log.trace, "Materialize batches without unhydrated batches");
        co_return;
    }

    // Cherry-pick enough L0 meta batches to materialize.
    try {
        chunked_vector<cloud_topics::extent_meta> to_materialize;
        auto unhydrated_it = _unhydrated.begin();
        size_t materialize_bytes = 0;
        for (; unhydrated_it != _unhydrated.end(); ++unhydrated_it) {
            size_t hydrated_batch_size = ss::visit(
              unhydrated_it->data,
              [](const local_log_batch::payload& payload) {
                  return payload.size_bytes();
              },
              [](const cloud_topics::extent_meta& meta) {
                  return meta.byte_range_size();
              });
            if (is_over_limit(hydrated_batch_size)) {
                // If the next meta batch exceeds the max bytes limit, we stop
                // materializing. The only exception is if we didn't collect any
                // batches yet, in which case we still materialize the next
                // batch. This could happen if the first meta batch is larger
                // than the max bytes limit (oversized batch or too small
                // limit). In this case we don't want to stall the reader
                // completely.
                vlog(
                  cd_log.trace,
                  "Materialize batches overshot at {} bytes, config: {}, last "
                  "hydrated batch size: {}",
                  materialize_bytes,
                  _config,
                  hydrated_batch_size);
                break;
            }
            _config.bytes_consumed += hydrated_batch_size;
            if (
              auto* meta = std::get_if<cloud_topics::extent_meta>(
                &unhydrated_it->data)) {
                materialize_bytes += meta->byte_range_size;
                to_materialize.push_back(*meta);
                vlog(
                  cd_log.trace,
                  "Materialize {} bytes total...",
                  materialize_bytes);
            }
        }
        size_t materialize_count = to_materialize.size();
        vlog(
          cd_log.trace,
          "Invoking 'materialize' for {}: {} bytes, {} batches to materialize",
          _underlying->ntp(),
          materialize_bytes,
          materialize_count);
        // Ask data layer to bring data from the cloud storage.
        auto mat_res = co_await _ct_api->materialize(
          _underlying->ntp(),
          materialize_bytes,
          std::move(to_materialize),
          deadline);
        if (mat_res.has_error()) {
            throw std::runtime_error(
              fmt::format(
                "Failed to materialize batches from the cloud storage: {}",
                mat_res.error().message()));
        }
        auto batches = std::move(mat_res.value());
        if (batches.size() != materialize_count) {
            throw std::runtime_error(
              fmt::format(
                "Materialized unexpected number of batches: {}, expected: {}",
                batches.size(),
                materialize_count));
        }
        // Merge our selected subset of unhydrated batches with the materialized
        // batches, preserving control batches from the local log.
        auto batches_it = batches.begin();
        chunked_circular_buffer<model::record_batch> hydrated;
        auto range_to_materialize = std::ranges::subrange(
          _unhydrated.begin(), unhydrated_it);
        for (local_log_batch& local_batch : range_to_materialize) {
            auto& local_batch_header = local_batch.header;
            model::record_batch batch = ss::visit(
              local_batch.data,
              [this, &local_batch_header, &batches_it](
                const cloud_topics::extent_meta&) {
                  model::record_batch batch = std::move(*batches_it);
                  ++batches_it;
                  auto size = batch.header().size_bytes;
                  batch.header() = local_batch_header;
                  batch.header().type = model::record_batch_type::raft_data;
                  batch.header().size_bytes = size;
                  batch.header().reset_size_checksum_metadata(batch.data());
                  // Propagate materialized batches to the record batch cache
                  if (cache_enabled()) {
                      vlog(
                        cd_log.trace,
                        "Putting batch for {} to cache: {}, term: {}",
                        _underlying->ntp(),
                        batch.base_offset(),
                        batch.term());
                      _ct_api->cache_put(_underlying->ntp(), batch.copy());
                  }
                  return batch;
              },
              [&local_batch_header](local_log_batch::payload& payload) {
                  return model::record_batch(
                    local_batch_header,
                    std::move(payload),
                    model::record_batch::tag_ctor_ng{});
              });
            hydrated.push_back(std::move(batch));
        }
        vassert(
          batches_it == batches.end(),
          "All materialized batches should be used");
        _unhydrated.erase(
          range_to_materialize.begin(), range_to_materialize.end());
        _hydrated = std::move(hydrated);
        // Materialize batches from the L0 meta batches.
        vlog(
          cd_log.debug,
          "Materialized {} batches from the L0 meta batches",
          _hydrated.size());
    } catch (...) {
        vlog(
          cd_log.info,
          "Failed to materialize batches {}",
          std::current_exception());
        _hydrated.clear();
        _unhydrated.clear();
        _current = state::end_of_stream_state;
        throw;
    }

    _current = _hydrated.empty() ? state::end_of_stream_state
                                 : state::materialized_state;
}

bool level_zero_log_reader_impl::cache_enabled() const {
    if (_config.skip_cache) {
        return false;
    }
    if (!_underlying->log()->config().cache_enabled()) {
        return false;
    }
    if (config::shard_local_cfg().disable_batch_cache()) {
        return false;
    }
    return true;
}

void level_zero_log_reader_impl::consume_materialized_batches(
  chunked_circular_buffer<model::record_batch>* dest) {
    vlog(
      cd_log.debug,
      "consuming {} materialized batches, cached {} extents",
      _hydrated.size(),
      _unhydrated.size());
    *dest = std::exchange(_hydrated, {});
    _config.start_offset = model::offset_cast(
      model::next_offset(dest->back().last_offset()));
    _current = _unhydrated.empty() ? state::empty_state : state::ready_state;
}

void level_zero_log_reader_impl::print(std::ostream& o) {
    o << "cloud_topics_reader";
}

bool level_zero_log_reader_impl::is_end_of_stream() const {
    return _current == state::end_of_stream_state;
}

bool level_zero_log_reader_impl::is_over_limit(size_t size) const {
    return (_config.strict_max_bytes || _config.bytes_consumed > 0)
           && (_config.bytes_consumed + size) > _config.max_bytes;
}
} // namespace cloud_topics
