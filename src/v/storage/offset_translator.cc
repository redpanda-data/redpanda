/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/offset_translator.h"

#include "base/vlog.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/logger.h"
#include "storage/storage_resources.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace storage {

static ss::logger logger{"offset_translator"};

offset_translator::offset_translator(
  std::vector<model::record_batch_type> filtered_types,
  raft::group_id group,
  model::ntp ntp,
  storage::kvstore& kvs,
  storage::storage_resources& resources)
  : _filtered_types(std::move(filtered_types))
  , _state(ss::make_lw_shared<storage::offset_translator_state>(std::move(ntp)))
  , _group(group)
  , _logger(logger, ssx::sformat("ntp: {}", _state->ntp()))
  , _kvs(kvs)
  , _resources(resources) {}

offset_translator::offset_translator(
  std::vector<model::record_batch_type> filtered_types,
  raft::group_id group,
  model::ntp ntp,
  storage::api& storage_api)
  : offset_translator(
      std::move(filtered_types),
      group,
      std::move(ntp),
      storage_api.kvs(),
      storage_api.resources()) {}

void offset_translator::process(const model::record_batch& batch) {
    if (_filtered_types.empty()) {
        return;
    }

    _bytes_processed += batch.size_bytes();

    // Update resource manager for the extra dirty bytes, it may hint us
    // to checkpoint early in response.
    _checkpoint_hint |= _resources.offset_translator_take_bytes(
      batch.size_bytes(), _bytes_processed_units);

    if (
      std::find(
        _filtered_types.begin(), _filtered_types.end(), batch.header().type)
      != _filtered_types.end()) {
        _state->add_gap(batch.base_offset(), batch.last_offset());

        vlog(
          _logger.trace,
          "adding batch, offsets: [{},{}], delta: {}",
          batch.base_offset(),
          batch.last_offset(),
          _state->last_delta());

        ++_map_version;
    }

    _highest_known_offset = std::max(
      _highest_known_offset, batch.last_offset());
}

namespace {

enum class kvstore_key_type : int8_t {
    offsets_map = 0,
    highest_known_offset = 1,
};

bytes serialize_kvstore_key(raft::group_id group, kvstore_key_type key_type) {
    iobuf buf;
    reflection::serialize(buf, key_type, group);
    return iobuf_to_bytes(buf);
}

} // namespace

bytes offset_translator::kvstore_offsetmap_key(raft::group_id group) {
    return serialize_kvstore_key(group, kvstore_key_type::offsets_map);
}

bytes offset_translator::kvstore_highest_known_offset_key(
  raft::group_id group) {
    return serialize_kvstore_key(group, kvstore_key_type::highest_known_offset);
}

ss::future<> offset_translator::start(must_reset reset) {
    vassert(
      _state->empty(),
      "ntp {}: offset translator state was modified before start()",
      _state->ntp());

    if (_filtered_types.empty()) {
        co_return;
    }

    if (reset) {
        vlog(_logger.info, "resetting offset translation state");

        *_state = storage::offset_translator_state(
          _state->ntp(), model::offset::min(), 0);
        ++_map_version;
        _highest_known_offset = model::offset::min();

        co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
    } else {
        auto map_buf = _kvs.get(
          storage::kvstore::key_space::offset_translator, offsets_map_key());
        auto highest_known_offset_buf = _kvs.get(
          storage::kvstore::key_space::offset_translator,
          highest_known_offset_key());

        if (map_buf && highest_known_offset_buf) {
            *_state = storage::offset_translator_state::from_serialized_map(
              _state->ntp(), std::move(*map_buf));
            _highest_known_offset = reflection::from_iobuf<model::offset>(
              std::move(*highest_known_offset_buf));

            // highest known offset could be more stale than the map, in
            // this case we take it from the map
            _highest_known_offset = std::max(
              _highest_known_offset, _state->last_gap_offset());
        } else {
            vlog(
              _logger.info,
              "offset translation kvstore state not found, will recover it "
              "from the log later");
            *_state = storage::offset_translator_state(
              _state->ntp(), model::offset::min(), 0);
            ++_map_version;
            _highest_known_offset = model::offset::min();
        }
    }

    vassert(
      !_state->empty(),
      "ntp {}: offset translation state shouldn't be empty",
      _state->ntp());

    vlog(
      _logger.info,
      "started, state: {}, highest_known_offset: {}",
      _state,
      _highest_known_offset);
}

ss::future<> offset_translator::sync_with_log(
  storage::log& log, storage::opt_abort_source_t as) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_state->empty(),
      "ntp {}: offset translation state shouldn't be empty",
      _state->ntp());

    auto log_offsets = log.offsets();

    // Trim the offset2delta map to log dirty_offset (discrepancy can
    // happen if the offsets map was persisted, but the log wasn't flushed).
    if (_state->truncate(model::next_offset(log_offsets.dirty_offset))) {
        ++_map_version;
    }

    if (log_offsets.dirty_offset < _highest_known_offset) {
        _highest_known_offset = log_offsets.dirty_offset;
        co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
    }

    // Read the log to insert the remaining entries into map.
    model::offset start_offset = model::next_offset(_highest_known_offset);

    vlog(
      _logger.debug,
      "starting sync with log, state: {}, reading offsets {}-{}",
      _state,
      _highest_known_offset,
      log_offsets.dirty_offset);

    auto reader_cfg = storage::log_reader_config(
      start_offset, log_offsets.dirty_offset, ss::default_priority_class(), as);
    auto reader = co_await log.make_reader(reader_cfg);

    struct log_consumer {
        explicit log_consumer(offset_translator& self)
          : self(self) {}

        ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
            self.process(b);
            co_return ss::stop_iteration::no;
        }

        void end_of_stream() {}

        offset_translator& self;
    };

    co_await std::move(reader).for_each_ref(
      log_consumer{*this}, model::no_timeout);

    if (_highest_known_offset < log_offsets.dirty_offset) {
        throw std::runtime_error{_logger.format(
          "couldn't sync offset translator up to the log tip, "
          "highest_known_offset: {}, log dirty offset: {}",
          _highest_known_offset,
          log_offsets.dirty_offset)};
    }

    vlog(
      _logger.info,
      "synced with log, state: {}, highest_known_offset: {}",
      _state,
      _highest_known_offset);

    co_await maybe_checkpoint();
}

ss::future<> offset_translator::truncate(model::offset offset) {
    if (_filtered_types.empty()) {
        co_return;
    }

    if (_state->truncate(offset)) {
        ++_map_version;
    }

    model::offset prev = model::prev_offset(offset);
    _highest_known_offset = std::min(prev, _highest_known_offset);

    vlog(_logger.info, "truncate at offset: {}, new state: {}", offset, _state);

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<> offset_translator::prefix_truncate(
  model::offset offset,
  std::optional<model::offset_delta> force_truncate_delta) {
    if (_filtered_types.empty()) {
        co_return;
    }

    if (unlikely(offset > _highest_known_offset)) {
        if (!force_truncate_delta.has_value()) {
            throw std::runtime_error{_logger.format(
              "trying to prefix truncate offset translator at offset {} which "
              "is > highest_known_offset {}",
              offset,
              _highest_known_offset)};
        }
        // If the truncation is for past the end of the log (e.g. in the case
        // of a stale replica being caught up from a snapshot), allow it.
        *_state = storage::offset_translator_state(
          _state->ntp(), offset, force_truncate_delta.value());
        _highest_known_offset = offset;
    } else if (!_state->prefix_truncate(offset)) {
        co_return;
    }

    ++_map_version;

    vlogl(
      _logger,
      force_truncate_delta.has_value() ? ss::log_level::info
                                       : ss::log_level::debug,
      "prefix_truncate at offset: {}, force_truncate_delta: {}, new state: {}",
      offset,
      force_truncate_delta,
      _state);

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<> offset_translator::remove_persistent_state() {
    if (_filtered_types.empty()) {
        co_return;
    }

    co_await _kvs.remove(
      storage::kvstore::key_space::offset_translator,
      highest_known_offset_key());
    co_await _kvs.remove(
      storage::kvstore::key_space::offset_translator, offsets_map_key());
}

bytes offset_translator::offsets_map_key() const {
    return kvstore_offsetmap_key(_group);
}

bytes offset_translator::highest_known_offset_key() const {
    return kvstore_highest_known_offset_key(_group);
}

ss::future<> offset_translator::maybe_checkpoint(size_t checkpoint_threshold) {
    if (_filtered_types.empty()) {
        co_return;
    }

    auto maybe_locked = _checkpoint_lock.try_get_units();
    if (!maybe_locked) {
        // A checkpoint attempt is in progress, it doesn't make much sense to
        // do another one.
        co_return;
    }

    if (
      _bytes_processed < _bytes_processed_at_checkpoint + checkpoint_threshold
      && !_checkpoint_hint) {
        co_return;
    }

    vlog(
      _logger.trace,
      "threshold reached, performing checkpoint; state: {}, "
      "highest_known_offset: {} (hint={})",
      _state,
      _highest_known_offset,
      _checkpoint_hint);

    co_await do_checkpoint();
}

ss::future<> offset_translator::do_checkpoint() {
    // Read state in a single continuation to get a consistent snapshot.

    size_t bytes_processed = _bytes_processed;
    size_t map_version = _map_version;

    std::optional<iobuf> map_buf;
    if (map_version > _map_version_at_checkpoint) {
        map_buf.emplace(_state->serialize_map());
    }

    iobuf hko_buf = reflection::to_iobuf(_highest_known_offset);

    // Persisting offsets map before highest offset so that if the latter
    // fails, we are still left with a consistent state (map can be
    // recreated by reading log from the highest known offset).

    if (map_buf) {
        co_await _kvs.put(
          storage::kvstore::key_space::offset_translator,
          offsets_map_key(),
          std::move(*map_buf));
        _map_version_at_checkpoint = map_version;
    }

    co_await _kvs.put(
      storage::kvstore::key_space::offset_translator,
      highest_known_offset_key(),
      std::move(hko_buf));
    _bytes_processed_at_checkpoint = bytes_processed;
    _bytes_processed_units.return_all();

    _checkpoint_hint = false;
}

ss::future<> offset_translator::copy_persistent_state(
  raft::group_id group,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api) {
    struct ot_state {
        std::optional<iobuf> highest_known_offset;
        std::optional<iobuf> offset_map;
    };
    vlog(
      storage::stlog.debug,
      "moving group {} offset translator state to {}",
      group,
      target_shard);
    static constexpr auto ks = storage::kvstore::key_space::offset_translator;
    ot_state state{
      .highest_known_offset = source_kvs.get(
        ks,
        serialize_kvstore_key(group, kvstore_key_type::highest_known_offset)),
      .offset_map = source_kvs.get(
        ks, serialize_kvstore_key(group, kvstore_key_type::offsets_map)),
    };

    co_await api.invoke_on(
      target_shard, [gr = group, &state](storage::api& api) -> ss::future<> {
          std::vector<ss::future<>> write_futures;
          write_futures.reserve(2);
          if (state.offset_map) {
              write_futures.push_back(api.kvs().put(
                ks,
                serialize_kvstore_key(gr, kvstore_key_type::offsets_map),
                state.offset_map->copy()));
          }
          if (state.highest_known_offset) {
              write_futures.push_back(api.kvs().put(
                ks,
                serialize_kvstore_key(
                  gr, kvstore_key_type::highest_known_offset),
                state.highest_known_offset->copy()));
          }

          return ss::when_all_succeed(std::move(write_futures));
      });
}

ss::future<> offset_translator::remove_persistent_state(
  raft::group_id group, storage::kvstore& kvs) {
    static constexpr auto ks = storage::kvstore::key_space::offset_translator;
    std::vector<ss::future<>> remove_futures;
    remove_futures.reserve(2);
    remove_futures.push_back(kvs.remove(
      ks,
      serialize_kvstore_key(group, kvstore_key_type::highest_known_offset)));
    remove_futures.push_back(kvs.remove(
      ks, serialize_kvstore_key(group, kvstore_key_type::offsets_map)));
    co_await ss::when_all_succeed(std::move(remove_futures));
}

} // namespace storage
