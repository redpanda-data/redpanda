/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/offset_translator.h"

#include "model/adl_serde.h"
#include "raft/consensus_utils.h"
#include "reflection/adl.h"
#include "serde/serde.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace raft {

static ss::logger logger{"offset_translator"};

offset_translator::offset_translator(
  std::vector<model::record_batch_type> filtered_types,
  raft::group_id group,
  model::ntp ntp,
  storage::api& storage_api)
  : _filtered_types(std::move(filtered_types))
  , _group(group)
  , _ntp(ntp)
  , _logger(logger, ssx::sformat("ntp: {}", _ntp))
  , _storage_api(storage_api) {}

int64_t offset_translator::delta(model::offset o) const {
    if (_filtered_types.empty()) {
        return 0;
    }

    auto it = _offset2delta.lower_bound(o);
    if (it == _offset2delta.begin()) {
        throw std::runtime_error{_logger.format(
          "log offset {} is outside the translation range (starting at {})",
          o,
          _offset2delta.begin()->first)};
    }
    return std::prev(it)->second;
}

model::offset offset_translator::from_log_offset(model::offset o) const {
    const auto d = delta(o);
    return model::offset(o - d);
}

model::offset offset_translator::to_log_offset(
  model::offset data_offset, model::offset hint) const {
    if (_filtered_types.empty()) {
        return data_offset;
    }

    if (data_offset == model::offset::max()) {
        return data_offset;
    }

    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    model::offset min_log_offset = details::next_offset(
      _offset2delta.begin()->first);

    model::offset min_data_offset
      = min_log_offset - model::offset(_offset2delta.begin()->second);
    vassert(
      data_offset >= min_data_offset,
      "ntp {}: data offset {} must be inside translation range (starting at "
      "{})",
      _ntp,
      data_offset,
      min_data_offset);

    model::offset search_start = std::max(
      std::max(hint, data_offset), min_log_offset);

    // We iterate over the intervals (beginning exclusive, end inclusive)
    // with constant delta, starting at the interval containing
    // log offset equal to `data_offset` (because log offset is at least as
    // big as data offset) and stopping when we find the interval where
    // given data offset is achievable.
    auto interval_end_it = _offset2delta.lower_bound(search_start);
    auto delta = std::prev(interval_end_it)->second;

    while (interval_end_it != _offset2delta.end()) {
        // last data record in this interval is at offset
        // interval_end_it->first - 1
        model::offset max_do_this_interval
          = details::prev_offset(interval_end_it->first) - model::offset(delta);
        if (max_do_this_interval >= data_offset) {
            break;
        }

        delta = interval_end_it->second;
        ++interval_end_it;
    }

    return data_offset + model::offset(delta);
}

void offset_translator::process(const model::record_batch& batch) {
    if (_filtered_types.empty()) {
        return;
    }

    _bytes_processed += batch.size_bytes();

    if (
      std::find(
        _filtered_types.begin(), _filtered_types.end(), batch.header().type)
      != _filtered_types.end()) {
        // TODO: maybe store batch spans instead of adding each individual
        // batch record.
        for (auto o = batch.base_offset(); o <= batch.last_offset(); ++o) {
            do_add(o);
        }
    }

    _highest_known_offset = std::max(
      _highest_known_offset, batch.last_offset());
}

namespace {

enum class kvstore_key_type : int8_t {
    offsets_map = 0,
    highest_known_offset = 1,
};

struct persisted_offsets_map
  : serde::envelope<persisted_offsets_map, serde::version<0>> {
    int64_t start_delta = 0;
    std::vector<model::offset> offsets;
};

bytes serialize_kvstore_key(raft::group_id group, kvstore_key_type key_type) {
    iobuf buf;
    reflection::serialize(buf, key_type, group);
    return iobuf_to_bytes(buf);
}

} // namespace

iobuf offset_translator::serialize_offsets_map(
  const absl::btree_map<model::offset, int64_t>& offset2delta) {
    vassert(
      !offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    std::vector<model::offset> offsets;
    offsets.reserve(offset2delta.size());
    for (const auto& o2d : offset2delta) {
        offsets.push_back(o2d.first);
    }

    persisted_offsets_map persisted{
      .start_delta = offset2delta.begin()->second,
      .offsets = std::move(offsets),
    };

    return serde::to_iobuf(std::move(persisted));
}

absl::btree_map<model::offset, int64_t>
offset_translator::deserialize_offsets_map(iobuf buf) {
    auto persisted = serde::from_iobuf<persisted_offsets_map>(std::move(buf));
    if (persisted.offsets.empty()) {
        throw std::runtime_error{
          _logger.format("persisted offset translator map shouldn't be empty")};
    }

    absl::btree_map<model::offset, int64_t> offset2delta;
    int64_t cur_delta = persisted.start_delta;
    model::offset prev_offset;
    for (auto it = persisted.offsets.begin(); it != persisted.offsets.end();
         ++it) {
        model::offset offset = *it;
        if (it != persisted.offsets.begin() && offset <= prev_offset) {
            throw std::runtime_error{_logger.format(
              "inconsistency in serialized offset translator state: offset {} "
              "is after {}",
              offset,
              prev_offset)};
        }

        offset2delta.emplace(offset, cur_delta);
        cur_delta += 1;
        prev_offset = offset;
    }

    return offset2delta;
}

ss::future<>
offset_translator::start(must_reset reset, bootstrap_state&& bootstrap) {
    vassert(
      _offset2delta.empty(),
      "ntp {}: offset_translator was modified before start()",
      _ntp);

    if (_filtered_types.empty()) {
        co_return;
    }

    if (reset) {
        vlog(_logger.info, "resetting offset translation state");

        _offset2delta.emplace(model::offset::min(), 0);
        ++_map_version;
        _highest_known_offset = model::offset::min();

        co_await do_checkpoint();
    } else {
        auto map_buf = _storage_api.kvs().get(
          storage::kvstore::key_space::offset_translator, offsets_map_key());
        auto highest_known_offset_buf = _storage_api.kvs().get(
          storage::kvstore::key_space::offset_translator,
          highest_known_offset_key());

        if (map_buf && highest_known_offset_buf) {
            _offset2delta = deserialize_offsets_map(std::move(*map_buf));
            _highest_known_offset = reflection::from_iobuf<model::offset>(
              std::move(*highest_known_offset_buf));

            // highest known offset could be more stale than the map, in
            // this case we take it from the map
            _highest_known_offset = std::max(
              _highest_known_offset, _offset2delta.rbegin()->first);
        } else {
            // For backwards compatibility: load state from
            // configuration_manager state
            vlog(
              _logger.info,
              "offset translation kvstore state not found, loading from "
              "provided bootstrap state");

            _offset2delta = std::move(bootstrap.offset2delta);
            ++_map_version;
            _highest_known_offset = bootstrap.highest_known_offset;

            co_await do_checkpoint();
        }
    }

    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    vlog(
      _logger.info,
      "started, base offset/delta: {}/{}, map size: {}, "
      "highest_known_offset: {}",
      _offset2delta.begin()->first,
      _offset2delta.begin()->second,
      _offset2delta.size(),
      _highest_known_offset);
}

ss::future<> offset_translator::sync_with_log(
  storage::log log, storage::opt_abort_source_t as) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    auto log_offsets = log.offsets();

    // Trim the offset2delta map to log dirty_offset (discrepancy can
    // happen if the offsets map was persisted, but the log wasn't flushed).
    auto end_it = _offset2delta.upper_bound(log_offsets.dirty_offset);
    if (end_it == _offset2delta.begin()) {
        throw std::runtime_error{_logger.format(
          "can't sync offset_translator with log: dirty offset {} is < base "
          "translation offset {}",
          log_offsets.dirty_offset,
          _offset2delta.begin()->first)};
    }

    if (end_it != _offset2delta.end()) {
        _offset2delta.erase(end_it, _offset2delta.end());
        ++_map_version;
    }

    if (log_offsets.dirty_offset < _highest_known_offset) {
        _highest_known_offset = log_offsets.dirty_offset;
        co_await do_checkpoint();
    }

    // read the log to insert the remaining entries into map
    model::offset start_offset = details::next_offset(_highest_known_offset);
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

    vlog(
      _logger.info,
      "synced with log, base offset/delta: {}/{}, map size: {}, "
      "highest_known_offset: {}",
      _offset2delta.begin()->first,
      _offset2delta.begin()->second,
      _offset2delta.size(),
      _highest_known_offset);

    co_await maybe_checkpoint();
}

ss::future<> offset_translator::truncate(model::offset offset) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    auto it = _offset2delta.lower_bound(offset);
    if (it == _offset2delta.begin()) {
        throw std::runtime_error{_logger.format(
          "trying to truncate offset_translator at offset {} which "
          "is <= base translation offset {}",
          offset,
          _offset2delta.begin()->first)};
    }

    if (it != _offset2delta.end()) {
        _offset2delta.erase(it, _offset2delta.end());
        ++_map_version;
    }

    model::offset prev = details::prev_offset(offset);
    _highest_known_offset = std::min(prev, _highest_known_offset);

    vlog(
      _logger.info,
      "truncate at offset: {}, new map size: {}",
      offset,
      _offset2delta.size());

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<> offset_translator::prefix_truncate(model::offset offset) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    if (offset > _highest_known_offset) {
        throw std::runtime_error{_logger.format(
          "trying to prefix truncate offset translator at offset {} which is > "
          "highest_known_offset {}",
          offset,
          _highest_known_offset)};
    }

    auto it = _offset2delta.upper_bound(offset);
    if (it == _offset2delta.begin()) {
        co_return;
    }

    auto prev_it = std::prev(it);
    if (prev_it == _offset2delta.begin() && prev_it->first == offset) {
        co_return;
    }

    auto base_delta = prev_it->second;
    _offset2delta.erase(_offset2delta.begin(), it);
    _offset2delta.emplace(offset, base_delta);
    ++_map_version;

    vlog(
      _logger.info,
      "prefix_truncate at offset: {}, new base delta: {}, new map size: {}",
      offset,
      base_delta,
      _offset2delta.size());

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<>
offset_translator::prefix_truncate_reset(model::offset offset, int64_t delta) {
    if (_filtered_types.empty()) {
        co_return;
    }

    if (offset <= _highest_known_offset) {
        co_await prefix_truncate(offset);
        co_return;
    }

    _offset2delta.clear();
    _offset2delta.emplace(offset, delta);
    ++_map_version;

    _highest_known_offset = offset;

    vlog(
      _logger.info,
      "prefix_truncate_reset at offset/delta: {}/{}",
      offset,
      delta);

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<> offset_translator::remove_persistent_state() {
    if (_filtered_types.empty()) {
        co_return;
    }

    co_await _storage_api.kvs().remove(
      storage::kvstore::key_space::offset_translator,
      highest_known_offset_key());
    co_await _storage_api.kvs().remove(
      storage::kvstore::key_space::offset_translator, offsets_map_key());
}

void offset_translator::do_add(model::offset offset) {
    vassert(
      !_offset2delta.empty(), "ntp {}: offsets map shouldn't be empty", _ntp);

    auto rbegin = _offset2delta.rbegin();
    vassert(
      offset > rbegin->first,
      "ntp {}: trying to add batch to offset translator at offset {} that "
      "is "
      "not higher than the previous offset {}",
      _ntp,
      offset,
      rbegin->first);
    int64_t prev_delta = rbegin->second;

    vlog(_logger.trace, "adding offset: {} delta: {}", offset, prev_delta + 1);

    _offset2delta.emplace(offset, prev_delta + 1);
    ++_map_version;
}

bytes offset_translator::offsets_map_key() const {
    return serialize_kvstore_key(_group, kvstore_key_type::offsets_map);
}

bytes offset_translator::highest_known_offset_key() const {
    return serialize_kvstore_key(
      _group, kvstore_key_type::highest_known_offset);
}

ss::future<> offset_translator::maybe_checkpoint() {
    if (_filtered_types.empty()) {
        co_return;
    }

    constexpr size_t checkpoint_threshold = 64_MiB;

    co_await _checkpoint_lock.with([this]() -> ss::future<> {
        if (
          _bytes_processed
          < _bytes_processed_at_checkpoint + checkpoint_threshold) {
            co_return;
        }

        vlog(
          _logger.trace,
          "threshold reached, performing checkpoint; base offset/delta: {}/{}, "
          "map size: {}, highest_known_offset: {}",
          _offset2delta.begin()->first,
          _offset2delta.begin()->second,
          _offset2delta.size(),
          _highest_known_offset);

        co_await do_checkpoint();
    });
}

ss::future<> offset_translator::do_checkpoint() {
    // Read state in a single continuation to get a consistent snapshot.

    size_t bytes_processed = _bytes_processed;
    size_t map_version = _map_version;

    std::optional<iobuf> map_buf;
    if (map_version > _map_version_at_checkpoint) {
        map_buf.emplace(serialize_offsets_map(_offset2delta));
    }

    iobuf hko_buf = reflection::to_iobuf(_highest_known_offset);

    // Persisting offsets map before highest offset so that if the latter
    // fails, we are still left with a consistent state (map can be
    // recreated by reading log from the highest known offset).

    if (map_buf) {
        co_await _storage_api.kvs().put(
          storage::kvstore::key_space::offset_translator,
          offsets_map_key(),
          std::move(*map_buf));
        _map_version_at_checkpoint = map_version;
    }

    co_await _storage_api.kvs().put(
      storage::kvstore::key_space::offset_translator,
      highest_known_offset_key(),
      std::move(hko_buf));
    _bytes_processed_at_checkpoint = bytes_processed;
}

} // namespace raft
