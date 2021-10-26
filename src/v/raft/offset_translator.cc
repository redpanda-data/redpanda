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

#include "raft/consensus_utils.h"
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
  , _ntp(std::move(ntp))
  , _logger(logger, ssx::sformat("ntp: {}", _ntp))
  , _storage_api(storage_api) {}

int64_t offset_translator::delta(model::offset o) const {
    if (_filtered_types.empty()) {
        return 0;
    }

    auto it = _last_offset2batch.lower_bound(o);
    if (it == _last_offset2batch.begin()) {
        throw std::runtime_error{_logger.format(
          "log offset {} is outside the translation range (starting at {})",
          o,
          details::next_offset(_last_offset2batch.begin()->first))};
    }

    auto delta = std::prev(it)->second.next_delta;
    if (it == _last_offset2batch.end() || o < it->second.base_offset) {
        return delta;
    } else {
        // The offset is inside the non-data batch, so the data offset stops
        // increasing at the base offset.
        return delta + (o - it->second.base_offset);
    }
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
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    model::offset min_log_offset = details::next_offset(
      _last_offset2batch.begin()->first);

    model::offset min_data_offset
      = min_log_offset
        - model::offset(_last_offset2batch.begin()->second.next_delta);
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
    auto interval_end_it = _last_offset2batch.lower_bound(search_start);
    vassert(
      interval_end_it != _last_offset2batch.begin(),
      "ntp {}: log offset search start too small: {}",
      search_start);
    auto delta = std::prev(interval_end_it)->second.next_delta;

    while (interval_end_it != _last_offset2batch.end()) {
        model::offset max_do_this_interval
          = details::prev_offset(interval_end_it->second.base_offset)
            - model::offset{delta};
        if (max_do_this_interval >= data_offset) {
            break;
        }

        delta = interval_end_it->second.next_delta;
        ++interval_end_it;
    }

    return data_offset + model::offset(delta);
}

void offset_translator::process(const model::record_batch& batch) {
    if (_filtered_types.empty()) {
        return;
    }

    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    _bytes_processed += batch.size_bytes();

    if (
      std::find(
        _filtered_types.begin(), _filtered_types.end(), batch.header().type)
      != _filtered_types.end()) {
        auto rbegin = _last_offset2batch.rbegin();
        vassert(
          batch.base_offset() > rbegin->first,
          "ntp {}: trying to add batch to offset translator at offset {} that "
          "is not higher than the previous last offset {}",
          _ntp,
          batch.base_offset(),
          rbegin->first);

        int32_t length = batch.header().last_offset_delta + 1;
        int64_t next_delta = rbegin->second.next_delta + length;

        vlog(
          _logger.trace,
          "adding batch, offsets: [{},{}], delta: {}",
          batch.base_offset(),
          batch.last_offset(),
          next_delta);

        _last_offset2batch.emplace(
          batch.last_offset(),
          offset_translator::batch_info{
            .base_offset = batch.base_offset(), .next_delta = next_delta});
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

struct persisted_batch {
    model::offset base_offset;
    int32_t length;

    friend inline void read_nested(
      iobuf_parser& in, persisted_batch& b, size_t const bytes_left_limit) {
        serde::read_nested(in, b.base_offset, bytes_left_limit);
        serde::read_nested(in, b.length, bytes_left_limit);
    }

    friend inline void write(iobuf& out, const persisted_batch& b) {
        serde::write(out, b.base_offset);
        serde::write(out, b.length);
    }
};

struct persisted_batches_map
  : serde::envelope<
      persisted_batches_map,
      serde::version<0>,
      serde::compat_version<0>> {
    int64_t start_delta = 0;
    std::vector<persisted_batch> batches;
};

bytes serialize_kvstore_key(raft::group_id group, kvstore_key_type key_type) {
    iobuf buf;
    reflection::serialize(buf, key_type, group);
    return iobuf_to_bytes(buf);
}

} // namespace

iobuf offset_translator::serialize_batches_map(
  const batches_map_t& last_offset2batch) {
    vassert(
      !last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    std::vector<persisted_batch> batches;
    batches.reserve(last_offset2batch.size());
    for (const auto& [o, b] : last_offset2batch) {
        int32_t length = int32_t(o - b.base_offset) + 1;
        batches.push_back(
          persisted_batch{.base_offset = b.base_offset, .length = length});
    }

    persisted_batches_map persisted{
      .start_delta = last_offset2batch.begin()->second.next_delta,
      .batches = std::move(batches),
    };

    return serde::to_iobuf(std::move(persisted));
}

offset_translator::batches_map_t
offset_translator::deserialize_batches_map(iobuf buf) {
    auto persisted = serde::from_iobuf<persisted_batches_map>(std::move(buf));
    if (persisted.batches.empty()) {
        throw std::runtime_error{
          _logger.format("persisted offset translator map shouldn't be empty")};
    }

    absl::btree_map<model::offset, batch_info> last_offset2batch;
    int64_t cur_delta = persisted.start_delta;
    model::offset prev_last_offset;
    for (auto it = persisted.batches.begin(); it != persisted.batches.end();
         ++it) {
        const persisted_batch& b = *it;
        if (it != persisted.batches.begin()) {
            if (b.base_offset <= prev_last_offset) {
                throw std::runtime_error{_logger.format(
                  "inconsistency in serialized offset translator state: offset "
                  "{} is after {}",
                  b.base_offset,
                  prev_last_offset)};
            }
            cur_delta += b.length;
        }

        model::offset last_offset = b.base_offset + model::offset{b.length - 1};
        last_offset2batch.emplace(
          last_offset,
          batch_info{.base_offset = b.base_offset, .next_delta = cur_delta});
        prev_last_offset = last_offset;
    }

    return last_offset2batch;
}

ss::future<>
offset_translator::start(must_reset reset, bootstrap_state&& bootstrap) {
    vassert(
      _last_offset2batch.empty(),
      "ntp {}: offset_translator was modified before start()",
      _ntp);

    if (_filtered_types.empty()) {
        co_return;
    }

    if (reset) {
        vlog(_logger.info, "resetting offset translation state");

        _last_offset2batch.emplace(
          model::offset::min(),
          batch_info{.base_offset = model::offset::min(), .next_delta = 0});
        ++_map_version;
        _highest_known_offset = model::offset::min();

        co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
    } else {
        auto map_buf = _storage_api.kvs().get(
          storage::kvstore::key_space::offset_translator, offsets_map_key());
        auto highest_known_offset_buf = _storage_api.kvs().get(
          storage::kvstore::key_space::offset_translator,
          highest_known_offset_key());

        if (map_buf && highest_known_offset_buf) {
            _last_offset2batch = deserialize_batches_map(std::move(*map_buf));
            _highest_known_offset = reflection::from_iobuf<model::offset>(
              std::move(*highest_known_offset_buf));

            // highest known offset could be more stale than the map, in
            // this case we take it from the map
            _highest_known_offset = std::max(
              _highest_known_offset, _last_offset2batch.rbegin()->first);
        } else {
            // For backwards compatibility: load state from
            // configuration_manager state
            vlog(
              _logger.info,
              "offset translation kvstore state not found, loading from "
              "provided bootstrap state");

            for (const auto& [o, d] : bootstrap.offset2delta) {
                _last_offset2batch.emplace(
                  o, batch_info{.base_offset = o, .next_delta = d});
            }
            ++_map_version;
            _highest_known_offset = bootstrap.highest_known_offset;

            co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
        }
    }

    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    vlog(
      _logger.info,
      "started, base offset/delta: {}/{}, map size: {}, last delta: {}, "
      "highest_known_offset: {}",
      _last_offset2batch.begin()->first,
      _last_offset2batch.begin()->second.next_delta,
      _last_offset2batch.size(),
      _last_offset2batch.rbegin()->second.next_delta,
      _highest_known_offset);
}

ss::future<> offset_translator::sync_with_log(
  storage::log log, storage::opt_abort_source_t as) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    auto log_offsets = log.offsets();

    // Trim the offset2delta map to log dirty_offset (discrepancy can
    // happen if the offsets map was persisted, but the log wasn't flushed).
    auto end_it = _last_offset2batch.upper_bound(log_offsets.dirty_offset);
    if (end_it == _last_offset2batch.begin()) {
        throw std::runtime_error{_logger.format(
          "can't sync offset_translator with log: dirty offset {} is < "
          "base translation offset {}",
          log_offsets.dirty_offset,
          _last_offset2batch.begin()->first)};
    }

    if (end_it != _last_offset2batch.end()) {
        _last_offset2batch.erase(end_it, _last_offset2batch.end());
        ++_map_version;
    }

    if (log_offsets.dirty_offset < _highest_known_offset) {
        _highest_known_offset = log_offsets.dirty_offset;
        co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
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
      "synced with log, base offset/delta: {}/{}, map size: {}, last delta: "
      "{}, highest_known_offset: {}",
      _last_offset2batch.begin()->first,
      _last_offset2batch.begin()->second.next_delta,
      _last_offset2batch.size(),
      _last_offset2batch.rbegin()->second.next_delta,
      _highest_known_offset);

    co_await maybe_checkpoint();
}

ss::future<> offset_translator::truncate(model::offset offset) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    auto it = _last_offset2batch.lower_bound(offset);
    if (it == _last_offset2batch.begin()) {
        throw std::runtime_error{_logger.format(
          "trying to truncate offset_translator at offset {} which "
          "is <= base translation offset {}",
          offset,
          _last_offset2batch.begin()->first)};
    }

    if (it != _last_offset2batch.end()) {
        if (offset > it->second.base_offset) {
            throw std::runtime_error{_logger.format(
              "trying to truncate offset_translator at offset {} which "
              "is in the middle of the batch [{},{}]",
              offset,
              it->second.base_offset,
              it->first)};
        }

        _last_offset2batch.erase(it, _last_offset2batch.end());
        ++_map_version;
    }

    model::offset prev = details::prev_offset(offset);
    _highest_known_offset = std::min(prev, _highest_known_offset);

    vlog(
      _logger.info,
      "truncate at offset: {}, new map size: {}, new last delta: {}",
      offset,
      _last_offset2batch.size(),
      _last_offset2batch.rbegin()->second.next_delta);

    co_await _checkpoint_lock.with([this] { return do_checkpoint(); });
}

ss::future<> offset_translator::prefix_truncate(model::offset offset) {
    if (_filtered_types.empty()) {
        co_return;
    }

    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    if (offset > _highest_known_offset) {
        throw std::runtime_error{_logger.format(
          "trying to prefix truncate offset translator at offset {} which "
          "is > highest_known_offset {}",
          offset,
          _highest_known_offset)};
    }

    auto it = _last_offset2batch.upper_bound(offset);
    if (it != _last_offset2batch.end() && offset >= it->second.base_offset) {
        throw std::runtime_error{_logger.format(
          "trying to prefix truncate offset translator at offset {} which "
          "is in the middle of the batch {}-{}",
          offset,
          it->second.base_offset,
          it->first)};
    }

    if (it == _last_offset2batch.begin()) {
        co_return;
    }

    auto prev_it = std::prev(it);
    if (prev_it == _last_offset2batch.begin() && prev_it->first == offset) {
        co_return;
    }

    auto base_batch = prev_it->second;
    _last_offset2batch.erase(_last_offset2batch.begin(), it);
    _last_offset2batch.emplace(offset, base_batch);
    ++_map_version;

    vlog(
      _logger.info,
      "prefix_truncate at offset: {}, new base delta: {}, new map size: {}, "
      "new last delta: {}",
      offset,
      base_batch.next_delta,
      _last_offset2batch.size(),
      _last_offset2batch.rbegin()->second.next_delta);

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

    vassert(
      delta >= 0,
      "not enough state to recover offset translator. Requested to reset "
      "at offset {}. Translator highest_known offset {}, last delta: {}, base "
      "offset: {}, base delta: {}",
      offset,
      _highest_known_offset,
      _last_offset2batch.begin()->second.base_offset,
      _last_offset2batch.begin()->second.next_delta,
      _last_offset2batch.rbegin()->second.next_delta);

    _last_offset2batch.clear();
    _last_offset2batch.emplace(
      offset, batch_info{.base_offset = offset, .next_delta = delta});
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
          "map size: {}, last delta: {}, highest_known_offset: {}",
          _last_offset2batch.begin()->first,
          _last_offset2batch.begin()->second.next_delta,
          _last_offset2batch.size(),
          _last_offset2batch.rbegin()->second.next_delta,
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
        map_buf.emplace(serialize_batches_map(_last_offset2batch));
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
