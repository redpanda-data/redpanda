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

#include "storage/offset_translator_state.h"

#include "model/fundamental.h"
#include "storage/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <iterator>

namespace storage {

int64_t offset_translator_state::delta(model::offset o) const {
    if (_last_offset2batch.empty()) {
        return 0;
    }

    auto it = _last_offset2batch.lower_bound(o);
    if (it == _last_offset2batch.begin()) {
        // We don't have enough information to calculate delta if we've ended up
        // here (even if we have an entry with the key o in the
        // _last_offset2batch map). The reason is that the first entry of the
        // map doesn't represent a real non-data batch, but rather an
        // amalgamation of all non-data batches prior to the start of the
        // translation range that is needed to save the delta at the log start.
        //
        // One common way to get this error is when the client code tries to
        // translate the end offset of an empty log (which is by convention
        // prev(start_offset) if start_offset >= 0, and therefore lies outside
        // the translation range). In this case the client code should detect
        // that the offset range is empty and manually set the end of the
        // translated range to prev(translated(start_offset)).

        throw std::runtime_error{fmt::format(
          "ntp {}: log offset {} is outside the translation range (starting at "
          "{})",
          _ntp,
          o,
          model::next_offset(_last_offset2batch.begin()->first))};
    }

    auto delta = std::prev(it)->second.next_delta;
    if (it == _last_offset2batch.end() || o < it->second.base_offset) {
        // This is the common case: offset o is the offset of a record in a data
        // batch between non-data batches pointed to by iterators `it` and
        // `std::prev(it)` (or, if `it` is the map end, o is beyond the last
        // non-data batch in the log). Delta that we need is stored in the map
        // element pointed to by `std::prev(it)`.
        return delta;
    } else {
        // The offset is inside the non-data batch, so the data offset stops
        // increasing at the base offset. This means that for all records in the
        // gap, data offset is equal to the data offset of the *next* data
        // record. A heuristic to recall this rule: suppose the record at log
        // (redpanda) offset 0 is a config batch. Then its data (kafka) offset
        // must be 0, the same as the data (kafka) offset of the data record at
        // log (redpanda) offset 1.
        return delta + (o - it->second.base_offset);
    }
}
model::offset_delta
offset_translator_state::next_offset_delta(model::offset o) const {
    return model::offset_delta(delta(model::next_offset(o)));
}

model::offset offset_translator_state::from_log_offset(model::offset o) const {
    const auto d = delta(o);
    return model::offset(o - d);
}

model::offset offset_translator_state::to_log_offset(
  model::offset data_offset, model::offset hint) const {
    if (_last_offset2batch.empty()) {
        return data_offset;
    }

    if (data_offset == model::offset::max()) {
        return data_offset;
    }

    model::offset min_log_offset = model::next_offset(
      _last_offset2batch.begin()->first);

    model::offset min_data_offset
      = min_log_offset
        - model::offset(_last_offset2batch.begin()->second.next_delta);
    if (data_offset < min_data_offset) {
        throw std::runtime_error{fmt::format(
          "ntp {}: data offset {} is outside the translation range (starting "
          "at {})",
          _ntp,
          data_offset,
          min_data_offset)};
    }

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
          = model::prev_offset(interval_end_it->second.base_offset)
            - model::offset{delta};
        if (max_do_this_interval >= data_offset) {
            break;
        }

        delta = interval_end_it->second.next_delta;
        ++interval_end_it;
    }

    return data_offset + model::offset(delta);
}

int64_t offset_translator_state::last_delta() const {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    return _last_offset2batch.rbegin()->second.next_delta;
}

model::offset offset_translator_state::last_gap_offset() const {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    return _last_offset2batch.rbegin()->first;
}

void offset_translator_state::add_gap(
  model::offset base_offset, model::offset last_offset) {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    if (last_offset < _last_offset2batch.begin()->first) {
        // The gap is added before the
        vlog(
          stlog.error,
          "ntp {}: can't add gap before the begining of the map, base offset: "
          "{}, last offset: {}, OT start offset: {}",
          _ntp,
          base_offset,
          last_offset,
          _last_offset2batch.begin()->first);
        return;
    }
    auto rbegin = _last_offset2batch.rbegin();
    int64_t length = last_offset() - base_offset() + 1;
    int64_t next_delta = rbegin->second.next_delta + length;

    if (base_offset <= rbegin->first) {
        auto it = _last_offset2batch.find(last_offset);
        if (
          it == _last_offset2batch.end()
          || it->second.base_offset != base_offset) {
            // If the gap is added second time it should match
            // the existing one.
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "ntp {}: inconsistent add_gap: {}-{}, next gap offset: {}, next "
              "gap delta: {}",
              _ntp,
              base_offset,
              last_offset,
              rbegin->second.base_offset,
              rbegin->second.next_delta));
        }
        return;
    }

    vlog(
      stlog.debug,
      "add_gap: {} - {}, next delta {}",
      base_offset,
      last_offset,
      next_delta);
    _last_offset2batch.emplace(
      last_offset,
      batch_info{.base_offset = base_offset, .next_delta = next_delta});
}

bool offset_translator_state::add_absolute_delta(
  model::offset offset, int64_t delta) {
    vassert(
      delta <= offset(),
      "ntp {}: inconsistent add_absolute_delta: delta {} can't be > offset "
      "{}",
      _ntp,
      delta,
      offset);

    // Remove all overlapping elements
    auto gap_end = model::prev_offset(offset);
    auto gap_length = delta;
    auto it = _last_offset2batch.upper_bound(gap_end);
    // Add new element if empty or delta is different
    model::offset gap_begin = offset - model::offset(delta);
    if (it != _last_offset2batch.begin()) {
        auto back_it = std::prev(it);
        gap_length -= back_it->second.next_delta;
        gap_begin = offset - model::offset(gap_length);
        if (gap_length < 0) {
            // gap is inconsistent and will overlap with the previous
            // one
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "ntp {}: inconsistent add_absolute_delta (offset {}, delta {}), "
              "but last_offset: {}, last_delta: {}, last_base_offset: {}, "
              "gap_length: {}",
              _ntp,
              offset,
              delta,
              back_it->first,
              back_it->second.next_delta,
              back_it->second.base_offset,
              gap_length));
        }
    }
    _last_offset2batch.erase(it, _last_offset2batch.end());
    if (gap_length > 0 || _last_offset2batch.empty()) {
        _last_offset2batch.emplace(
          gap_end, batch_info{.base_offset = gap_begin, .next_delta = delta});
        return true;
    }
    return false;
}

void offset_translator_state::reset() { _last_offset2batch.clear(); }

bool offset_translator_state::truncate(model::offset offset) {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    auto it = _last_offset2batch.lower_bound(offset);
    if (it == _last_offset2batch.begin()) {
        throw std::runtime_error{fmt::format(
          "ntp {}: trying to truncate offset_translator at offset {} which "
          "is "
          "<= base translation offset {}",
          _ntp,
          offset,
          _last_offset2batch.begin()->first)};
    }

    if (it != _last_offset2batch.end()) {
        if (offset > it->second.base_offset) {
            throw std::runtime_error{fmt::format(
              "ntp {}: trying to truncate offset_translator at offset {} "
              "which "
              "is in the middle of the batch [{},{}]",
              _ntp,
              offset,
              it->second.base_offset,
              it->first)};
        }

        _last_offset2batch.erase(it, _last_offset2batch.end());
        return true;
    }

    return false;
}

bool offset_translator_state::prefix_truncate(model::offset offset) {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    auto it = _last_offset2batch.upper_bound(offset);
    if (it != _last_offset2batch.end() && offset >= it->second.base_offset) {
        throw std::runtime_error{fmt::format(
          "ntp {}: trying to prefix truncate offset translator at offset "
          "{} "
          "which is in the middle of the batch {}-{}",
          _ntp,
          offset,
          it->second.base_offset,
          it->first)};
    }

    if (it == _last_offset2batch.begin()) {
        return false;
    }

    auto prev_it = std::prev(it);
    if (prev_it == _last_offset2batch.begin() && prev_it->first == offset) {
        return false;
    }

    auto base_batch = prev_it->second;
    base_batch.base_offset = offset;
    _last_offset2batch.erase(_last_offset2batch.begin(), it);
    _last_offset2batch.emplace(offset, base_batch);
    return true;
}

namespace {

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

} // namespace

iobuf offset_translator_state::serialize_map() const {
    vassert(
      !_last_offset2batch.empty(),
      "ntp {}: offsets map shouldn't be empty",
      _ntp);

    std::vector<persisted_batch> batches;
    batches.reserve(_last_offset2batch.size());
    for (const auto& [o, b] : _last_offset2batch) {
        int32_t length = int32_t(o - b.base_offset) + 1;
        batches.push_back(
          persisted_batch{.base_offset = b.base_offset, .length = length});
    }

    persisted_batches_map persisted{
      .start_delta = _last_offset2batch.begin()->second.next_delta,
      .batches = std::move(batches),
    };

    return serde::to_iobuf(std::move(persisted));
}

offset_translator_state
offset_translator_state::from_serialized_map(model::ntp ntp, iobuf buf) {
    auto persisted = serde::from_iobuf<persisted_batches_map>(std::move(buf));
    if (persisted.batches.empty()) {
        throw std::runtime_error{fmt::format(
          "ntp {}: persisted offset translator map shouldn't be empty", ntp)};
    }

    absl::btree_map<model::offset, batch_info> last_offset2batch;
    int64_t cur_delta = persisted.start_delta;
    model::offset prev_last_offset;
    for (auto it = persisted.batches.begin(); it != persisted.batches.end();
         ++it) {
        const persisted_batch& b = *it;
        if (it != persisted.batches.begin()) {
            if (b.base_offset <= prev_last_offset) {
                throw std::runtime_error{fmt::format(
                  "ntp {}: inconsistency in serialized offset translator "
                  "state: offset {} is after {}",
                  ntp,
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

    offset_translator_state state(std::move(ntp));
    state._last_offset2batch = std::move(last_offset2batch);
    return state;
}

offset_translator_state offset_translator_state::from_bootstrap_state(
  model::ntp ntp, const absl::btree_map<model::offset, int64_t>& offset2delta) {
    offset_translator_state state(std::move(ntp));
    for (const auto& [o, d] : offset2delta) {
        state._last_offset2batch.emplace(
          o, batch_info{.base_offset = o, .next_delta = d});
    }
    return state;
}

std::ostream&
operator<<(std::ostream& os, const offset_translator_state& state) {
    const auto& map = state._last_offset2batch;

    if (map.empty()) {
        return os << "{empty}";
    }

    return os << "{base offset/delta: " << map.begin()->first << "/"
              << map.begin()->second.next_delta << ", map size: " << map.size()
              << ", last delta: " << map.rbegin()->second.next_delta << "}";
}

} // namespace storage
