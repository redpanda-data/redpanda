// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/index_state.h"

#include "base/unreachable.h"
#include "base/vassert.h"
#include "bytes/iobuf_parser.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "hashing/crc32c.h"
#include "hashing/xx.h"
#include "reflection/adl.h"
#include "serde/peek.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"
#include "serde/serde_exception.h"
#include "storage/logger.h"
#include "utils/to_string.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <exception>
#include <optional>

namespace storage {

bool operator==(
  const std::unique_ptr<index_columns_base>& lhs,
  const std::unique_ptr<index_columns_base>& rhs) {
    // This comparison operator is used only in tests.
    // Since we're comparing objects using pointers to base abstract
    // class we don't have direct access to columns. The naive way
    // to compare them is to use `get_element` method iteratively.
    // But this will lead to poor performance with column store and
    // long test runs.
    incremental_xxhash64 lhs_xx{};
    incremental_xxhash64 rhs_xx{};
    lhs->checksum(lhs_xx);
    rhs->checksum(rhs_xx);
    return lhs_xx.digest() == rhs_xx.digest();
}

uint32_t
compressed_index_columns::get_relative_offset_index(int ix) const noexcept {
    // This is relatively inefficient (at least compared to array access by
    // index) but it is possible to optimize this by using iterators instead of
    // indexes.
    auto res = _relative_offset_index.at_index(ix);
    vassert(
      res != _relative_offset_index.end(), "Index lookup failed at {}", ix);
    return static_cast<uint32_t>(*res);
}

uint32_t
compressed_index_columns::get_relative_time_index(int ix) const noexcept {
    auto res = _relative_time_index.at_index(ix);
    vassert(res != _relative_time_index.end(), "Index lookup failed at {}", ix);
    return static_cast<uint32_t>(*res);
}

uint64_t compressed_index_columns::get_position_index(int ix) const noexcept {
    auto res = _position_index.at_index(ix);
    vassert(res != _position_index.end(), "Index lookup failed at {}", ix);
    return *res;
}

std::optional<int>
compressed_index_columns::offset_lower_bound(uint32_t needle) const noexcept {
    assert_column_sizes();
    auto it = _relative_offset_index.lower_bound(needle);
    if (it != _relative_offset_index.end()) {
        return it.index();
    }
    return std::nullopt;
}

std::optional<int>
compressed_index_columns::position_upper_bound(uint64_t needle) const noexcept {
    assert_column_sizes();
    auto it = _position_index.upper_bound(needle);
    if (it != _position_index.end()) {
        return it.index();
    }
    return std::nullopt;
}

std::optional<int>
compressed_index_columns::time_lower_bound(uint32_t needle) const noexcept {
    assert_column_sizes();
    auto it = _relative_time_index.lower_bound(needle);
    if (it != _relative_time_index.end()) {
        return it.index();
    }
    return std::nullopt;
}

bool compressed_index_columns::try_reset_relative_time_index(
  chunked_vector<uint32_t> input) {
    if (_relative_time_index.size() != input.size() || input.empty()) {
        return false;
    }
    _relative_time_index = {};
    for (auto t : input) {
        _relative_time_index.append(t);
    }
    return true;
}

size_t compressed_index_columns::size() const noexcept {
    assert_column_sizes();
    return _relative_offset_index.size();
}

chunked_vector<uint32_t>
compressed_index_columns::copy_relative_offset_index() const noexcept {
    chunked_vector<uint32_t> res;
    res.reserve(size());
    for (const auto r : _relative_offset_index) {
        res.push_back(static_cast<uint32_t>(r));
    }
    return res;
}

chunked_vector<uint32_t>
compressed_index_columns::copy_relative_time_index() const noexcept {
    chunked_vector<uint32_t> res;
    res.reserve(size());
    for (const auto r : _relative_time_index) {
        res.push_back(static_cast<uint32_t>(r));
    }
    return res;
}

chunked_vector<uint64_t>
compressed_index_columns::copy_position_index() const noexcept {
    chunked_vector<uint64_t> res;
    res.reserve(size());
    for (const auto r : _position_index) {
        res.push_back(r);
    }
    return res;
}

void compressed_index_columns::write(iobuf& buf) const {
    serde::write(buf, copy_relative_offset_index());
    serde::write(buf, copy_relative_time_index());
    serde::write(buf, copy_position_index());
}

namespace {
struct pos_difference {
    bool operator()(uint64_t lhs, uint64_t rhs) const {
        return (lhs + position_index_step) > rhs;
    }
};
static bool valid_offsets_and_positions(
  const chunked_vector<uint32_t>& offsets,
  const chunked_vector<uint64_t>& positions) {
    // Offsets should be monotonic
    bool monotonic = std::adjacent_find(
                       offsets.begin(), offsets.end(), std::greater<uint32_t>())
                     == offsets.end();
    // 32_KiB step between adjacent file offsets
    bool correct_step = std::adjacent_find(
                          positions.begin(), positions.end(), pos_difference())
                        == positions.end();
    return monotonic && correct_step;
}
} // namespace

std::unique_ptr<index_columns_base>
compressed_index_columns::read_nested(iobuf_parser& p) {
    chunked_vector<uint32_t> offsets;
    chunked_vector<uint32_t> timestamps;
    chunked_vector<uint64_t> positions;
    serde::read_nested(p, offsets, 0U);
    serde::read_nested(p, timestamps, 0U);
    serde::read_nested(p, positions, 0U);
    if (!valid_offsets_and_positions(offsets, positions)) {
        auto fallback = std::make_unique<index_columns>(
          std::move(offsets), std::move(timestamps), std::move(positions));
        return fallback;
    }
    assign_relative_offset_index(std::move(offsets));
    assign_relative_time_index(std::move(timestamps));
    assign_position_index(std::move(positions));
    return nullptr;
}

std::unique_ptr<index_columns_base>
compressed_index_columns::from(iobuf_parser& parser) {
    chunked_vector<uint32_t> offsets;
    chunked_vector<uint32_t> timestamps;
    chunked_vector<uint64_t> positions;

    const uint32_t vsize = ss::le_to_cpu(
      reflection::adl<uint32_t>{}.from(parser));

    offsets.reserve(vsize);
    timestamps.reserve(vsize);
    positions.reserve(vsize);

    for (auto i = 0U; i < vsize; ++i) {
        offsets.push_back(reflection::adl<uint32_t>{}.from(parser));
    }
    for (auto i = 0U; i < vsize; ++i) {
        timestamps.push_back(reflection::adl<uint32_t>{}.from(parser));
    }
    for (auto i = 0U; i < vsize; ++i) {
        positions.push_back(reflection::adl<uint64_t>{}.from(parser));
    }
    if (!valid_offsets_and_positions(offsets, positions)) {
        auto fallback = std::make_unique<index_columns>(
          std::move(offsets), std::move(timestamps), std::move(positions));
        return fallback;
    }
    assign_relative_offset_index(std::move(offsets));
    assign_relative_time_index(std::move(timestamps));
    assign_position_index(std::move(positions));
    return nullptr;
}

void compressed_index_columns::to(iobuf& out) const {
    auto sz = size();
    reflection::adl<uint32_t>{}.to(out, sz);

    for_each_relative_offset_index([&out](uint64_t n) {
        reflection::adl<uint32_t>{}.to(out, static_cast<uint32_t>(n));
    });
    for_each_relative_time_index([&out](uint64_t n) {
        reflection::adl<uint32_t>{}.to(out, static_cast<uint32_t>(n));
    });
    for_each_position_index(
      [&out](uint64_t n) { reflection::adl<uint64_t>{}.to(out, n); });
}

void compressed_index_columns::checksum(incremental_xxhash64& xx) const {
    for_each_relative_offset_index([&](uint32_t o) { xx.update(o); });
    for_each_relative_time_index([&](uint32_t t) { xx.update(t); });
    for_each_position_index([&](uint64_t p) { xx.update(p); });
}

void compressed_index_columns::assign_relative_offset_index(
  chunked_vector<uint32_t> xs) noexcept {
    _relative_offset_index = {};
    for (auto x : xs) {
        _relative_offset_index.append(x);
    }
}

void compressed_index_columns::assign_relative_time_index(
  chunked_vector<uint32_t> xs) noexcept {
    _relative_time_index = {};
    for (auto x : xs) {
        _relative_time_index.append(x);
    }
}

void compressed_index_columns::assign_position_index(
  chunked_vector<uint64_t> xs) noexcept {
    _position_index = {};
    for (auto x : xs) {
        _position_index.append(x);
    }
}

void compressed_index_columns::add_entry(uint32_t o, uint32_t t, uint64_t p) {
    auto ix = _relative_offset_index.size();
    // Updates are transactional to guarantee that we're either add element to
    // all three columns or to non of the columns.
    auto offset_tx = _relative_offset_index.append_tx(o);
    auto time_tx = _relative_time_index.append_tx(t);
    auto pos_tx = _position_index.append_tx(p);

    // Update hints
    static constexpr int sampling_rate = 64;
    if (ix % sampling_rate == 0) {
        auto oh = _relative_offset_index.get_current_stream_pos();
        auto th = _relative_time_index.get_current_stream_pos();
        auto ph = _position_index.get_current_stream_pos();
        if (oh.has_value() && th.has_value() && ph.has_value()) {
            _hints[ix] = std::make_tuple(oh.value(), th.value(), ph.value());
        }
    }

    // The code below is guaranteed not to throw exceptions
    // because 'commit' is 'noexcept'
    if (offset_tx.has_value()) {
        std::move(offset_tx.value()).commit();
    }
    if (time_tx.has_value()) {
        std::move(time_tx.value()).commit();
    }
    if (pos_tx.has_value()) {
        std::move(pos_tx.value()).commit();
    }
    assert_column_sizes();
}

std::tuple<uint32_t, uint32_t, uint64_t>
compressed_index_columns::get_entry(size_t i) const {
    auto it = _hints.lower_bound(i);
    if (it != _hints.end()) {
        vassert(it->first <= i, "Found index {} instead of {}", i, it->first);
        auto [offset_hint, time_hint, pos_hint] = it->second;
        uint32_t offset = *_relative_offset_index.at_index(i, offset_hint);
        uint32_t timestamp = *_relative_time_index.at_index(i, time_hint);
        uint64_t pos = *_position_index.at_index(i, pos_hint);
        return std::make_tuple(offset, timestamp, pos);
    } else {
        uint32_t offset = *_relative_offset_index.at_index(i);
        uint32_t timestamp = *_relative_time_index.at_index(i);
        uint64_t pos = *_position_index.at_index(i);
        return std::make_tuple(offset, timestamp, pos);
    }
}

void compressed_index_columns::pop_back(int n) {
    // The 'pop_back' implementation partially copies all columns
    // but this is OK because 'pop_back' is never invoked in the hot path
    offset_column_t tmp_offsets;
    column_t tmp_timestamps;
    pos_column_t tmp_positions;
    auto expected_size = _relative_offset_index.size();
    if (static_cast<size_t>(n) >= expected_size) {
        // Fast path for full cleanup
        _relative_offset_index = {};
        _relative_time_index = {};
        _position_index = {};
        return;
    } else {
        expected_size -= static_cast<size_t>(n);
    }
    std::for_each_n(
      _relative_offset_index.begin(),
      expected_size,
      [&tmp_offsets](uint64_t o) { tmp_offsets.append(o); });
    std::for_each_n(
      _relative_time_index.begin(),
      expected_size,
      [&tmp_timestamps](uint64_t t) { tmp_timestamps.append(t); });
    std::for_each_n(
      _position_index.begin(), expected_size, [&tmp_positions](uint64_t p) {
          tmp_positions.append(p);
      });
    _relative_offset_index = std::move(tmp_offsets);
    _relative_time_index = std::move(tmp_timestamps);
    _position_index = std::move(tmp_positions);
    assert_column_sizes();
}

void compressed_index_columns::shrink_to_fit() { assert_column_sizes(); }

std::unique_ptr<index_columns_base> compressed_index_columns::copy() const {
    auto res = std::make_unique<compressed_index_columns>();
    for_each_relative_offset_index(
      [&res](uint32_t n) { res->_relative_offset_index.append(n); });
    for_each_relative_time_index(
      [&res](uint32_t n) { res->_relative_time_index.append(n); });
    for_each_position_index(
      [&res](uint64_t n) { res->_position_index.append(n); });
    return res;
}

std::ostream& operator<<(std::ostream& o, const compressed_index_columns& s) {
    fmt::print(
      o,
      "index({}, {}, {})",
      s._relative_offset_index.size(),
      s._relative_time_index.size(),
      s._position_index.size());
    return o;
}

index_columns::index_columns(
  chunked_vector<uint32_t> offsets,
  chunked_vector<uint32_t> timestamps,
  chunked_vector<uint64_t> positions)
  : _relative_offset_index(std::move(offsets))
  , _relative_time_index(std::move(timestamps))
  , _position_index(std::move(positions)) {}

std::optional<int>
index_columns::offset_lower_bound(uint32_t needle) const noexcept {
    auto it = std::lower_bound(
      std::begin(_relative_offset_index),
      std::end(_relative_offset_index),
      needle,
      std::less<uint32_t>());
    if (it == std::end(_relative_offset_index)) {
        return std::nullopt;
    }
    return std::distance(std::begin(_relative_offset_index), it);
}

std::optional<int>
index_columns::position_upper_bound(uint64_t needle) const noexcept {
    auto it = std::upper_bound(
      std::begin(_position_index),
      std::end(_position_index),
      needle,
      std::less<uint64_t>());
    if (it == std::end(_position_index)) {
        return std::nullopt;
    }
    return std::distance(std::begin(_position_index), it);
}

std::optional<int>
index_columns::time_lower_bound(uint32_t needle) const noexcept {
    auto it = std::lower_bound(
      std::begin(_relative_time_index),
      std::end(_relative_time_index),
      needle,
      std::less<uint32_t>());
    if (it == std::end(_relative_time_index)) {
        return std::nullopt;
    }
    return std::distance(std::begin(_relative_time_index), it);
}

bool index_columns::try_reset_relative_time_index(
  chunked_vector<uint32_t> input) {
    if (_relative_time_index.size() != input.size() || input.empty()) {
        return false;
    }
    _relative_time_index = std::move(input);
    return true;
}

void index_columns::write(iobuf& buf) const {
    serde::write(buf, _relative_offset_index.copy());
    serde::write(buf, _relative_time_index.copy());
    serde::write(buf, _position_index.copy());
}

std::unique_ptr<index_columns_base>
index_columns::read_nested(iobuf_parser& p) {
    _relative_offset_index = {};
    _relative_time_index = {};
    _position_index = {};
    serde::read_nested(p, _relative_offset_index, 0U);
    serde::read_nested(p, _relative_time_index, 0U);
    serde::read_nested(p, _position_index, 0U);
    return nullptr;
}

size_t index_columns::size() const noexcept {
    vassert(
      _relative_offset_index.size() == _relative_time_index.size()
        && _relative_offset_index.size() == _position_index.size(),
      "ALL indexes must match in size. {}",
      *this);
    return _relative_offset_index.size();
}

std::unique_ptr<index_columns_base> index_columns::from(iobuf_parser& parser) {
    const uint32_t vsize = ss::le_to_cpu(
      reflection::adl<uint32_t>{}.from(parser));

    _relative_offset_index = {};
    _relative_time_index = {};
    _position_index = {};

    _relative_offset_index.reserve(vsize);
    _relative_time_index.reserve(vsize);
    _position_index.reserve(vsize);

    for (auto i = 0U; i < vsize; ++i) {
        _relative_offset_index.push_back(
          reflection::adl<uint32_t>{}.from(parser));
    }
    for (auto i = 0U; i < vsize; ++i) {
        _relative_time_index.push_back(
          reflection::adl<uint32_t>{}.from(parser));
    }
    for (auto i = 0U; i < vsize; ++i) {
        _position_index.push_back(reflection::adl<uint64_t>{}.from(parser));
    }
    return nullptr;
}

void index_columns::to(iobuf& out) const {
    auto sz = size();
    reflection::adl<uint32_t>{}.to(out, sz);

    for (const auto n : _relative_offset_index) {
        reflection::adl<uint32_t>{}.to(out, static_cast<uint32_t>(n));
    }
    for (const auto n : _relative_time_index) {
        reflection::adl<uint32_t>{}.to(out, static_cast<uint32_t>(n));
    }
    for (const auto n : _position_index) {
        reflection::adl<uint64_t>{}.to(out, n);
    }
}

void index_columns::checksum(incremental_xxhash64& xx) const {
    for (const auto o : _relative_offset_index) {
        xx.update(o);
    }
    for (const auto t : _relative_time_index) {
        xx.update(t);
    }
    for (const auto p : _position_index) {
        xx.update(p);
    }
}

void index_columns::assign_relative_offset_index(
  chunked_vector<uint32_t> xs) noexcept {
    _relative_offset_index = std::move(xs);
}
void index_columns::assign_relative_time_index(
  chunked_vector<uint32_t> xs) noexcept {
    _relative_time_index = std::move(xs);
}
void index_columns::assign_position_index(
  chunked_vector<uint64_t> xs) noexcept {
    _position_index = std::move(xs);
}

void index_columns::add_entry(
  uint32_t relative_offset, uint32_t relative_time, uint64_t pos) {
    _relative_offset_index.push_back(relative_offset);
    _relative_time_index.push_back(relative_time);
    _position_index.push_back(pos);
}

std::tuple<uint32_t, uint32_t, uint64_t>
index_columns::get_entry(size_t i) const {
    uint32_t offset = _relative_offset_index.at(i);
    uint32_t timestamp = _relative_time_index.at(i);
    uint64_t pos = _position_index.at(i);
    return std::make_tuple(offset, timestamp, pos);
}

void index_columns::shrink_to_fit() {
    vassert(
      _relative_offset_index.size() == _relative_time_index.size()
        && _relative_offset_index.size() == _position_index.size(),
      "ALL indexes must match in size. {}",
      *this);
    _relative_offset_index.shrink_to_fit();
    _relative_time_index.shrink_to_fit();
    _position_index.shrink_to_fit();
}

std::unique_ptr<index_columns_base> index_columns::copy() const {
    vassert(
      _relative_offset_index.size() == _relative_time_index.size()
        && _relative_offset_index.size() == _position_index.size(),
      "ALL indexes must match in size. {}",
      *this);
    auto c = std::make_unique<index_columns>();
    c->assign_relative_offset_index(_relative_offset_index.copy());
    c->assign_relative_time_index(_relative_time_index.copy());
    c->assign_position_index(_position_index.copy());
    return c;
}

void index_columns::pop_back(int n) {
    _relative_offset_index.pop_back_n(n);
    _relative_time_index.pop_back_n(n);
    _position_index.pop_back_n(n);
}

std::ostream& operator<<(std::ostream& o, const index_columns& s) {
    fmt::print(
      o,
      "index({}, {}, {})",
      s._relative_offset_index.size(),
      s._relative_time_index.size(),
      s._position_index.size());
    return o;
}

offset_time_index::offset_time_index(
  model::timestamp ts, offset_delta_time with_offset)
  : _with_offset(with_offset) {
    if (_with_offset == offset_delta_time::yes) {
        _val = static_cast<uint32_t>(
          std::clamp(ts(), delta_time_min, delta_time_max) + offset);
    } else {
        _val = _val = static_cast<uint32_t>(std::clamp(
          ts(),
          model::timestamp::type{std::numeric_limits<uint32_t>::min()},
          model::timestamp::type{std::numeric_limits<uint32_t>::max()}));
    }
}

uint32_t offset_time_index::operator()() const {
    if (_with_offset == offset_delta_time::yes) {
        return _val - static_cast<uint32_t>(offset);
    } else {
        return _val;
    }
}

offset_time_index::offset_time_index(
  uint32_t val, offset_delta_time with_offset)
  : _with_offset(with_offset)
  , _val(val) {}

uint32_t offset_time_index::raw_value() const { return _val; }

index_state index_state::make_empty_index(offset_delta_time with_offset) {
    index_state idx{};
    idx.with_offset = with_offset;

    return idx;
}

index_state::index_state() {
    if (config::shard_local_cfg().log_segment_index_compression.value()) {
        index = std::make_unique<compressed_index_columns>();
    } else {
        index = std::make_unique<index_columns>();
    }
}

std::ostream& operator<<(std::ostream& o, const index_state::entry& e) {
    return o << "{offset:" << e.offset << ", time:" << e.timestamp
             << ", filepos:" << e.filepos << "}";
}

bool index_state::maybe_index(
  size_t accumulator,
  size_t step,
  size_t starting_position_in_file,
  model::offset batch_base_offset,
  model::offset batch_max_offset,
  model::timestamp first_timestamp,
  model::timestamp last_timestamp,
  std::optional<model::timestamp> new_broker_timestamp,
  bool user_data,
  size_t compactible_records) {
    vassert(
      batch_base_offset >= base_offset,
      "cannot track offsets that are lower than our base, o:{}, "
      "_state.base_offset:{} - index: {}",
      batch_base_offset,
      base_offset,
      *this);

    bool retval = false;

    // The first non-config batch in the segment, use its timestamp
    // to override the timestamps of any config batch that was indexed
    // by virtue of being the first in the segment.
    if (user_data && non_data_timestamps) {
        auto time_col_reset = index->try_reset_relative_time_index(
          {offset_time_index{last_timestamp, with_offset}.raw_value()});
        // We can only add a non-data timestamp to the empty index. This
        // is why we can assume that the index size is 1. The
        // 'try_reset_relative_time_index' will return true if this is the
        // case. Otherwise it will be impossible to to reset the non-data
        // timestamp.
        vassert(
          time_col_reset,
          "Relative time index can not be reset, unexpected index size {} "
          "(expected 1). This can only happen if more than one non-data "
          "timestamp was added to the index.",
          index->size());

        base_timestamp = first_timestamp;
        max_timestamp = first_timestamp;
        non_data_timestamps = false;
    }

    // index_state
    if (empty()) {
        // Ordinarily, we do not allow configuration batches to contribute
        // to the segment's timestamp bounds (because config batches use
        // walltime but user data timestamps may be anything).  However, for
        // the first batch we set the timestamps, and then set a
        // `non_data_timestamps` flag so that the next time we see user data
        // we will overwrite the walltime timestamps with the user data
        // timestamps.
        non_data_timestamps = !user_data;

        base_timestamp = first_timestamp;
        max_timestamp = first_timestamp;
        retval = true;
    }

    // NOTE: we don't need the 'max()' trick below because we controll the
    // offsets ourselves and it would be a bug otherwise - see assert above
    max_offset = batch_max_offset;

    // Do not allow config batches to contribute to segment timestamp
    // bounds, because their timestamps may differ wildly from user-provided
    // timestamps
    if (user_data) {
        // some clients leave max timestamp uninitialized in cases there is
        // a single record in a batch in this case we use first timestamp as
        // a last one
        last_timestamp = std::max(first_timestamp, last_timestamp);
        max_timestamp = std::max(max_timestamp, last_timestamp);
        if (new_broker_timestamp.has_value()) {
            broker_timestamp = *new_broker_timestamp;
        }
    }
    if (compactible_records > 0) {
        num_compactible_records_appended
          = num_compactible_records_appended.value_or(0) + compactible_records;
    }
    // always saving the first batch simplifies a lot of book keeping
    if ((accumulator >= step && user_data) || retval) {
        add_entry(
          // We know that a segment cannot be > 4GB
          batch_base_offset() - base_offset(),
          offset_time_index{last_timestamp - base_timestamp, with_offset},
          starting_position_in_file);

        retval = true;
    }
    return retval;
}

std::ostream& operator<<(std::ostream& o, const index_state& s) {
    return o << "{header_bitflags:" << s.bitflags
             << ", base_offset:" << s.base_offset
             << ", max_offset:" << s.max_offset
             << ", base_timestamp:" << s.base_timestamp
             << ", max_timestamp:" << s.max_timestamp
             << ", batch_timestamps_are_monotonic:"
             << s.batch_timestamps_are_monotonic
             << ", with_offset:" << s.with_offset
             << ", non_data_timestamps:" << s.non_data_timestamps
             << ", broker_timestamp:" << s.broker_timestamp
             << ", num_compactible_records_appended:"
             << s.num_compactible_records_appended
             << ", clean_compact_timestamp:" << s.clean_compact_timestamp
             << ", may_have_tombstone_records:" << s.may_have_tombstone_records
             << ", " << s.index << "}";
}

void index_state::serde_write(iobuf& out) const {
    using serde::write;

    iobuf tmp;
    write(tmp, bitflags);
    write(tmp, base_offset);
    write(tmp, max_offset);
    write(tmp, base_timestamp);
    write(tmp, max_timestamp);

    index->write(tmp);

    write(tmp, batch_timestamps_are_monotonic);
    write(tmp, with_offset);
    write(tmp, non_data_timestamps);
    write(tmp, broker_timestamp);
    write(tmp, num_compactible_records_appended);
    write(tmp, clean_compact_timestamp);
    write(tmp, may_have_tombstone_records);

    crc::crc32c crc;
    crc_extend_iobuf(crc, tmp);
    const uint32_t tmp_crc = crc.value();

    // data blob + crc
    write(out, std::move(tmp));
    write(out, tmp_crc);
}

void read_nested(
  iobuf_parser& in, index_state& st, const size_t bytes_left_limit) {
    /*
     * peek at the 1-byte version prefix. this will either correspond to a
     * version from the deprecated format, or a version in the range
     * supported by the serde format.
     */
    const auto compat_version = serde::peek_version(in);

    /*
     * supported old version to avoid rebuilding all indices.
     */
    if (compat_version == serde_compat::index_state_serde::ondisk_version) {
        in.skip(sizeof(int8_t));
        st = serde_compat::index_state_serde::decode(in);
        st.batch_timestamps_are_monotonic = false;
        return;
    }

    /*
     * unsupported old version.
     */
    if (compat_version < serde_compat::index_state_serde::ondisk_version) {
        throw serde::serde_exception(
          fmt_with_ctx(fmt::format, "Unsupported version: {}", compat_version));
    }

    /*
     * support for new serde format.
     */
    const auto hdr = serde::read_header<index_state>(in, bytes_left_limit);

    using serde::read_nested;

    // data blog + crc
    iobuf tmp;
    uint32_t tmp_crc = 0;
    read_nested(in, tmp, hdr._bytes_left_limit);
    read_nested(in, tmp_crc, hdr._bytes_left_limit);

    crc::crc32c crc;
    crc_extend_iobuf(crc, tmp);
    const uint32_t expected_tmp_crc = crc.value();

    if (tmp_crc != expected_tmp_crc) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Mismatched checksum {} expected {}",
          tmp_crc,
          expected_tmp_crc));
    }

    // unwrap actual fields
    iobuf_parser p(std::move(tmp));
    read_nested(p, st.bitflags, 0U);
    read_nested(p, st.base_offset, 0U);
    read_nested(p, st.max_offset, 0U);
    read_nested(p, st.base_timestamp, 0U);
    read_nested(p, st.max_timestamp, 0U);
    auto fallback = st.index->read_nested(p);
    if (fallback != nullptr) {
        // The fallback can be used if the index compression is
        // enabled but at the same time the index contains non-monotonic
        // offsets. Normally, this shouldn't happen.
        vlog(stlog.error, "Using fallback index representation");
        st.index = std::move(fallback);
    }

    if (hdr._version < index_state::monotonic_timestamps_version) {
        st.batch_timestamps_are_monotonic = false;
        return;
    }

    if (hdr._version >= index_state::monotonic_timestamps_version) {
        read_nested(p, st.batch_timestamps_are_monotonic, 0U);
        read_nested(p, st.with_offset, 0U);
        // if we are deserializing we are likely dealing with a closed
        // segment, this means that the value of this flag is unused in this
        // object, since no new data will be appended. but it's still
        // necessary to read it.
        read_nested(p, st.non_data_timestamps, 0U);
    }

    if (hdr._version >= index_state::broker_timestamp_version) {
        read_nested(p, st.broker_timestamp, 0U);
    }
    if (hdr._version >= index_state::num_compactible_records_version) {
        read_nested(p, st.num_compactible_records_appended, 0U);
    } else {
        st.num_compactible_records_appended = std::nullopt;
    }
    if (hdr._version >= index_state::clean_compact_timestamp_version) {
        read_nested(p, st.clean_compact_timestamp, 0U);
    } else {
        st.clean_compact_timestamp = std::nullopt;
    }
    if (hdr._version >= index_state::may_have_tombstone_records_version) {
        read_nested(p, st.may_have_tombstone_records, 0U);
    } else {
        st.may_have_tombstone_records = true;
    }
}

index_state index_state::copy() const { return *this; }

size_t index_state::size() const { return index->size(); }

bool index_state::empty() const { return index->size() == 0; }

void index_state::add_entry(
  uint32_t relative_offset, offset_time_index relative_time, uint64_t pos) {
    index->add_entry(relative_offset, relative_time.raw_value(), pos);
}

void index_state::pop_back(size_t n) {
    index->pop_back(n);
    if (empty()) {
        non_data_timestamps = false;
    }
}

std::tuple<uint32_t, offset_time_index, uint64_t>
index_state::get_entry(size_t i) const {
    auto [offset, ts, pos] = index->get_entry(i);
    return {offset, offset_time_index{ts, with_offset}, pos};
}

void index_state::shrink_to_fit() { index->shrink_to_fit(); }

std::optional<std::tuple<uint32_t, offset_time_index, uint64_t>>
index_state::find_entry(model::timestamp ts) {
    const auto idx = offset_time_index{ts, with_offset};

    const auto dist = index->time_lower_bound(idx.raw_value());
    if (!dist.has_value()) {
        return std::nullopt;
    }

    // lower_bound will place us on the first batch in the index that has
    // 'max_timestamp' greater than 'ts'. Since not every batch is indexed,
    // it's not guaranteed* that 'ts' will be present in the batch
    // (i.e. 'ts > first_timestamp'). For this reason, we go back one batch.
    //
    // *In the case where lower_bound places on the first batch, we'll
    // start the timequery from the beggining of the segment as the user
    // data batch is always indexed.
    return get_entry(dist.value() > 0 ? dist.value() - 1 : 0);
}

void index_state::update_batch_timestamps_are_monotonic(bool pred) {
    batch_timestamps_are_monotonic = batch_timestamps_are_monotonic && pred;
}

index_state::index_state(const index_state& o) noexcept
  : bitflags(o.bitflags)
  , base_offset(o.base_offset)
  , max_offset(o.max_offset)
  , base_timestamp(o.base_timestamp)
  , max_timestamp(o.max_timestamp)
  , index(o.index->copy())
  , batch_timestamps_are_monotonic(o.batch_timestamps_are_monotonic)
  , with_offset(o.with_offset)
  , non_data_timestamps(o.non_data_timestamps)
  , broker_timestamp(o.broker_timestamp)
  , num_compactible_records_appended(o.num_compactible_records_appended)
  , clean_compact_timestamp(o.clean_compact_timestamp)
  , may_have_tombstone_records(o.may_have_tombstone_records) {}

namespace serde_compat {
uint64_t index_state_serde::checksum(const index_state& r) {
    auto xx = incremental_xxhash64{};
    xx.update_all(
      r.bitflags,
      r.base_offset(),
      r.max_offset(),
      r.base_timestamp(),
      r.max_timestamp(),
      uint32_t(r.index->size()));
    r.index->checksum(xx);
    return xx.digest();
}

index_state index_state_serde::decode(iobuf_parser& parser) {
    index_state retval;

    const auto size = reflection::adl<uint32_t>{}.from(parser);
    if (unlikely(parser.bytes_left() != size)) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Index size does not match header size. Got:{}, expected:{}",
          parser.bytes_left(),
          size));
    }

    const auto expected_checksum = reflection::adl<uint64_t>{}.from(parser);
    retval.bitflags = reflection::adl<uint32_t>{}.from(parser);
    retval.base_offset = model::offset(
      reflection::adl<model::offset::type>{}.from(parser));
    retval.max_offset = model::offset(
      reflection::adl<model::offset::type>{}.from(parser));
    retval.base_timestamp = model::timestamp(
      reflection::adl<model::timestamp::type>{}.from(parser));
    retval.max_timestamp = model::timestamp(
      reflection::adl<model::timestamp::type>{}.from(parser));

    auto fallback = retval.index->from(parser);
    if (fallback != nullptr) {
        // The fallback can be used if the index compression is
        // enabled but at the same time the index contains non-monotonic
        // offsets.
        vlog(stlog.error, "Using fallback index representation");
        retval.index = std::move(fallback);
    }

    const auto computed_checksum = checksum(retval);
    if (unlikely(expected_checksum != computed_checksum)) {
        throw serde::serde_exception(fmt_with_ctx(
          fmt::format,
          "Invalid checksum for index. Got:{}, expected:{}",
          computed_checksum,
          expected_checksum));
    }

    return retval;
}

iobuf index_state_serde::encode(const index_state& st) {
    iobuf out;
    const uint32_t final_size
      = sizeof(uint64_t) // checksum
        + sizeof(storage::index_state::bitflags)
        + sizeof(storage::index_state::base_offset)
        + sizeof(storage::index_state::max_offset)
        + sizeof(storage::index_state::base_timestamp)
        + sizeof(storage::index_state::max_timestamp)
        + sizeof(uint32_t) // index size
        + (st.index->size() * (sizeof(uint32_t) * 2 + sizeof(uint64_t)));
    const uint64_t computed_checksum = checksum(st);
    reflection::serialize(
      out,
      ondisk_version,
      final_size,
      computed_checksum,
      st.bitflags,
      st.base_offset(),
      st.max_offset(),
      st.base_timestamp(),
      st.max_timestamp());
    st.index->to(out);

    // add back the version and size field
    const auto expected_size = final_size + sizeof(int8_t) + sizeof(uint32_t);
    vassert(
      out.size_bytes() == expected_size,
      "Unexpected serialization size {} != expected {}",
      out.size_bytes(),
      expected_size);
    return out;
}
} // namespace serde_compat

index_state::entry index_state::translate_index_entry(
  std::tuple<uint32_t, offset_time_index, uint64_t> input) {
    auto [relative_offset, relative_time, filepos] = input;
    return entry{
      .offset = model::offset(relative_offset + base_offset()),
      .timestamp = model::timestamp(relative_time() + base_timestamp()),
      .filepos = filepos,
    };
}

std::optional<index_state::entry> index_state::find_nearest(model::offset o) {
    if (o < base_offset || empty()) {
        return std::nullopt;
    }
    const uint32_t needle = o() - base_offset();

    auto ix = index->offset_lower_bound(needle).value_or(index->size() - 1);

    // make it signed so it can be negative
    do {
        auto entry = get_entry(ix);
        if (std::get<0>(entry) <= needle) {
            return translate_index_entry(entry);
        }
    } while (ix-- > 0);

    return std::nullopt;
}

std::optional<index_state::entry>
index_state::find_nearest(model::timestamp t) {
    if (t < base_timestamp) {
        return std::nullopt;
    }
    if (empty()) {
        return std::nullopt;
    }

    const auto delta = t - base_timestamp;
    const auto entry = find_entry(delta);
    if (!entry) {
        return std::nullopt;
    }

    return translate_index_entry(*entry);
}

std::optional<index_state::entry>
index_state::find_above_size_bytes(size_t distance) {
    if (empty()) {
        return std::nullopt;
    }
    auto it = index->position_upper_bound(distance);

    if (it == std::nullopt) {
        return std::nullopt;
    }
    int i = it.value();
    return translate_index_entry(get_entry(i));
}

std::optional<index_state::entry>
index_state::find_below_size_bytes(size_t distance) {
    if (empty()) {
        return std::nullopt;
    }
    auto it = index->position_upper_bound(distance);

    auto ix = it.value_or(index->size());
    if (ix > 0) {
        ix--;
    } else {
        return std::nullopt;
    }

    return translate_index_entry(get_entry(ix));
}

bool index_state::truncate(
  model::offset new_max_offset, model::timestamp new_max_timestamp) {
    bool needs_persistence = false;
    if (new_max_offset < base_offset) {
        return needs_persistence;
    }
    const uint32_t i = new_max_offset() - base_offset();
    auto res = index->offset_lower_bound(i);
    size_t remove_back_elems = index->size() - res.value_or(index->size());
    if (remove_back_elems > 0) {
        needs_persistence = true;
        pop_back(remove_back_elems);
    }
    if (new_max_offset < max_offset) {
        needs_persistence = true;
        if (empty()) {
            max_timestamp = base_timestamp;
            max_offset = base_offset;
        } else {
            max_timestamp = new_max_timestamp;
            max_offset = new_max_offset;
        }
    }
    return needs_persistence;
}

} // namespace storage
