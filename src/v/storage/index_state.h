/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "utils/delta_for.h"

#include <seastar/util/bool_class.hh>

#include <cstdint>
#include <optional>
#include <tuple>
#include <variant>

class iobuf_parser;

namespace storage {

inline constexpr uint64_t position_index_step = 32 * 1024;

class compressed_index_columns {
public:
    // Accessors
    uint32_t get_relative_offset_index(int ix) const noexcept;
    uint32_t get_relative_time_index(int ix) const noexcept;
    uint64_t get_position_index(int ix) const noexcept;

    template<class Fn>
    void for_each_relative_offset_index(Fn&& fn) const {
        assert_column_sizes();
        std::for_each(
          std::begin(_relative_offset_index),
          std::end(_relative_offset_index),
          std::forward<Fn>(fn));
    }

    template<class Fn>
    void for_each_relative_time_index(Fn&& fn) const {
        assert_column_sizes();
        std::for_each(
          std::begin(_relative_time_index),
          std::end(_relative_time_index),
          std::forward<Fn>(fn));
    }

    template<class Fn>
    void for_each_position_index(Fn&& fn) const {
        assert_column_sizes();
        std::for_each(
          std::begin(_position_index),
          std::end(_position_index),
          std::forward<Fn>(fn));
    }

    void assert_column_sizes() const {
        auto ro = _relative_offset_index.size();
        auto rt = _relative_time_index.size();
        auto ps = _position_index.size();
        vassert(
          ro == rt && rt == ps, "Column sizes differ: {}, {}, {}", ro, rt, ps);
    }

    /// Return index of the element or nullopt
    std::optional<int> offset_lower_bound(uint32_t needle) const noexcept;

    /// Return index of the element or nullopt
    std::optional<int> position_upper_bound(uint64_t needle) const noexcept;

    /// Return index of the element or nullopt
    std::optional<int> time_lower_bound(uint32_t needle) const noexcept;

    /// If the size() ==  1 reset the time column with the
    /// provided value.
    /// If the relative_time_index column is empty or size() > 1
    /// the operation fails and method returns 'false'.
    bool try_reset_relative_time_index(uint32_t);

    bool empty() const noexcept;
    size_t size() const noexcept;

    // These methods are used by serialization
    chunked_vector<uint32_t> copy_relative_offset_index() const noexcept;
    chunked_vector<uint32_t> copy_relative_time_index() const noexcept;
    chunked_vector<uint64_t> copy_position_index() const noexcept;
    void assign_relative_offset_index(chunked_vector<uint32_t>) noexcept;
    void assign_relative_time_index(chunked_vector<uint32_t>) noexcept;
    void assign_position_index(chunked_vector<uint64_t>) noexcept;

    void
    add_entry(uint32_t relative_offset, uint32_t relative_time, uint64_t pos);

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    void pop_back(int n = 1);

    void shrink_to_fit();

    /// Make deep copy
    compressed_index_columns copy() const;

    friend bool operator==(
      const compressed_index_columns& lhs, const compressed_index_columns& rhs);

    friend std::ostream&
    operator<<(std::ostream&, const compressed_index_columns&);

private:
    using pos_column_t = deltafor_column<
      uint64_t,
      details::delta_delta<uint64_t>,
      position_index_step>;
    using column_t = deltafor_column<uint64_t, details::delta_xor, 0>;

    column_t _relative_offset_index;
    column_t _relative_time_index;
    pos_column_t _position_index;
};

class index_columns {
public:
    // Accessors
    uint32_t get_relative_offset_index(int ix) const noexcept;
    uint32_t get_relative_time_index(int ix) const noexcept;
    uint64_t get_position_index(int ix) const noexcept;

    template<class Fn>
    void for_each_relative_offset_index(Fn&& fn) const {
        std::for_each(
          std::begin(_relative_offset_index),
          std::end(_relative_offset_index),
          std::forward<Fn>(fn));
    }

    template<class Fn>
    void for_each_relative_time_index(Fn&& fn) const {
        std::for_each(
          std::begin(_relative_time_index),
          std::end(_relative_time_index),
          std::forward<Fn>(fn));
    }

    template<class Fn>
    void for_each_position_index(Fn&& fn) const {
        std::for_each(
          std::begin(_position_index),
          std::end(_position_index),
          std::forward<Fn>(fn));
    }

    /// Return index of the element or nullopt
    std::optional<int> offset_lower_bound(uint32_t needle) const noexcept;

    /// Return index of the element or nullopt
    std::optional<int> position_upper_bound(uint64_t needle) const noexcept;

    /// Return index of the element or nullopt
    std::optional<int> time_lower_bound(uint32_t needle) const noexcept;

    /// If the size() ==  1 reset the time column with the
    /// provided value.
    /// If the relative_time_index column is empty or size() > 1
    /// the operation fails and method returns 'false'.
    bool try_reset_relative_time_index(uint32_t);

    bool empty() const noexcept;
    size_t size() const noexcept;

    // These methods are used by serialization
    chunked_vector<uint32_t> copy_relative_offset_index() const noexcept;
    chunked_vector<uint32_t> copy_relative_time_index() const noexcept;
    chunked_vector<uint64_t> copy_position_index() const noexcept;
    void assign_relative_offset_index(chunked_vector<uint32_t>) noexcept;
    void assign_relative_time_index(chunked_vector<uint32_t>) noexcept;
    void assign_position_index(chunked_vector<uint64_t>) noexcept;

    void
    add_entry(uint32_t relative_offset, uint32_t relative_time, uint64_t pos);

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    void pop_back(int n = 1);

    void shrink_to_fit();

    /// Make deep copy
    index_columns copy() const;

    friend bool operator==(const index_columns&, const index_columns&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const index_columns&);

private:
    chunked_vector<uint32_t> _relative_offset_index;
    chunked_vector<uint32_t> _relative_time_index;
    chunked_vector<uint64_t> _position_index;
};

class index_columns_mux {
    using variant_t = std::variant<index_columns, compressed_index_columns>;
    static variant_t initialize(bool compressed) {
        if (compressed) {
            return compressed_index_columns();
        }
        return index_columns();
    }
    explicit index_columns_mux(index_columns col)
      : _val(std::move(col)) {}
    explicit index_columns_mux(compressed_index_columns col)
      : _val(std::move(col)) {}

public:
    explicit index_columns_mux(
      bool compressed
      = config::shard_local_cfg().log_segment_index_compression.value())
      : _val(initialize(compressed)) {}

    uint32_t get_relative_offset_index(int ix) const noexcept {
        return std::visit(
          [ix](auto&& c) { return c.get_relative_offset_index(ix); }, _val);
    }
    uint32_t get_relative_time_index(int ix) const noexcept {
        return std::visit(
          [ix](auto&& c) { return c.get_relative_time_index(ix); }, _val);
    }
    uint64_t get_position_index(int ix) const noexcept {
        return std::visit(
          [ix](auto&& c) { return c.get_position_index(ix); }, _val);
    }

    template<class Fn>
    void for_each_relative_offset_index(Fn&& fn) const {
        return std::visit(
          [&](auto&& c) mutable {
              return c.for_each_relative_offset_index(std::forward<Fn>(fn));
          },
          _val);
    }

    template<class Fn>
    void for_each_relative_time_index(Fn&& fn) const {
        return std::visit(
          [&](auto&& c) mutable {
              return c.for_each_relative_time_index(std::forward<Fn>(fn));
          },
          _val);
    }

    template<class Fn>
    void for_each_position_index(Fn&& fn) const {
        return std::visit(
          [&](auto&& c) mutable {
              return c.for_each_position_index(std::forward<Fn>(fn));
          },
          _val);
    }

    /// Return index of the element or nullopt
    std::optional<int> offset_lower_bound(uint32_t needle) const noexcept {
        return std::visit(
          [needle](auto&& c) { return c.offset_lower_bound(needle); }, _val);
    }

    /// Return index of the element or nullopt
    std::optional<int> position_upper_bound(uint64_t needle) const noexcept {
        return std::visit(
          [needle](auto&& c) { return c.position_upper_bound(needle); }, _val);
    }

    /// Return index of the element or nullopt
    std::optional<int> time_lower_bound(uint32_t needle) const noexcept {
        return std::visit(
          [needle](auto&& c) { return c.time_lower_bound(needle); }, _val);
    }

    /// If the size() ==  1 reset the time column with the
    /// provided value.
    /// If the relative_time_index column is empty or size() > 1
    /// the operation fails and method returns 'false'.
    bool try_reset_relative_time_index(uint32_t t) {
        return std::visit(
          [t](auto&& c) { return c.try_reset_relative_time_index(t); }, _val);
    }

    bool empty() const noexcept {
        return std::visit([](auto&& c) { return c.empty(); }, _val);
    }
    size_t size() const noexcept {
        return std::visit([](auto&& c) { return c.size(); }, _val);
    }

    // These methods are used by serialization
    chunked_vector<uint32_t> copy_relative_offset_index() const noexcept {
        return std::visit(
          [](auto&& c) { return c.copy_relative_offset_index(); }, _val);
    }
    chunked_vector<uint32_t> copy_relative_time_index() const noexcept {
        return std::visit(
          [](auto&& c) { return c.copy_relative_time_index(); }, _val);
    }
    chunked_vector<uint64_t> copy_position_index() const noexcept {
        return std::visit(
          [](auto&& c) { return c.copy_position_index(); }, _val);
    }
    void assign_relative_offset_index(chunked_vector<uint32_t> vec) noexcept {
        std::visit(
          [v = std::move(vec)](auto&& c) mutable {
              return c.assign_relative_offset_index(std::move(v));
          },
          _val);
    }
    void assign_relative_time_index(chunked_vector<uint32_t> vec) noexcept {
        std::visit(
          [v = std::move(vec)](auto&& c) mutable {
              return c.assign_relative_time_index(std::move(v));
          },
          _val);
    }
    void assign_position_index(chunked_vector<uint64_t> vec) noexcept {
        std::visit(
          [v = std::move(vec)](auto&& c) mutable {
              return c.assign_position_index(std::move(v));
          },
          _val);
    }

    void
    add_entry(uint32_t relative_offset, uint32_t relative_time, uint64_t pos) {
        std::visit(
          [relative_offset, relative_time, pos](auto&& c) {
              return c.add_entry(relative_offset, relative_time, pos);
          },
          _val);
    }

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    void pop_back(int n = 1) {
        std::visit([n](auto&& c) { return c.pop_back(n); }, _val);
    }

    void shrink_to_fit() {
        std::visit([](auto&& c) { return c.shrink_to_fit(); }, _val);
    }

    /// Make deep copy
    index_columns_mux copy() const {
        return std::visit(
          [](auto&& c) { return index_columns_mux(c.copy()); }, _val);
    }

    friend bool operator==(
      const index_columns_mux& lhs, const index_columns_mux& rhs) noexcept {
        if (
          std::holds_alternative<index_columns>(lhs._val)
          && std::holds_alternative<index_columns>(rhs._val)) {
            return std::get<index_columns>(lhs._val)
                   == std::get<index_columns>(rhs._val);
        }
        if (
          std::holds_alternative<compressed_index_columns>(lhs._val)
          && std::holds_alternative<compressed_index_columns>(rhs._val)) {
            return std::get<compressed_index_columns>(lhs._val)
                   == std::get<compressed_index_columns>(rhs._val);
        }
        return false;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const index_columns_mux& m) {
        std::visit([&o](auto&& c) { o << c; }, m._val);
        return o;
    }

private:
    variant_t _val;
};

using offset_delta_time = ss::bool_class<struct offset_delta_time_tag>;

/*
 * In order to be able to represent negative time deltas (required for
 * out of order timestamps, the time delta stored in 'index_state' is
 * offset by 2^31. The 'offset_time_index' class below deals with
 * this translation. The range for time deltas is roughly from -596h to +596h.
 */
class offset_time_index {
public:
    static constexpr model::timestamp::type offset = 2147483648; // 2^31
    static constexpr model::timestamp::type delta_time_min = -offset;
    static constexpr model::timestamp::type delta_time_max = offset - 1;

    offset_time_index(model::timestamp ts, offset_delta_time with_offset);

    uint32_t operator()() const;

private:
    offset_time_index(uint32_t val, offset_delta_time with_offset);

    uint32_t raw_value() const;

    offset_delta_time _with_offset;
    uint32_t _val;

    friend struct index_state;
};

/* Fileformat:
   1 byte  - version
   4 bytes - size - does not include the version or size
   8 bytes - checksum - xxhash32 -- we checksum everything below the checksum
   4 bytes - bitflags - unused
   8 bytes - based_offset
   8 bytes - max_offset
   8 bytes - base_time
   8 bytes - max_time
   4 bytes - index.size()
   [] relative_offset_index
   [] relative_time_index
   [] position_index
   1 byte  - batch_timestamps_are_monotonic
   1 byte  - with_offset
   1 byte  - non_data_timestamps
 */
struct index_state
  : serde::envelope<index_state, serde::version<9>, serde::compat_version<4>> {
    static constexpr auto monotonic_timestamps_version = 5;
    static constexpr auto broker_timestamp_version = 6;
    static constexpr auto num_compactible_records_version = 7;
    static constexpr auto clean_compact_timestamp_version = 8;
    static constexpr auto may_have_tombstone_records_version = 9;

    static index_state make_empty_index(offset_delta_time with_offset);

    struct entry {
        model::offset offset;
        model::timestamp timestamp;
        size_t filepos;
        friend std::ostream& operator<<(std::ostream&, const entry&);
    };

    index_state() = default;

    index_state(index_state&&) noexcept = default;
    index_state& operator=(index_state&&) noexcept = default;
    index_state& operator=(const index_state&) = delete;
    ~index_state() noexcept = default;

    index_state copy() const;

    std::optional<entry> find_nearest(model::offset o);

    std::optional<entry> find_nearest(model::timestamp);

    std::optional<entry> find_above_size_bytes(size_t distance);

    std::optional<entry> find_below_size_bytes(size_t distance);

    bool
    truncate(model::offset new_max_offset, model::timestamp new_max_timestamp);

    /// \brief unused
    uint32_t bitflags{0};
    // the batch's base_offset of the first batch
    model::offset base_offset{0};
    // it is the batch's last_offset of the last batch
    model::offset max_offset{0};
    // the batch's base_timestamp of the first batch
    model::timestamp base_timestamp{0};
    // the batch's max_timestamp of the last batch
    model::timestamp max_timestamp{0};

    index_columns_mux index;

    // flag indicating whether the maximum timestamp on the batches
    // of this segment are monontonically increasing.
    bool batch_timestamps_are_monotonic{true};

    // flag indicating whether the relative time index has been offset
    offset_delta_time with_offset{false};

    // flag indicating whether this segment contains non user-data timestamps
    // this flag is meaningfull only in an open segment, during append op.
    bool non_data_timestamps{false};

    // a place to register the broker timestamp of the last modification.
    // used for retention. std::optional to allow upgrading without rewriting
    // the index.
    std::optional<model::timestamp> broker_timestamp{std::nullopt};

    // The number of compactible records appended to the segment. This may not
    // necessarily indicate the exact number of compactible records, e.g. if
    // the segment was truncated, the count will remain the same. As such, this
    // value may be an overestimate of the exact number of compactible records.
    //
    // Returns std::nullopt if this index was written in a version that didn't
    // support this field, and we can't conclude anything.
    std::optional<size_t> num_compactible_records_appended{0};

    // If set, the timestamp at which every record up to and including
    // those in this segment were first compacted via sliding window.
    // If not yet set, sliding window compaction has not yet been applied to
    // every previous record in the log.
    std::optional<model::timestamp> clean_compact_timestamp{std::nullopt};

    // may_have_tombstone_records is `true` by default, until compaction
    // deduplication/segment data copying is performed and it is proven that
    // the segment does not contain any tombstone records.
    bool may_have_tombstone_records{true};

    size_t size() const;

    bool empty() const;

    void add_entry(
      uint32_t relative_offset, offset_time_index relative_time, uint64_t pos);

    void pop_back(size_t n = 1);

    std::tuple<uint32_t, offset_time_index, uint64_t> get_entry(size_t i) const;

    void shrink_to_fit();

    std::optional<std::tuple<uint32_t, offset_time_index, uint64_t>>
    find_entry(model::timestamp ts);

    bool maybe_index(
      size_t accumulator,
      size_t step,
      size_t starting_position_in_file,
      model::offset base_offset,
      model::offset batch_max_offset,
      model::timestamp first_timestamp,
      model::timestamp last_timestamp,
      std::optional<model::timestamp> new_broker_timestamp,
      bool user_data,
      size_t compactible_records);

    void update_batch_timestamps_are_monotonic(bool pred);

    friend bool operator==(const index_state&, const index_state&) = default;

    friend std::ostream& operator<<(std::ostream&, const index_state&);

    void serde_write(iobuf&) const;
    friend void read_nested(iobuf_parser&, index_state&, const size_t);

private:
    index_state(const index_state& o) noexcept;
    entry translate_index_entry(
      std::tuple<uint32_t, offset_time_index, uint64_t> entry);
};

namespace serde_compat {
struct index_state_serde {
    static constexpr int8_t ondisk_version = 3;
    static uint64_t checksum(const index_state& r);
    static index_state decode(iobuf_parser& parser);
    static iobuf encode(const index_state& st);
};
} // namespace serde_compat

} // namespace storage
