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

#include "absl/container/btree_map.h"
#include "bytes/iobuf.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "utils/delta_for.h"

#include <seastar/util/bool_class.hh>

#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <variant>

class iobuf_parser;

namespace storage {

inline constexpr uint64_t position_index_step = 32 * 1024;

/// Base class for the underlying columnar data format
class index_columns_base {
public:
    virtual ~index_columns_base() = default;

    /// Return index of the element or nullopt
    virtual std::optional<int>
    offset_lower_bound(uint32_t needle) const noexcept = 0;

    /// Return index of the element or nullopt
    virtual std::optional<int>
    position_upper_bound(uint64_t needle) const noexcept = 0;

    /// Return index of the element or nullopt
    virtual std::optional<int>
    time_lower_bound(uint32_t needle) const noexcept = 0;

    // serde serialization
    virtual void write(iobuf& buf) const = 0;
    virtual void read_nested(iobuf_parser& buf) = 0;
    virtual void to(iobuf& buf) const = 0;

    // legacy serialization (adl)
    virtual void from(iobuf_parser& buf) = 0;

    // checksum
    virtual void checksum(incremental_xxhash64&) const = 0;

    /// If the size() == input.size() reset the time column with the
    /// provided values.
    /// If the relative_time_index column is empty or size() doesn't match
    /// the operation fails and method returns 'false'.
    virtual bool try_reset_relative_time_index(chunked_vector<uint32_t> input)
      = 0;

    virtual size_t size() const noexcept = 0;

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    virtual void pop_back(int n = 1) = 0;

    /// Add single entry to the index
    virtual void
    add_entry(uint32_t relative_offset, uint32_t relative_time, uint64_t pos)
      = 0;

    /// Optimize memory layout
    virtual void shrink_to_fit() = 0;

    /// Get 3-tuple with offset, timestamp, and file pos by index
    virtual std::tuple<uint32_t, uint32_t, uint64_t>
    get_entry(size_t i) const = 0;

    /// Make deep copy
    virtual std::unique_ptr<index_columns_base> copy() const = 0;

    friend bool operator==(const index_columns_base&, const index_columns_base&)
      = default;
};

/// Operator to use in tests
bool operator==(
  const std::unique_ptr<index_columns_base>& lhs,
  const std::unique_ptr<index_columns_base>& rhs);

class compressed_index_columns : public index_columns_base {
    friend struct test_data;

public:
    /// Return index of the element or nullopt
    std::optional<int>
    offset_lower_bound(uint32_t needle) const noexcept override;

    /// Return index of the element or nullopt
    std::optional<int>
    position_upper_bound(uint64_t needle) const noexcept override;

    /// Return index of the element or nullopt
    std::optional<int>
    time_lower_bound(uint32_t needle) const noexcept override;

    /// If the size() == input.size() reset the time column with the
    /// provided values.
    /// If the relative_time_index column is empty or size() doesn't match
    /// the operation fails and method returns 'false'.
    bool try_reset_relative_time_index(chunked_vector<uint32_t>) override;

    size_t size() const noexcept override;

    // These methods are used by serialization
    void write(iobuf&) const override;
    void read_nested(iobuf_parser& p) override;
    void from(iobuf_parser& buf) override;
    void to(iobuf& buf) const override;

    // hashing
    void checksum(incremental_xxhash64&) const override;

    void add_entry(
      uint32_t relative_offset, uint32_t relative_time, uint64_t pos) override;

    std::tuple<uint32_t, uint32_t, uint64_t> get_entry(size_t i) const override;

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    void pop_back(int n = 1) override;

    void shrink_to_fit() override;

    /// Make deep copy
    std::unique_ptr<index_columns_base> copy() const override;

    friend std::ostream&
    operator<<(std::ostream&, const compressed_index_columns&);

private:
    uint32_t get_relative_offset_index(int ix) const noexcept;
    uint32_t get_relative_time_index(int ix) const noexcept;
    uint64_t get_position_index(int ix) const noexcept;

    void assert_column_sizes() const {
        auto ro = _relative_offset_index.size();
        auto rt = _relative_time_index.size();
        auto ps = _position_index.size();
        vassert(
          ro == rt && rt == ps, "Column sizes differ: {}, {}, {}", ro, rt, ps);
    }

    chunked_vector<uint32_t> copy_relative_offset_index() const noexcept;
    chunked_vector<uint32_t> copy_relative_time_index() const noexcept;
    chunked_vector<uint64_t> copy_position_index() const noexcept;
    void assign_relative_offset_index(chunked_vector<uint32_t>) noexcept;
    void assign_relative_time_index(chunked_vector<uint32_t>) noexcept;
    void assign_position_index(chunked_vector<uint64_t>) noexcept;

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

    using pos_column_t = deltafor_column<
      uint64_t,
      details::delta_delta<uint64_t>,
      position_index_step>;
    using column_t = deltafor_column<uint64_t, details::delta_xor, 0>;
    using offset_column_t
      = deltafor_column<uint64_t, details::delta_delta<uint64_t>, 0>;

    using pos_hint_t = deltafor_stream_pos_t<uint64_t>;
    using offset_hint_t = deltafor_stream_pos_t<uint64_t>;
    using time_hint_t = deltafor_stream_pos_t<uint64_t>;
    using hint_vec_t = std::tuple<offset_hint_t, time_hint_t, pos_hint_t>;

    using hint_map_t
      = absl::btree_map<uint64_t, hint_vec_t, std::greater<uint32_t>>;

    offset_column_t _relative_offset_index;
    column_t _relative_time_index;
    pos_column_t _position_index;

    // Collection of hints used to speedup random access.
    hint_map_t _hints;
};

class index_columns : public index_columns_base {
    friend struct test_data;

public:
    /// Return index of the element or nullopt
    std::optional<int>
    offset_lower_bound(uint32_t needle) const noexcept override;

    /// Return index of the element or nullopt
    std::optional<int>
    position_upper_bound(uint64_t needle) const noexcept override;

    /// Return index of the element or nullopt
    std::optional<int>
    time_lower_bound(uint32_t needle) const noexcept override;

    /// If the size() == param.size() reset the time column with the
    /// provided value.
    /// If the relative_time_index column is empty or size is wrong
    /// the operation fails and method returns 'false'.
    bool try_reset_relative_time_index(chunked_vector<uint32_t>) override;

    // These methods are used by serialization
    void write(iobuf&) const override;
    void read_nested(iobuf_parser& p) override;
    void from(iobuf_parser& parser) override;
    void to(iobuf& buf) const override;

    // hashing
    void checksum(incremental_xxhash64& xx) const override;

    void add_entry(
      uint32_t relative_offset, uint32_t relative_time, uint64_t pos) override;

    std::tuple<uint32_t, uint32_t, uint64_t> get_entry(size_t i) const override;

    /// Pop back one element. This is ineffective with columnar format but
    /// it's not invoked often and when it is invoked it usually invoked not
    /// that many times.
    void pop_back(int n = 1) override;

    void shrink_to_fit() override;

    size_t size() const noexcept override;

    /// Make deep copy
    std::unique_ptr<index_columns_base> copy() const override;

    friend std::ostream& operator<<(std::ostream&, const index_columns&);

private:
    void assign_relative_offset_index(chunked_vector<uint32_t>) noexcept;
    void assign_relative_time_index(chunked_vector<uint32_t>) noexcept;
    void assign_position_index(chunked_vector<uint64_t>) noexcept;

    chunked_vector<uint32_t> _relative_offset_index;
    chunked_vector<uint32_t> _relative_time_index;
    chunked_vector<uint64_t> _position_index;
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

    index_state();

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

    std::unique_ptr<index_columns_base> index;

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

    std::tuple<uint32_t, offset_time_index, uint64_t> get_entry(size_t i) const;

    void shrink_to_fit();

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
    void pop_back(size_t n = 1);

    std::optional<std::tuple<uint32_t, offset_time_index, uint64_t>>
    find_entry(model::timestamp ts);

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
