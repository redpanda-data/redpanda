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

#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/timestamp.h"
#include "serde/async.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/rw.h"

#include <seastar/core/smp.hh>
#include <seastar/util/optimized_optional.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/numeric.hpp>

#include <bitset>
#include <compare>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <numeric>
#include <variant>
#include <vector>

namespace model {

/// \brief Attributes associated with a record.
///
/// Record attributes are part of the Kafka record format:
///
///   https://kafka.apache.org/documentation/#record
///
/// The record attributes in Kafka are unused (as of 3 Oct 2019). However, by
/// including them here (1) it is easier to manage the translation from the
/// on-disk record size to that used by kafka, and (2) we may track attributes
/// for internal message types.
class record_attributes final {
public:
    using type = int8_t;

    record_attributes() noexcept = default;

    explicit record_attributes(type v) noexcept
      : _attributes(v) {}

    type value() const { return static_cast<type>(_attributes.to_ulong()); }

    bool operator==(const record_attributes& other) const {
        return _attributes == other._attributes;
    }

    bool operator!=(const record_attributes& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_attributes&);

private:
    std::bitset<8> _attributes;
};

class record_header {
public:
    record_header(int32_t k_len, iobuf k, int32_t v_len, iobuf v)
      : _key_size(k_len)
      , _key(std::move(k))
      , _val_size(v_len)
      , _value(std::move(v)) {}

    size_t memory_usage() const {
        return sizeof(*this) + _key.size_bytes() + _value.size_bytes();
    }
    record_header share() {
        return record_header(_key_size, share_key(), _val_size, share_value());
    }
    record_header copy() const {
        return record_header(_key_size, _key.copy(), _val_size, _value.copy());
    }

    int32_t key_size() const { return _key_size; }
    const iobuf& key() const { return _key; }
    iobuf release_key() { return std::exchange(_key, {}); }
    iobuf share_key() { return _key.share(0, _key.size_bytes()); }

    int32_t value_size() const { return _val_size; }
    const iobuf& value() const { return _value; }
    iobuf release_value() { return std::exchange(_value, {}); }
    iobuf share_value() { return _value.share(0, _value.size_bytes()); }

    std::optional<iobuf> share_key_opt() {
        return key_size() < 0 ? std::nullopt
                              : std::make_optional<iobuf>(share_key());
    }
    std::optional<iobuf> share_value_opt() {
        return value_size() < 0 ? std::nullopt
                                : std::make_optional<iobuf>(share_value());
    }

    bool operator==(const record_header& rhs) const {
        return _key_size == rhs._key_size && _val_size == rhs._val_size
               && _key == rhs._key && _value == rhs._value;
    }

    friend std::ostream& operator<<(std::ostream&, const record_header&);

private:
    // If negative, the key is nil.
    int32_t _key_size{-1};
    iobuf _key;
    // If negative, the value is nil.
    int32_t _val_size{-1};
    iobuf _value;
};

/// \brief
// DefaultRecord(int sizeInBytes,
//               byte attributes,
//               long offset,
//               long timestamp,
//               int sequence,
//               ByteBuffer key,
//               ByteBuffer value,
//               Header[] headers) {
//     this.sizeInBytes = sizeInBytes;
//     this.attributes = attributes;
//     this.offset = offset;
//     this.timestamp = timestamp;
//     this.sequence = sequence;
//     this.key = key;
//     this.value = value;
//     this.headers = headers;
// }
class record {
public:
    record() = default;
    ~record() noexcept = default;
    record(record&&) noexcept = default;
    record& operator=(record&&) noexcept = default;
    record(const record&) = delete;
    record operator=(const record&) = delete;
    record(
      int32_t size_bytes,
      record_attributes attributes,
      int64_t timestamp_delta,
      int32_t offset_delta,
      int32_t key_size,
      iobuf key,
      int32_t val_size,
      iobuf value,
      std::vector<record_header> hdrs) noexcept
      : _size_bytes(size_bytes)
      , _attributes(attributes)
      , _timestamp_delta(timestamp_delta)
      , _offset_delta(offset_delta)
      , _key_size(key_size)
      , _key(std::move(key))
      , _val_size(val_size)
      , _value(std::move(value))
      , _headers(std::move(hdrs)) {}

    record(
      record_attributes attributes,
      int64_t timestamp_delta,
      int32_t offset_delta,
      iobuf key,
      iobuf value,
      std::vector<record_header> hdrs) noexcept
      : _attributes(attributes)
      , _timestamp_delta(timestamp_delta)
      , _offset_delta(offset_delta)
      , _key_size(static_cast<int32_t>(key.size_bytes()))
      , _key(std::move(key))
      , _val_size(static_cast<int32_t>(value.size_bytes()))
      , _value(std::move(value))
      , _headers(std::move(hdrs)) {
        _size_bytes = static_cast<int32_t>(
          sizeof(model::record_attributes::type)   //
          + vint::vint_size(_timestamp_delta)      //
          + vint::vint_size(_offset_delta)         //
          + _key_size + vint::vint_size(_key_size) //
          + _val_size + vint::vint_size(_val_size) //
          + std::accumulate(
            _headers.begin(),
            _headers.end(),
            size_t(0),
            [](size_t acc, const record_header& h) {
                return acc + h.memory_usage();
            }) //
        );
    }

    // Size in bytes of everything except the size_bytes field.
    int32_t size_bytes() const { return _size_bytes; }

    // Used for acquiring units from semaphores limiting
    // memory resources.
    size_t memory_usage() const {
        return sizeof(*this) + _key.size_bytes() + _value.size_bytes()
               + std::accumulate(
                 _headers.begin(),
                 _headers.end(),
                 size_t(0),
                 [](size_t acc, const record_header& h) {
                     return acc + h.memory_usage();
                 });
    }

    record_attributes attributes() const { return _attributes; }

    int64_t timestamp_delta() const { return _timestamp_delta; }

    int32_t offset_delta() const { return _offset_delta; }

    int32_t key_size() const { return _key_size; }
    const iobuf& key() const { return _key; }
    iobuf release_key() { return std::exchange(_key, {}); }
    iobuf share_key() { return _key.share(0, _key.size_bytes()); }
    std::optional<iobuf> share_key_opt() {
        if (!has_key()) {
            return std::nullopt;
        }
        return share_key();
    }
    std::optional<iobuf> share_value_opt() {
        if (!has_value()) {
            return std::nullopt;
        }
        return share_value();
    }

    int32_t value_size() const { return _val_size; }
    const iobuf& value() const { return _value; }
    iobuf release_value() { return std::exchange(_value, {}); }
    iobuf share_value() { return _value.share(0, _value.size_bytes()); }
    bool has_value() const { return _val_size >= 0; }
    bool has_key() const { return _key_size >= 0; }
    bool is_tombstone() const { return !has_value(); }

    const std::vector<record_header>& headers() const { return _headers; }
    std::vector<record_header>& headers() { return _headers; }

    record share() {
        std::vector<record_header> copy;
        copy.reserve(_headers.size());
        for (auto& h : _headers) {
            copy.push_back(h.share());
        }
        return record(
          _size_bytes,
          _attributes,
          _timestamp_delta,
          _offset_delta,
          _key_size,
          share_key(),
          _val_size,
          share_value(),
          std::move(copy));
    }
    record copy() const {
        std::vector<record_header> cp;
        cp.reserve(_headers.size());
        for (auto& h : _headers) {
            cp.push_back(h.copy());
        }
        return record(
          _size_bytes,
          _attributes,
          _timestamp_delta,
          _offset_delta,
          _key_size,
          _key.copy(),
          _val_size,
          _value.copy(),
          std::move(cp));
    }
    bool operator==(const record& other) const {
        return _size_bytes == other._size_bytes
               && _timestamp_delta == other._timestamp_delta
               && _offset_delta == other._offset_delta && _key == other._key
               && _value == other._value && _headers == other._headers;
    }

    bool operator!=(const record& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream&, const record&);

private:
    int32_t _size_bytes{0};
    record_attributes _attributes;
    int64_t _timestamp_delta{0};
    int32_t _offset_delta{0};
    int32_t _key_size{-1};
    iobuf _key;
    int32_t _val_size{-1};
    iobuf _value;
    std::vector<record_header> _headers{};
};

class record_batch_attributes final {
public:
    static constexpr uint16_t compression_mask = 0x7;
    static constexpr uint16_t timestamp_type_mask = 0x8;
    static constexpr uint16_t transactional_mask = 0x10;
    static constexpr uint16_t control_mask = 0x20;

    using type = int16_t;

    record_batch_attributes() noexcept = default;

    explicit record_batch_attributes(type v) noexcept
      : _attributes(v) {}

    type value() const { return static_cast<type>(_attributes.to_ulong()); }

    bool is_control() const { return maskable_value() & control_mask; }

    bool is_transactional() const {
        return maskable_value() & transactional_mask;
    }
    bool is_valid_compression() const {
        auto at = maskable_value() & compression_mask;
        if (at >= 0 && at <= 4) {
            return true;
        }
        return false;
    }
    model::compression compression() const {
        auto value = maskable_value() & compression_mask;
        switch (value) {
        case 0:
            return compression::none;
        case 1:
            return compression::gzip;
        case 2:
            return compression::snappy;
        case 3:
            return compression::lz4;
        case 4:
            return compression::zstd;
        default:
            throw std::runtime_error(
              fmt::format("Unknown compression value: {}", value));
        }
    }

    model::timestamp_type timestamp_type() const {
        return _attributes.test(3) ? timestamp_type::append_time
                                   : timestamp_type::create_time;
    }

    void set_timestamp_type(model::timestamp_type t) {
        _attributes.set(3, t == timestamp_type::append_time);
    }

    void set_control_type() { _attributes |= control_mask; }

    void set_transactional_type() { _attributes |= transactional_mask; }

    bool operator==(const record_batch_attributes& other) const {
        return _attributes == other._attributes;
    }

    bool operator!=(const record_batch_attributes& other) const {
        return !(*this == other);
    }

    record_batch_attributes& operator|=(model::compression c) {
        // clang-format off
        _attributes |=
        static_cast<std::underlying_type_t<model::compression>>(c)
            & record_batch_attributes::compression_mask;
        // clang-format on
        return *this;
    }
    void remove_compression() {
        _attributes &= ~record_batch_attributes::compression_mask;
    }

    record_batch_attributes& operator|=(model::timestamp_type ts_t) {
        _attributes |= (static_cast<uint64_t>(ts_t) << uint64_t(3))
                       & record_batch_attributes::timestamp_type_mask;
        return *this;
    }
    friend inline void read_nested(
      iobuf_parser& in,
      record_batch_attributes& attrs,
      const size_t bytes_left_limit) {
        attrs._attributes = serde::read_nested<uint64_t>(in, bytes_left_limit);
    }

    friend inline void write(iobuf& out, record_batch_attributes el) {
        serde::write(out, static_cast<uint64_t>(el._attributes.to_ullong()));
    }

    friend std::ostream&
    operator<<(std::ostream&, const record_batch_attributes&);

private:
    uint16_t maskable_value() const {
        return static_cast<uint16_t>(_attributes.to_ulong());
    }

    // Bits 4 and 5 are used by Kafka and thus reserved.
    std::bitset<16> _attributes;
};

/** expect all fields to be serialized, except context fields */
struct record_batch_header
  : serde::envelope<
      record_batch_header,
      serde::version<0>,
      serde::compat_version<0>> {
    struct context
      : serde::envelope<context, serde::version<0>, serde::compat_version<0>> {
        context() noexcept = default;
        context(model::term_id t, ss::shard_id i)
          : term(t)
          , owner_shard(i) {}

        /*
         * term isn't part of the upstream kafka batch format, but we use it
         * along with other context for tracking some redpanda-specific info.
         *
         * one spot where term is used is during fetch to populate the kafka
         * "partition leader epoch" field and the term originates from the raft
         * term.
         *
         * in the context of produce a batch will have some value for this
         * "partition leader epoch" field set by the client, but it isn't used
         * internally despite being on the wire.
         *
         * this all causes a problem for the kafka/request_parser_test which
         * validates binary equivalence of requests generated from a thirdparty
         * kafka client and those generated by redpanda. in this case redpanda
         * _writes_ a term value in this field, but when we read data from the
         * client we ignore this field. originally this was passing because the
         * default values were all compatible. to solve this the kafka batch
         * adapter persists this field into the term context so that the
         * end-to-end test works.
         */
        model::term_id term;
        std::optional<ss::shard_id> owner_shard;

        void serde_write(iobuf& out) const {
            // serialize with serde a serde-only type
            using serde::write;
            write(out, term);
        }

        void serde_read(iobuf_parser& in, const serde::header& h) {
            // deserialize with serde a serde-only type
            using serde::read_nested;
            term = read_nested<model::term_id>(in, h._bytes_left_limit);
            owner_shard = ss::this_shard_id();
        }

        friend bool operator==(
          const record_batch_header::context&,
          const record_batch_header::context&)
          = default;
    };

    /// \brief every thing below this field gets CRC, except `context`
    /// which is excluded from the on-disk-format as well.
    /// Note: this is included first because the size of the header is fixed.
    /// it is encoded in little endian format.
    uint32_t header_crc{0};

    int32_t size_bytes{0};
    offset base_offset;
    /// \brief redpanda extension
    record_batch_type type;
    uint32_t crc{0};

    // -- below the CRC are checksummed by the kafka crc. see @crc field

    record_batch_attributes attrs;

    // The difference in offset between the first and last offset in the batch.
    int32_t last_offset_delta{0};
    timestamp first_timestamp;
    timestamp max_timestamp;
    int64_t producer_id{0};
    int16_t producer_epoch{0};
    int32_t base_sequence{0};

    // The number of records in the batch. Note, this may not necessarily be
    // the same as last_offset_delta, which is preserved across compaction,
    // unlike record_count.
    int32_t record_count{0};

    auto serde_fields() {
        return std::tie(
          header_crc,
          size_bytes,
          base_offset,
          type,
          crc,
          attrs,
          last_offset_delta,
          first_timestamp,
          max_timestamp,
          producer_id,
          producer_epoch,
          base_sequence,
          record_count,
          ctx);
    }

    bool contains(model::offset offset) const {
        return base_offset <= offset && offset <= last_offset();
    }
    /// context object with opaque environment data
    context ctx;

    offset last_offset() const {
        return base_offset + offset(last_offset_delta);
    }
    record_batch_header copy() const {
        record_batch_header h = *this;
        h.ctx.owner_shard = ss::this_shard_id();
        return h;
    }
    bool operator==(const record_batch_header& other) const {
        return header_crc == other.header_crc && size_bytes == other.size_bytes
               && base_offset == other.base_offset && crc == other.crc
               && attrs == other.attrs
               && last_offset_delta == other.last_offset_delta
               && first_timestamp == other.first_timestamp
               && max_timestamp == other.max_timestamp
               && record_count == other.record_count;
    }

    bool operator!=(const record_batch_header& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_batch_header&);
};

using tx_seq = named_type<int64_t, struct tm_tx_seq>;
using producer_id = named_type<int64_t, struct producer_identity_id>;
using producer_epoch = named_type<int16_t, struct producer_identity_epoch>;

static constexpr producer_epoch no_producer_epoch{-1};
static constexpr producer_id no_producer_id{-1};
struct producer_identity
  : serde::
      envelope<producer_identity, serde::version<0>, serde::compat_version<0>> {
    producer_id id{no_producer_id};
    producer_epoch epoch{0};

    producer_identity() noexcept = default;

    constexpr producer_identity(int64_t id, int16_t epoch)
      : id(id)
      , epoch(epoch) {}

    model::producer_id get_id() const { return model::producer_id(id); }

    model::producer_epoch get_epoch() const {
        return model::producer_epoch(epoch);
    }

    static model::producer_identity
    with_next_epoch(const model::producer_identity pid) {
        return {pid.id, pid.epoch + producer_epoch(1)};
    }

    bool has_exhausted_epoch() const {
        return epoch >= (producer_epoch::max() - producer_epoch{1});
    }

    auto operator<=>(const producer_identity&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const producer_identity& pid) {
        return H::combine(std::move(h), pid.id, pid.epoch);
    }

    friend std::ostream& operator<<(std::ostream&, const producer_identity&);

    auto serde_fields() { return std::tie(id, epoch); }
};

/// This structure is a part of rm_stm snapshot.
/// Any change has to be reconciled with the
/// snapshot (de)serialization logic.
struct tx_range
  : serde::envelope<tx_range, serde::version<0>, serde::compat_version<0>> {
    tx_range() = default;

    tx_range(model::producer_identity pid, model::offset f, model::offset l)
      : pid(pid)
      , first(f)
      , last(l) {}

    model::producer_identity pid;
    model::offset first;
    model::offset last;

    auto serde_fields() { return std::tie(pid, first, last); }

    auto operator<=>(const tx_range&) const = default;
    friend std::ostream& operator<<(std::ostream&, const tx_range&);

    template<typename H>
    friend H AbslHashValue(H h, const tx_range& range) {
        return H::combine(std::move(h), range.first, range.last, range.pid);
    }
};

// Comparator that sorts in ascending order by first offset.
struct tx_range_cmp {
    auto operator()(const tx_range& l, const tx_range& r) {
        return l.first > r.first;
    }
};

inline constexpr producer_identity no_pid{no_producer_id, no_producer_epoch};

struct batch_identity {
    static int32_t increment_sequence(int32_t sequence, int32_t increment) {
        if (sequence > std::numeric_limits<int32_t>::max() - increment) {
            return increment - (std::numeric_limits<int32_t>::max() - sequence)
                   - 1;
        }
        return sequence + increment;
    }

    static batch_identity from(const record_batch_header& hdr) {
        return batch_identity{
          .pid = model::producer_identity{hdr.producer_id, hdr.producer_epoch},
          .first_seq = hdr.base_sequence,
          .last_seq = increment_sequence(
            hdr.base_sequence, hdr.last_offset_delta),
          .record_count = hdr.record_count,
          .max_timestamp = hdr.max_timestamp,
          .is_transactional = hdr.attrs.is_transactional()};
    }

    producer_identity pid;
    int32_t first_seq{0};
    int32_t last_seq{0};
    int32_t record_count;
    timestamp max_timestamp;
    bool is_transactional{false};

    bool is_idempotent() const { return pid.id > no_producer_id; }

    friend std::ostream& operator<<(std::ostream&, const batch_identity&);
};

// A simple iterator for model::record_batch
//
// Usage:
//
// ```
// auto it = model::record_batch_iterator::create(batch);
// while (it.has_next()) {
//   model::record record = it.next();
//   // do something with record
//   co_await ss::coroutine::maybe_yield();
// }
// ```
class record_batch_iterator {
public:
    bool has_next() const noexcept;

    model::record next();

    static record_batch_iterator create(const model::record_batch& b);

private:
    record_batch_iterator(int32_t rc, iobuf_const_parser p);

    int32_t _index = 0;
    int32_t _record_count;
    iobuf_const_parser _parser;
};

constexpr uint32_t packed_record_batch_header_size
  = sizeof(model::record_batch_header::header_crc)          // 4
    + sizeof(model::record_batch_header::size_bytes)        // 4
    + sizeof(model::record_batch_header::base_offset)       // 8
    + sizeof(model::record_batch_type)                      // 1
    + sizeof(model::record_batch_header::crc)               // 4
    + sizeof(model::record_batch_attributes::type)          // 2
    + sizeof(model::record_batch_header::last_offset_delta) // 4
    + sizeof(model::record_batch_header::first_timestamp)   // 8
    + sizeof(model::record_batch_header::max_timestamp)     // 8
    + sizeof(model::record_batch_header::producer_id)       // 8
    + sizeof(model::record_batch_header::producer_epoch)    // 2
    + sizeof(model::record_batch_header::base_sequence)     // 4
    + sizeof(model::record_batch_header::record_count);     // 4
static_assert(packed_record_batch_header_size == 61);

class record_batch
  : public serde::envelope<
      model::record_batch,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    /**
     * Compatability interface. Compression is based on the record type rather
     * than the header (relevant for ghost batches). Uncompressed records are
     * automatically encoded into a single iobuf.
     */
    using uncompressed_records = std::vector<record>;
    using compressed_records = iobuf;
    using records_type = std::variant<uncompressed_records, compressed_records>;

    record_batch(record_batch_header header, records_type&& records)
      : _header(header)
      , _compressed(std::holds_alternative<compressed_records>(records)) {
        if (_compressed) {
            _records = std::move(std::get<compressed_records>(records));
        } else {
            const auto& recs = std::get<uncompressed_records>(records);
            vassert(
              _header.record_count == static_cast<int32_t>(recs.size()),
              "Batch header record count does not match payload");
            for (const auto& r : recs) {
                model::append_record_to_buffer(_records, r);
            }
        }
        vassert(
          _header.size_bytes
            == static_cast<int32_t>(
              model::packed_record_batch_header_size + _records.size_bytes()),
          "Record batch header size {} does not match calculated size {}",
          _header.size_bytes,
          model::packed_record_batch_header_size + _records.size_bytes());
    }

    /**
     * Build a record batch. The header must have the a compression flag set
     * which corresponds to the record encoding in the input iobuf.
     */
    struct tag_ctor_ng {};

    record_batch(record_batch_header header, iobuf records, tag_ctor_ng)
      : _header(header)
      , _records(std::move(records))
      , _compressed(_header.attrs.compression() != compression::none) {
        vassert(
          _header.size_bytes
            == static_cast<int32_t>(
              model::packed_record_batch_header_size + _records.size_bytes()),
          "Record batch header size {} does not match calculated size {}",
          _header.size_bytes,
          model::packed_record_batch_header_size + _records.size_bytes());
    }

    record_batch(const record_batch& o) = delete;
    record_batch& operator=(const record_batch&) = delete;
    record_batch(record_batch&&) noexcept = default;
    record_batch& operator=(record_batch&&) noexcept = default;
    ~record_batch() = default;

    bool empty() const { return _header.record_count <= 0; }

    bool compressed() const { return _compressed; }

    record_batch copy() const {
        // copy sets shard id
        return record_batch(_header.copy(), _records.copy(), _compressed);
    }

    int32_t record_count() const { return _header.record_count; }
    model::offset base_offset() const { return _header.base_offset; }
    model::offset last_offset() const { return _header.last_offset(); }
    model::term_id term() const { return _header.ctx.term; }
    void set_term(model::term_id i) { _header.ctx.term = i; }
    // Size in bytes of the header plus records.
    int32_t size_bytes() const { return _header.size_bytes; }
    bool contains_transactional_data() const {
        // A transactional batch can be a
        // 1. transactional data batch - transactional bit set
        // 2. transactional control batch (fence, abort etc) - control batch
        // and pid >= 0
        return _header.attrs.is_transactional()
               || (_header.attrs.is_control() && _header.producer_id >= 0);
    }

    size_t memory_usage() const {
        return sizeof(*this) + _records.size_bytes();
    }

    const record_batch_header& header() const { return _header; }
    record_batch_header& header() { return _header; }

    bool contains(model::offset offset) const {
        return _header.contains(offset);
    }

    bool operator==(const record_batch& other) const {
        return _header == other._header && _records == other._records;
    }

    bool operator!=(const record_batch& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_batch&);

    record_batch share() {
        return record_batch(
          _header, _records.share(0, _records.size_bytes()), _compressed);
    }

    /**
     * Set the batch max timestamp and recalculate checksums.
     *
     * The primary use case for this interface is supporting kafka's log append
     * time option which causes the max timestamp to be set at append time,
     * rather than at create time by the client.
     */
    void set_max_timestamp(timestamp_type ts_type, timestamp ts) {
        if (
          _header.attrs.timestamp_type() == ts_type
          && _header.max_timestamp == ts) {
            return;
        }
        _header.attrs.set_timestamp_type(ts_type);
        _header.max_timestamp = ts;
        _header.crc = model::crc_record_batch(*this);
        _header.header_crc = model::internal_header_only_crc(_header);
    }

    /**
     * Iterate over records with lazy record materialization.
     *
     * Use `model::for_each_record(..)` for futurized version.
     */
    template<typename Func>
    void for_each_record(Func f) const {
        auto it = record_batch_iterator::create(*this);
        while (it.has_next()) {
            if constexpr (std::is_void_v<
                            std::invoke_result_t<Func, model::record>>) {
                f(it.next());
            } else {
                ss::stop_iteration s = f(it.next());
                if (s == ss::stop_iteration::yes) {
                    return;
                }
            }
        }
    }

    template<typename Func>
    ss::future<> for_each_record_async(Func f) const {
        auto it = record_batch_iterator::create(*this);
        while (it.has_next()) {
            co_await ss::futurize_invoke(f, it.next());
        }
    }

    /**
     * Materialize records.
     *
     * Prefer lazy record construction via `for_each_record(..)` when accessing
     * records. However, some users explicitly store a single record in batches
     * and the looping construct to access that record is quite inconvenient.
     */
    std::vector<record> copy_records() const {
        std::vector<record> ret;
        ret.reserve(_header.record_count);
        for_each_record(
          [&ret](model::record&& r) { ret.push_back(std::move(r)); });
        return ret;
    }

    /**
     * Access raw record data.
     */
    const iobuf& data() const { return _records; }
    iobuf&& release_data() && { return std::move(_records); }
    void clear_data() { _records.clear(); }

    auto serde_fields() { return std::tie(_header, _records); }

    static model::record_batch
    serde_direct_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        auto header = read_nested<model::record_batch_header>(
          in, h._bytes_left_limit);
        auto data = read_nested<iobuf>(in, h._bytes_left_limit);

        return {header, std::move(data), tag_ctor_ng()};
    }

    static ss::future<model::record_batch>
    serde_async_direct_read(iobuf_parser& in, serde::header h) {
        using serde::read_async_nested;
        // TODO: change to coroutine after we upgrade to clang-16
        auto bytes_left_limit = h._bytes_left_limit;
        return read_async_nested<model::record_batch_header>(
                 in, bytes_left_limit)
          .then([&in, bytes_left_limit](model::record_batch_header header) {
              return read_async_nested<iobuf>(in, bytes_left_limit)
                .then([header](iobuf records) {
                    return model::record_batch{
                      header, std::move(records), tag_ctor_ng()};
                });
          });
    }

private:
    record_batch_header _header;
    iobuf _records;
    bool _compressed{false};

    record_batch(record_batch_header header, iobuf&& records, bool compressed)
      : _header(header)
      , _records(std::move(records))
      , _compressed(compressed) {}

    record_batch() = default;

    void verify_iterable() const {
        vassert(
          !_compressed,
          "Record iteration is not supported for compressed batches.");
    }

    explicit operator bool() const noexcept { return !empty(); }
    friend class ss::optimized_optional<record_batch>;
    friend class record_batch_iterator;

    template<typename Func>
    friend ss::future<>
    for_each_record(const model::record_batch& batch, Func&& f);
};

/**
 * Iterate over records with lazy record materialization.
 */
template<typename Func>
inline ss::future<>
for_each_record(const model::record_batch& batch, Func&& f) {
    return ss::do_with(
      record_batch_iterator::create(batch),
      record{},
      [f = std::forward<Func>(f)](
        record_batch_iterator& it, record& r) mutable {
          return ss::do_until(
            [&it]() { return !it.has_next(); },
            [&it, &r, f = std::forward<Func>(f)]() {
                r = it.next();
                return ss::futurize_invoke(f, r);
            });
      });
}

class record_batch_crc_checker {
public:
    explicit record_batch_crc_checker(bool verify_internal_header = true)
      : _verify_internal_header(verify_internal_header) {}

    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        bool header_crc_pass = true;
        if (_verify_internal_header) {
            header_crc_pass = rb.header().header_crc
                              == model::internal_header_only_crc(rb.header());
        }
        const bool crc_pass = rb.header().crc == crc_record_batch(rb);
        _crc_parse_success &= (header_crc_pass && crc_pass);
        return ss::make_ready_future<ss::stop_iteration>(
          _crc_parse_success ? ss::stop_iteration::no
                             : ss::stop_iteration::yes);
    }

    bool end_of_stream() { return _crc_parse_success; }

private:
    bool _crc_parse_success{true};
    bool _verify_internal_header{true};
};

} // namespace model
