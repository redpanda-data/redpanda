#pragma once

#include "bytes/iobuf.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "vassert.h"

#include <seastar/util/optimized_optional.hh>

#include <boost/range/numeric.hpp>

#include <bitset>
#include <cstdint>
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

    int32_t memory_usage() const {
        return (sizeof(int32_t) * 2) + _key.size_bytes() + _value.size_bytes();
    }
    record_header share() {
        return record_header(_key_size, share_key(), _val_size, share_value());
    }

    record_header foreign_share() {
        auto sh_key = iobuf_share_foreign_n(share_key(), 1);
        auto sh_val = iobuf_share_foreign_n(share_value(), 1);
        return record_header(
          _key_size,
          std::move(sh_key.back()),
          _val_size,
          std::move(sh_val.back()));
    }

    int32_t key_size() const { return _key_size; }
    const iobuf& key() const { return _key; }
    iobuf release_key() { return std::exchange(_key, {}); }
    iobuf share_key() { return _key.share(0, _key.size_bytes()); }

    int32_t value_size() const { return _val_size; }
    const iobuf& value() const { return _value; }
    iobuf release_value() { return std::exchange(_value, {}); }
    iobuf share_value() { return _value.share(0, _value.size_bytes()); }

    bool operator==(const record_header& rhs) const {
        return _key_size == rhs._key_size && _key == rhs._key
               && _val_size == rhs._val_size && _value == rhs._value;
    }

    friend std::ostream& operator<<(std::ostream&, const record_header&);

private:
    int32_t _key_size;
    iobuf _key;
    int32_t _val_size;
    iobuf _value;
};

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
      int32_t timestamp_delta,
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

    // Size in bytes of everything except the size_bytes field.
    int32_t size_bytes() const { return _size_bytes; }

    // Used for acquiring units from semaphores limiting
    // memory resources.
    int32_t memory_usage() const {
        return sizeof(record) + (_headers.size() * sizeof(record_header))
               + _size_bytes;
    }

    record_attributes attributes() const { return _attributes; }

    int32_t timestamp_delta() const { return _timestamp_delta; }

    int32_t offset_delta() const { return _offset_delta; }

    int32_t key_size() const { return _key_size; }
    const iobuf& key() const { return _key; }
    iobuf release_key() { return std::exchange(_key, {}); }
    iobuf share_key() { return _key.share(0, _key.size_bytes()); }

    int32_t value_size() const { return _val_size; }
    const iobuf& value() const { return _value; }
    iobuf release_value() { return std::exchange(_value, {}); }
    iobuf share_value() { return _value.share(0, _value.size_bytes()); }

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

    record foreign_share() {
        std::vector<record_header> copy;
        copy.reserve(_headers.size());
        for (auto& h : _headers) {
            copy.push_back(h.foreign_share());
        }
        auto sh_key = iobuf_share_foreign_n(share_key(), 1);
        auto sh_val = iobuf_share_foreign_n(share_value(), 1);
        return record(
          _size_bytes,
          _attributes,
          _timestamp_delta,
          _offset_delta,
          _key_size,
          std::move(sh_key.back()),
          _val_size,
          std::move(sh_val.back()),
          std::move(copy));
    }
    bool operator==(const record& other) const {
        return _size_bytes == other._size_bytes
               && _timestamp_delta == other._timestamp_delta
               && _offset_delta == other._offset_delta && _key == other._key
               && _value == other._value
               && _headers.size() == other._headers.size()
               && _headers == other._headers;
    }

    bool operator!=(const record& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream&, const record&);

private:
    int32_t _size_bytes{0};
    record_attributes _attributes;
    int32_t _timestamp_delta{0};
    int32_t _offset_delta{0};
    int32_t _key_size{0};
    iobuf _key;
    int32_t _val_size{0};
    iobuf _value;
    std::vector<record_header> _headers;
};

class record_batch_attributes final {
public:
    static constexpr uint16_t compression_mask = 0x7;
    static constexpr uint16_t timestamp_type_mask = 0x8;
    static constexpr uint16_t transactional_mask = 0x10;

    using type = int16_t;

    record_batch_attributes() noexcept = default;

    explicit record_batch_attributes(type v) noexcept
      : _attributes(v) {}

    type value() const { return static_cast<type>(_attributes.to_ulong()); }

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
        switch (maskable_value() & compression_mask) {
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
        }
        throw std::runtime_error("Unknown compression value");
    }

    model::timestamp_type timestamp_type() const {
        return _attributes.test(3) ? timestamp_type::append_time
                                   : timestamp_type::create_time;
    }

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

    record_batch_attributes& operator|=(model::timestamp_type ts_t) {
        _attributes |= (static_cast<uint64_t>(ts_t) << uint64_t(3))
                       & record_batch_attributes::timestamp_type_mask;
        return *this;
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

using record_batch_type = named_type<int8_t, struct model_record_batch_type>;

static constexpr std::array<record_batch_type, 4> well_known_record_batch_types{
  record_batch_type(),  // unknown - used for debugging
  record_batch_type(1), // raft::data
  record_batch_type(2), // raft::configuration
  record_batch_type(3)  // controller::*
};

/** expect all fields to be serialized, except context fields */
struct record_batch_header {
    struct context {
        model::term_id term;
    };

    int32_t size_bytes{0};
    offset base_offset;
    record_batch_type type; // redpanda extension
    int32_t crc{0};

    // -- below the CRC are checksummed

    record_batch_attributes attrs;
    int32_t last_offset_delta{0};
    timestamp first_timestamp;
    timestamp max_timestamp;
    int64_t producer_id{0};
    int16_t producer_epoch{0};
    int32_t base_sequence{0};
    int32_t record_count{0};

    /// context object with opaque environment data
    context ctx;

    record_batch_header() = default;
    offset last_offset() const {
        return base_offset + offset(last_offset_delta);
    }

    bool operator==(const record_batch_header& other) const {
        return size_bytes == other.size_bytes
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

// 57 bytes
constexpr uint32_t packed_record_batch_header_size
  = sizeof(model::record_batch_header::size_bytes)          // 4
    + sizeof(model::record_batch_header::base_offset)       // 8
    + sizeof(model::record_batch_type::type)                // 1
    + sizeof(model::record_batch_header::crc)               // 4
    + sizeof(model::record_batch_attributes::type)          // 2
    + sizeof(model::record_batch_header::last_offset_delta) // 4
    + sizeof(model::record_batch_header::first_timestamp)   // 8
    + sizeof(model::record_batch_header::max_timestamp)     // 8
    + sizeof(model::record_batch_header::producer_id)       // 8
    + sizeof(model::record_batch_header::producer_epoch)    // 2
    + sizeof(model::record_batch_header::base_sequence)     // 4
    + sizeof(model::record_batch_header::record_count);     // 4

class record_batch {
public:
    using uncompressed_records = std::vector<record>;
    using compressed_records = iobuf;
    using records_type = std::variant<uncompressed_records, compressed_records>;

    record_batch(record_batch_header header, records_type&& records) noexcept
      : _header(header)
      , _records(std::move(records)) {}
    record_batch(const record_batch& o) = delete;
    record_batch& operator=(const record_batch&) = delete;
    record_batch(record_batch&&) noexcept = default;
    record_batch& operator=(record_batch&&) noexcept = default;
    ~record_batch() = default;

    bool empty() const { return _header.record_count <= 0; }

    bool compressed() const {
        return std::holds_alternative<compressed_records>(_records);
    }

    int32_t record_count() const { return _header.record_count; }
    model::offset base_offset() const { return _header.base_offset; }
    model::offset last_offset() const { return _header.last_offset(); }
    model::term_id term() const { return _header.ctx.term; }
    void set_term(model::term_id i) { _header.ctx.term = i; }
    // Size in bytes of the header plus records.
    int32_t size_bytes() const { return _header.size_bytes; }

    int32_t memory_usage() const {
        return sizeof(*this)
               + ss::visit(
                 _records,
                 [](const compressed_records& records) -> int32_t {
                     return records.size_bytes();
                 },
                 [](const uncompressed_records& records) {
                     return boost::accumulate(
                       records, int32_t(0), [](int32_t usage, const record& r) {
                           return usage + r.memory_usage();
                       });
                 });
    }

    const record_batch_header& header() const { return _header; }
    record_batch_header& header() { return _header; }

    bool contains(model::offset offset) const {
        return _header.base_offset() <= offset
               && offset <= _header.last_offset();
    }

    uncompressed_records::const_iterator begin() const {
        verify_uncompressed_records();
        return std::get<uncompressed_records>(_records).begin();
    }
    uncompressed_records::const_iterator end() const {
        verify_uncompressed_records();
        return std::get<uncompressed_records>(_records).end();
    }
    uncompressed_records::iterator begin() {
        verify_uncompressed_records();
        return std::get<uncompressed_records>(_records).begin();
    }
    uncompressed_records::iterator end() {
        verify_uncompressed_records();
        return std::get<uncompressed_records>(_records).end();
    }

    // Can only be called if this holds compressed records.
    const compressed_records& get_compressed_records() const {
        return std::get<compressed_records>(_records);
    }
    compressed_records&& release() && {
        return std::move(std::get<compressed_records>(_records));
    }
    bool operator==(const record_batch& other) const {
        return _header == other._header && _records == other._records;
    }

    bool operator!=(const record_batch& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_batch&);

    uncompressed_records& get_uncompressed_records_for_testing() {
        verify_uncompressed_records();
        return std::get<uncompressed_records>(_records);
    }

    record_batch share() {
        record_batch_header h = _header;
        if (compressed()) {
            auto& recs = std::get<compressed_records>(_records);
            return record_batch(h, recs.share(0, recs.size_bytes()));
        }
        auto& originals = std::get<uncompressed_records>(_records);
        uncompressed_records r;
        r.reserve(originals.size());
        // share all individual records
        std::transform(
          originals.begin(),
          originals.end(),
          std::back_inserter(r),
          [](record& rec) -> record { return rec.share(); });

        return record_batch(h, std::move(r));
    }

    record_batch foreign_share() {
        record_batch_header h = _header;
        if (compressed()) {
            auto& recs = std::get<compressed_records>(_records);
            auto r_sh = iobuf_share_foreign_n(
              recs.share(0, recs.size_bytes()), 1);
            return record_batch(h, std::move(r_sh.back()));
        }
        auto& originals = std::get<uncompressed_records>(_records);
        uncompressed_records r;
        r.reserve(originals.size());
        // share all individual records
        std::transform(
          originals.begin(),
          originals.end(),
          std::back_inserter(r),
          [](record& rec) -> record { return rec.foreign_share(); });

        return record_batch(h, std::move(r));
    }

    void clear() {
        ss::visit(
          _records,
          [](compressed_records& r) { r = iobuf(); },
          [](uncompressed_records& u) { u.clear(); });
    }

private:
    record_batch_header _header;
    records_type _records;

    record_batch() = default;
    void verify_uncompressed_records() const {
        vassert(
          std::holds_alternative<uncompressed_records>(_records),
          "Iterators can only be called with uncompressed record variant.");
    }
    explicit operator bool() const noexcept { return !empty(); }
    friend class ss::optimized_optional<record_batch>;
};

using record_batch_opt = ss::optimized_optional<record_batch>;

/// Execute provided async action on each record of record batch

// clang-format off
template<typename Func>
CONCEPT(requires requires(Func f, model::record r) {
    { f(std::move(r)) } -> ss::future<>;
})
// clang-format on
inline ss::future<> consume_records(model::record_batch&& batch, Func&& f) {
    return ss::do_with(
      std::move(batch),
      [f = std::forward<Func>(f)](model::record_batch& batch) mutable {
          return ss::do_for_each(
            batch, [f = std::forward<Func>(f)](model::record& rec) mutable {
                return f(std::move(rec));
            });
      });
}

} // namespace model
