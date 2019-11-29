#pragma once

#include "bytes/iobuf.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

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
    using value_type = int8_t;

    record_attributes() noexcept = default;

    explicit record_attributes(int8_t v) noexcept
      : _attributes(v) {}

    value_type value() const {
        return static_cast<value_type>(_attributes.to_ulong());
    }

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

class record {
public:
    record() = default;
    record(record&&) noexcept = default;
    record& operator=(record&&) noexcept = default;
    record(const record&) = delete;
    record operator=(const record&) = delete;
    record(
      uint32_t size_bytes,
      record_attributes attributes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      iobuf key,
      iobuf value_and_headers) noexcept
      : _size_bytes(size_bytes)
      , _attributes(attributes)
      , _timestamp_delta(timestamp_delta)
      , _offset_delta(offset_delta)
      , _key(std::move(key))
      , _value_and_headers(std::move(value_and_headers)) {}

    // Size in bytes of everything except the size_bytes field.
    uint32_t size_bytes() const { return _size_bytes; }

    // Used for acquiring units from semaphores limiting
    // memory resources.
    uint32_t memory_usage() const {
        return sizeof(*this) + _key.size_bytes()
               + _value_and_headers.size_bytes();
    }

    record_attributes attributes() const { return _attributes; }

    int32_t timestamp_delta() const { return _timestamp_delta; }

    int32_t offset_delta() const { return _offset_delta; }

    const iobuf& key() const { return _key; }
    iobuf share_key() { return _key.share(); }

    const iobuf& packed_value_and_headers() const { return _value_and_headers; }
    iobuf share_packed_value_and_headers() {
        return _value_and_headers.share();
    }
    record share() {
        return record(
          _size_bytes,
          _attributes,
          _timestamp_delta,
          _offset_delta,
          _key.share(),
          _value_and_headers.share());
    }
    bool operator==(const record& other) const {
        return _size_bytes == other._size_bytes
               && _timestamp_delta == other._timestamp_delta
               && _offset_delta == other._offset_delta && _key == other._key
               && _value_and_headers == other._value_and_headers;
    }

    bool operator!=(const record& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream&, const record&);

private:
    uint32_t _size_bytes;
    record_attributes _attributes;
    int32_t _timestamp_delta;
    int32_t _offset_delta;
    iobuf _key;
    // Already contains the varint encoding of the
    // value size and of the header size.
    iobuf _value_and_headers;
};

class record_batch_attributes final {
public:
    static constexpr int16_t compression_mask = 0x7;
    static constexpr int16_t timestamp_type_mask = 0x8;
    using value_type = int16_t;

    record_batch_attributes() noexcept = default;

    explicit record_batch_attributes(int16_t v) noexcept
      : _attributes(v) {}

    value_type value() const {
        return static_cast<value_type>(_attributes.to_ulong());
    }

    model::compression compression() const {
        switch (_attributes.to_ulong() & compression_mask) {
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
        _attributes
          |= (static_cast<std::underlying_type_t<model::timestamp_type>>(ts_t)
              << 3)
             & record_batch_attributes::timestamp_type_mask;
        return *this;
    }

    friend std::ostream&
    operator<<(std::ostream&, const record_batch_attributes&);

private:
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

struct record_batch_header {
    // Size of the batch minus this field.
    uint32_t size_bytes;
    offset base_offset;
    record_batch_type type;
    int32_t crc;
    record_batch_attributes attrs;
    int32_t last_offset_delta;
    timestamp first_timestamp;
    timestamp max_timestamp;

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
               && max_timestamp == other.max_timestamp;
    }

    bool operator!=(const record_batch_header& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_batch_header&);
};

class record_batch {
public:
    // After moving, compressed_records is guaranteed to be empty().
    class compressed_records {
    public:
        compressed_records(uint32_t size, iobuf data) noexcept
          : _size(size)
          , _data(std::move(data)) {}
        compressed_records(const compressed_records&) = delete;
        compressed_records& operator=(const compressed_records&) = delete;
        compressed_records(compressed_records&&) noexcept = default;
        compressed_records& operator=(compressed_records&&) noexcept = default;

        uint32_t size() const { return _size; }

        uint32_t size_bytes() const { return _data.size_bytes(); }

        bool empty() const { return !_size; }

        const iobuf& records() const { return _data; }

        iobuf release() && {
            _size = 0;
            return std::move(_data);
        }
        compressed_records share() {
            return compressed_records(_size, _data.share());
        }
        bool operator==(const compressed_records& other) const {
            return _size == other._size && _data == other._data;
        }

        bool operator!=(const compressed_records& other) const {
            return !(*this == other);
        }

        friend std::ostream&
        operator<<(std::ostream&, const compressed_records&);

    private:
        uint32_t _size;
        iobuf _data;
    };

    using uncompressed_records = std::vector<record>;
    using records_type = std::variant<uncompressed_records, compressed_records>;

    record_batch(record_batch_header header, records_type&& records) noexcept
      : _header(std::move(header))
      , _records(std::move(records)) {}
    record_batch(const record_batch& o) = delete;
    record_batch& operator=(const record_batch&) = delete;
    record_batch(record_batch&&) noexcept = default;
    record_batch& operator=(record_batch&&) noexcept = default;

    bool empty() const {
        return seastar::visit(_records, [](auto& e) { return e.empty(); });
    }

    bool compressed() const {
        return std::holds_alternative<compressed_records>(_records);
    }

    uint32_t size() const {
        return seastar::visit(
          _records, [](auto& e) { return static_cast<uint32_t>(e.size()); });
    }

    // Size in bytes of the header plus records.
    uint32_t size_bytes() const { return _header.size_bytes; }

    uint32_t memory_usage() const {
        return sizeof(*this)
               + seastar::visit(
                 _records,
                 [](const compressed_records& records) {
                     return records.size_bytes();
                 },
                 [](const uncompressed_records& records) {
                     return boost::accumulate(
                       records,
                       uint32_t(0),
                       [](uint32_t usage, const record& r) {
                           return usage + r.memory_usage();
                       });
                 });
    }

    offset base_offset() const { return _header.base_offset; }
    record_batch_type type() const { return _header.type; }
    int32_t crc() const { return _header.crc; }

    record_batch_attributes attributes() const { return _header.attrs; }

    int32_t last_offset_delta() const { return _header.last_offset_delta; }

    timestamp first_timestamp() const { return _header.first_timestamp; }

    timestamp max_timestamp() const { return _header.max_timestamp; }

    offset last_offset() const { return _header.last_offset(); }

    // Can only be called if this holds a set of uncompressed records.
    uncompressed_records::const_iterator begin() const {
        return std::get<uncompressed_records>(_records).begin();
    }

    // Can only be called if this holds a set of uncompressed records.
    uncompressed_records::const_iterator end() const {
        return std::get<uncompressed_records>(_records).end();
    }
    // Can only be called if this holds a set of uncompressed records.
    uncompressed_records::iterator begin() {
        return std::get<uncompressed_records>(_records).begin();
    }

    // Can only be called if this holds a set of uncompressed records.
    uncompressed_records::iterator end() {
        return std::get<uncompressed_records>(_records).end();
    }

    // Can only be called if this holds compressed records.
    const compressed_records& get_compressed_records() const {
        return std::get<compressed_records>(_records);
    }
    compressed_records&& release() && {
        return std::move(std::get<compressed_records>(_records));
    }
    record_batch_header&& release_header() { return std::move(_header); }
    bool operator==(const record_batch& other) const {
        return _header == other._header && _records == other._records;
    }

    bool operator!=(const record_batch& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record_batch&);

    uncompressed_records& get_uncompressed_records_for_testing() {
        return std::get<uncompressed_records>(_records);
    }

    record_batch_header& get_header_for_testing() { return _header; }

    record_batch share() {
        record_batch_header h = _header;
        if (compressed()) {
            auto& recs = std::get<compressed_records>(_records);
            return record_batch(h, recs.share());
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

        return record_batch(std::move(h), std::move(r));
    }

private:
    record_batch_header _header;
    records_type _records;

    record_batch() = default;
    explicit operator bool() const noexcept { return size(); }
    friend class optimized_optional<record_batch>;
};

using record_batch_opt = optimized_optional<record_batch>;

} // namespace model
