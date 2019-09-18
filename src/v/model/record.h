#pragma once

#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/fragbuf.h"

#include <seastar/util/optimized_optional.hh>

#include <boost/range/numeric.hpp>

#include <bitset>
#include <cstdint>
#include <variant>
#include <vector>

namespace model {

class record {
public:
    record() noexcept = default;

    record(
      uint32_t size_bytes,
      int32_t timestamp_delta,
      int32_t offset_delta,
      fragbuf key,
      fragbuf value_and_headers) noexcept
      : _size_bytes(size_bytes)
      , _timestamp_delta(timestamp_delta)
      , _offset_delta(offset_delta)
      , _key(std::move(key))
      , _value_and_headers(std::move(value_and_headers)) {
    }

    // Size in bytes of everything except the size_bytes field.
    uint32_t size_bytes() const {
        return _size_bytes;
    }

    // Used for acquiring units from semaphores limiting
    // memory resources.
    uint32_t memory_usage() const {
        return sizeof(*this) + _key.size_bytes()
               + _value_and_headers.size_bytes();
    }

    int32_t timestamp_delta() const {
        return _timestamp_delta;
    }

    int32_t offset_delta() const {
        return _offset_delta;
    }

    const fragbuf& key() const {
        return _key;
    }
    fragbuf&& release_key() {
        return std::move(_key);
    }

    const fragbuf& packed_value_and_headers() const {
        return _value_and_headers;
    }
    fragbuf&& release_packed_value_and_headers() {
        return std::move(_value_and_headers);
    }

    bool operator==(const record& other) const {
        return _size_bytes == other._size_bytes
               && _timestamp_delta == other._timestamp_delta
               && _offset_delta == other._offset_delta && _key == other._key
               && _value_and_headers == other._value_and_headers;
    }

    bool operator!=(const record& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const record&);

private:
    uint32_t _size_bytes;
    int32_t _timestamp_delta;
    int32_t _offset_delta;
    fragbuf _key;
    // Already contains the varint encoding of the
    // value size and of the header size.
    fragbuf _value_and_headers;
};

class record_batch_attributes final {
public:
    using value_type = uint16_t;

    record_batch_attributes() noexcept = default;

    explicit record_batch_attributes(int16_t v) noexcept
      : _attributes(v) {
    }

    value_type value() const {
        return static_cast<int16_t>(_attributes.to_ulong());
    }

    model::compression compression() const {
        switch (_attributes.to_ulong() & 0x7) {
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

    friend std::ostream&
    operator<<(std::ostream&, const record_batch_attributes&);

private:
    // Bits 4 and 5 are used by Kafka and thus reserved.
    std::bitset<16> _attributes;
};

struct record_batch_header {
    // Size of the batch minus this field.
    uint32_t size_bytes;
    offset base_offset;
    int32_t crc;
    record_batch_attributes attrs;
    int32_t last_offset_delta;
    timestamp first_timestamp;
    timestamp max_timestamp;

    record_batch_header() = default;
    offset last_offset() const {
        return base_offset + last_offset_delta;
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
        compressed_records(uint32_t size, fragbuf data) noexcept
          : _size(size)
          , _data(std::move(data)) {
        }

        compressed_records(compressed_records&& other) noexcept
          : _size(std::exchange(other._size, 0))
          , _data(std::move(other._data)) {
        }

        compressed_records& operator=(compressed_records&& other) noexcept {
            _size = std::exchange(other._size, 0);
            _data = std::move(other._data);
            return *this;
        }

        uint32_t size() const {
            return _size;
        }

        uint32_t size_bytes() const {
            return _data.size_bytes();
        }

        bool empty() const {
            return !_size;
        }

        const fragbuf& records() const {
            return _data;
        }

        fragbuf&& release() && {
            _size = 0;
            return std::move(_data);
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
        fragbuf _data;
    };

    using uncompressed_records = std::vector<record>;
    using records_type = std::variant<uncompressed_records, compressed_records>;

    record_batch(record_batch_header header, records_type&& records) noexcept
      : _header(std::move(header))
      , _records(std::move(records)) {
    }
    record_batch(record_batch&& o) noexcept
      : _header(std::move(o._header))
      , _records(std::move(o._records)) {
    }
    record_batch& operator=(record_batch&& o) noexcept {
        if (this != &o) {
            this->~record_batch();
            new (this) record_batch(std::move(o));
        }
        return *this;
    }
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
    uint32_t size_bytes() const {
        return _header.size_bytes;
    }

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

    offset base_offset() const {
        return _header.base_offset;
    }

    int32_t crc() const {
        return _header.crc;
    }

    record_batch_attributes attributes() const {
        return _header.attrs;
    }

    int32_t last_offset_delta() const {
        return _header.last_offset_delta;
    }

    timestamp first_timestamp() const {
        return _header.first_timestamp;
    }

    timestamp max_timestamp() const {
        return _header.max_timestamp;
    }

    offset last_offset() const {
        return _header.last_offset();
    }

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
    record_batch_header&& release_header() {
        return std::move(_header);
    }
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

private:
    record_batch_header _header;
    records_type _records;

    record_batch() = default;
    explicit operator bool() const noexcept {
        return size();
    }
    friend class optimized_optional<record_batch>;
};

using record_batch_opt = optimized_optional<record_batch>;

} // namespace model
