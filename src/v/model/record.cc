/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/record.h"

#include "serde/serde_exception.h"

#include <array>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

namespace model {

namespace {

constexpr size_t record_batch_header_serde_fields_size
  = sizeof(uint32_t)                         // header_crc
    + sizeof(int32_t)                        // size_bytes
    + sizeof(int64_t)                        // base_offset
    + sizeof(serde::serde_enum_serialized_t) // type
    + sizeof(uint32_t)                       // crc
    + sizeof(uint64_t)                       // attrs
    + sizeof(int32_t)                        // last_offset_delta
    + sizeof(int64_t)                        // first_timestamp
    + sizeof(int64_t)                        // max_timestamp
    + sizeof(int64_t)                        // producer_id
    + sizeof(int16_t)                        // producer_epoch
    + sizeof(int32_t)                        // base_sequence
    + sizeof(int32_t);                       // record_count

constexpr size_t serde_envelope_header_size = 2 * sizeof(serde::version_t)
                                              + sizeof(serde::serde_size_t);
constexpr size_t record_batch_header_context_body_size = sizeof(int64_t);
constexpr size_t record_batch_header_context_encoded_size
  = serde_envelope_header_size + record_batch_header_context_body_size;
constexpr size_t record_batch_header_body_size
  = record_batch_header_serde_fields_size
    + record_batch_header_context_encoded_size;
constexpr size_t record_batch_prefix_size
  = record_batch_header_serde_fields_size
    + record_batch_header_context_encoded_size + sizeof(serde::serde_size_t);

record_batch_type
decode_serde_record_batch_type(serde::serde_enum_serialized_t value) {
    if (
      unlikely(
        std::cmp_greater(
          value,
          std::numeric_limits<
            std::underlying_type_t<record_batch_type>>::max()))) {
        throw serde::serde_exception("record batch type is out of range");
    }
    return static_cast<record_batch_type>(value);
}

} // namespace

bool record_batch_copy_iterator::has_next() const noexcept {
    return _index < _record_count;
}

model::record record_batch_copy_iterator::next() {
    auto r = model::parse_one_record_copy_from_buffer(_parser);
    ++_index;
    // if we're done, then check that we read all the buffer
    if (!has_next() && _parser.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Record iteration stopped with {} bytes remaining",
            _parser.bytes_left()));
    }
    return r;
}

record_batch_copy_iterator
record_batch_copy_iterator::create(const model::record_batch& b) {
    b.verify_iterable();
    return {b.record_count(), iobuf_const_parser(b._records)};
}

record_batch_copy_iterator::record_batch_copy_iterator(
  int32_t rc, iobuf_const_parser p)
  : _record_count(rc)
  , _parser(std::move(p)) {}

bool record_batch_iterator::has_next() const noexcept {
    return _index < _record_count;
}

model::record record_batch_iterator::next() {
    auto r = model::parse_one_record_from_buffer(_parser);
    ++_index;
    // if we're done, then check that we read all the buffer
    if (!has_next() && _parser.bytes_left()) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "Record iteration stopped with {} bytes remaining",
            _parser.bytes_left()));
    }
    return r;
}

record_batch_iterator record_batch_iterator::create(model::record_batch&& b) {
    b.verify_iterable();
    return {b.record_count(), iobuf_parser(std::move(b).release_data())};
}

record_batch_iterator::record_batch_iterator(int32_t rc, iobuf_parser p)
  : _record_count(rc)
  , _parser(std::move(p)) {}

fmt::iterator tx_range::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "pid: {}, range: [{}, {}]", pid, first, last);
}

packed_record_batch_header
pack_record_batch_header(const record_batch_header& header) {
    packed_record_batch_header encoded;
    char* cursor = encoded.data();
    auto write_le = [&cursor](auto value) {
        const auto little_endian = ss::cpu_to_le(value);
        std::memcpy(cursor, &little_endian, sizeof(little_endian));
        cursor += sizeof(little_endian);
    };

    write_le(header.header_crc);
    write_le(header.size_bytes);
    write_le(header.base_offset());
    write_le(
      static_cast<std::underlying_type_t<record_batch_type>>(header.type));
    write_le(header.crc);
    write_le(header.attrs.value());
    write_le(header.last_offset_delta);
    write_le(header.first_timestamp.value());
    write_le(header.max_timestamp.value());
    write_le(header.producer_id);
    write_le(header.producer_epoch);
    write_le(header.base_sequence);
    write_le(header.record_count);
    return encoded;
}

record_batch_header
unpack_record_batch_header(const packed_record_batch_header& encoded) {
    const char* cursor = encoded.data();
    auto read_le = [&cursor]<typename T>() {
        T value;
        std::memcpy(&value, cursor, sizeof(value));
        cursor += sizeof(value);
        return ss::le_to_cpu(value);
    };

    return record_batch_header{
      .header_crc = read_le.operator()<uint32_t>(),
      .size_bytes = read_le.operator()<int32_t>(),
      .base_offset = model::offset(read_le.operator()<int64_t>()),
      .type = static_cast<model::record_batch_type>(
        read_le.operator()<std::underlying_type_t<record_batch_type>>()),
      .crc = read_le.operator()<uint32_t>(),
      .attrs = model::record_batch_attributes(read_le.operator()<int16_t>()),
      .last_offset_delta = read_le.operator()<int32_t>(),
      .first_timestamp = model::timestamp(read_le.operator()<int64_t>()),
      .max_timestamp = model::timestamp(read_le.operator()<int64_t>()),
      .producer_id = read_le.operator()<int64_t>(),
      .producer_epoch = read_le.operator()<int16_t>(),
      .base_sequence = read_le.operator()<int32_t>(),
      .record_count = read_le.operator()<int32_t>()};
}
record_batch_header record_batch_header::serde_direct_read(
  iobuf_parser& in, const serde::header& envelope) {
    record_batch_header header{};
    const auto available = in.bytes_left() - envelope._bytes_left_limit;

    if (available < record_batch_header_serde_fields_size) [[unlikely]] {
        auto read = [&in, &envelope](auto& field) {
            if (in.bytes_left() == envelope._bytes_left_limit) {
                return false;
            }
            using serde::read_nested;
            read_nested(in, field, envelope._bytes_left_limit);
            return true;
        };

        if (
          !read(header.header_crc) || !read(header.size_bytes)
          || !read(header.base_offset) || !read(header.type)
          || !read(header.crc) || !read(header.attrs)
          || !read(header.last_offset_delta) || !read(header.first_timestamp)
          || !read(header.max_timestamp) || !read(header.producer_id)
          || !read(header.producer_epoch) || !read(header.base_sequence)
          || !read(header.record_count)) {
            return header;
        }
    } else {
        std::array<char, record_batch_header_serde_fields_size> encoded;
        in.consume_to(encoded.size(), encoded.begin());
        const char* cursor = encoded.data();
        auto read_le = [&cursor]<typename T>() {
            T value;
            std::memcpy(&value, cursor, sizeof(value));
            cursor += sizeof(value);
            return ss::le_to_cpu(value);
        };

        header.header_crc = read_le.operator()<uint32_t>();
        header.size_bytes = read_le.operator()<int32_t>();
        header.base_offset = model::offset(read_le.operator()<int64_t>());
        header.type = decode_serde_record_batch_type(
          read_le.operator()<serde::serde_enum_serialized_t>());
        header.crc = read_le.operator()<uint32_t>();
        header.attrs = model::record_batch_attributes(
          static_cast<model::record_batch_attributes::type>(
            read_le.operator()<uint64_t>()));
        header.last_offset_delta = read_le.operator()<int32_t>();
        header.first_timestamp = model::timestamp(
          read_le.operator()<int64_t>());
        header.max_timestamp = model::timestamp(read_le.operator()<int64_t>());
        header.producer_id = read_le.operator()<int64_t>();
        header.producer_epoch = read_le.operator()<int16_t>();
        header.base_sequence = read_le.operator()<int32_t>();
        header.record_count = read_le.operator()<int32_t>();
    }

    if (in.bytes_left() > envelope._bytes_left_limit) {
        serde::read_nested(in, header.ctx, envelope._bytes_left_limit);
    }
    return header;
}

ss::future<record_batch> record_batch::serde_async_direct_read(
  iobuf_parser& in, serde::header envelope) {
    const auto header_envelope = serde::read_header<record_batch_header>(
      in, envelope._bytes_left_limit);
    const auto header_body_size = in.bytes_left()
                                  - header_envelope._bytes_left_limit;

    if (
      envelope._version == record_batch::redpanda_serde_version
      && envelope._compat_version == record_batch::redpanda_serde_compat_version
      && header_envelope._version == record_batch_header::redpanda_serde_version
      && header_envelope._compat_version
           == record_batch_header::redpanda_serde_compat_version
      && header_body_size == record_batch_header_body_size
      && in.bytes_left() - envelope._bytes_left_limit
           >= record_batch_prefix_size) {
        std::array<char, record_batch_prefix_size> encoded;
        in.consume_to(encoded.size(), encoded.begin());
        const char* cursor = encoded.data();
        auto read_le = [&cursor]<typename T>() {
            T value;
            std::memcpy(&value, cursor, sizeof(value));
            cursor += sizeof(value);
            return ss::le_to_cpu(value);
        };

        record_batch_header header{};
        header.header_crc = read_le.operator()<uint32_t>();
        header.size_bytes = read_le.operator()<int32_t>();
        header.base_offset = model::offset(read_le.operator()<int64_t>());
        header.type = decode_serde_record_batch_type(
          read_le.operator()<serde::serde_enum_serialized_t>());
        header.crc = read_le.operator()<uint32_t>();
        header.attrs = model::record_batch_attributes(
          static_cast<model::record_batch_attributes::type>(
            read_le.operator()<uint64_t>()));
        header.last_offset_delta = read_le.operator()<int32_t>();
        header.first_timestamp = model::timestamp(
          read_le.operator()<int64_t>());
        header.max_timestamp = model::timestamp(read_le.operator()<int64_t>());
        header.producer_id = read_le.operator()<int64_t>();
        header.producer_epoch = read_le.operator()<int16_t>();
        header.base_sequence = read_le.operator()<int32_t>();
        header.record_count = read_le.operator()<int32_t>();

        const auto context_version = read_le.operator()<serde::version_t>();
        const auto context_compat_version
          = read_le.operator()<serde::version_t>();
        const auto context_size = read_le.operator()<serde::serde_size_t>();
        if (
          context_compat_version
            > record_batch_header::context::redpanda_serde_version
          || context_version
               < record_batch_header::context::redpanda_serde_compat_version
          || context_size != record_batch_header_context_body_size) {
            throw serde::serde_exception(
              "unexpected record batch header context layout");
        }
        header.ctx.term = model::term_id(read_le.operator()<int64_t>());
        header.ctx.owner_shard = ss::this_shard_id();

        const auto records_size = read_le.operator()<serde::serde_size_t>();
        if (records_size > in.bytes_left() - envelope._bytes_left_limit)
          [[unlikely]] {
            throw serde::serde_exception(
              "record batch data exceeds its envelope");
        }

        return ss::make_ready_future<record_batch>(
          record_batch{header, in.share(records_size), tag_ctor_ng()});
    }

    auto header = record_batch_header::serde_direct_read(in, header_envelope);
    if (in.bytes_left() > header_envelope._bytes_left_limit) {
        in.skip(in.bytes_left() - header_envelope._bytes_left_limit);
    }
    return serde::read_async_nested<iobuf>(in, envelope._bytes_left_limit)
      .then([header](iobuf records) {
          return record_batch{header, std::move(records), tag_ctor_ng()};
      });
}

void record_batch_header::reset_size_checksum_metadata(const iobuf& records) {
    size_bytes = model::packed_record_batch_header_size + records.size_bytes();
    crc = model::crc_record_batch(*this, records);
    header_crc = model::internal_header_only_crc(*this);
}

} // namespace model
