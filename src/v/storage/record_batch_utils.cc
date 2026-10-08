// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/record_batch_utils.h"

#include "model/record.h"
#include "reflection/adl.h"

#include <cstring>
#include <span>

namespace storage {

namespace {

/// Minimal parser over a contiguous buffer, exposing just enough of the
/// iobuf_parser interface for reflection::adl to decode fixed-layout scalar
/// fields.
class buffer_parser {
public:
    explicit buffer_parser(std::span<const char> buf)
      : _buf(buf)
      , _size(buf.size()) {}

    template<typename T>
    T consume_type() {
        vassert(
          _buf.size() >= sizeof(T),
          "buffer_parser over-read: need {} bytes, have {}",
          sizeof(T),
          _buf.size());
        T v;
        std::memcpy(&v, _buf.data(), sizeof(T));
        _buf = _buf.subspan(sizeof(T));
        return v;
    }

    size_t bytes_consumed() const { return _size - _buf.size(); }

private:
    std::span<const char> _buf;
    size_t _size;
};

template<typename Parser>
model::record_batch_header parse_header(Parser& parser) {
    auto header_crc = reflection::adl<uint32_t>{}.from(parser);
    auto sz = reflection::adl<int32_t>{}.from(parser);
    using offset_t = model::offset::type;
    auto off = model::offset(reflection::adl<offset_t>{}.from(parser));
    auto type = reflection::adl<model::record_batch_type>{}.from(parser);
    auto crc = reflection::adl<uint32_t>{}.from(parser);
    using attr_t = model::record_batch_attributes::type;
    auto attrs = model::record_batch_attributes(
      reflection::adl<attr_t>{}.from(parser));
    auto delta = reflection::adl<int32_t>{}.from(parser);
    using tmstmp_t = model::timestamp::type;
    auto first = model::timestamp(reflection::adl<tmstmp_t>{}.from(parser));
    auto max = model::timestamp(reflection::adl<tmstmp_t>{}.from(parser));
    auto producer_id = reflection::adl<int64_t>{}.from(parser);
    auto producer_epoch = reflection::adl<int16_t>{}.from(parser);
    auto base_sequence = reflection::adl<int32_t>{}.from(parser);
    auto record_count = reflection::adl<int32_t>{}.from(parser);
    vassert(
      parser.bytes_consumed() == model::packed_record_batch_header_size,
      "Error in header parsing. Must consume:{} bytes, but consumed:{}",
      model::packed_record_batch_header_size,
      parser.bytes_consumed());
    auto hdr = model::record_batch_header{
      .header_crc = header_crc,
      .size_bytes = sz,
      .base_offset = off,
      .type = type,
      .crc = crc,
      .attrs = attrs,
      .last_offset_delta = delta,
      .first_timestamp = first,
      .max_timestamp = max,
      .producer_id = producer_id,
      .producer_epoch = producer_epoch,
      .base_sequence = base_sequence,
      .record_count = record_count};
    return hdr;
}

} // namespace

iobuf batch_header_to_disk_iobuf(const model::record_batch_header& h) {
    iobuf b;
    reflection::serialize(
      b,
      h.header_crc,
      h.size_bytes,
      h.base_offset(),
      h.type,
      h.crc,
      h.attrs.value(),
      h.last_offset_delta,
      h.first_timestamp.value(),
      h.max_timestamp.value(),
      h.producer_id,
      h.producer_epoch,
      h.base_sequence,
      h.record_count);
    vassert(
      b.size_bytes() == model::packed_record_batch_header_size,
      "disk headers must be of static size:{}, but got{}",
      model::packed_record_batch_header_size,
      b.size_bytes());
    return b;
}

model::record_batch_header batch_header_from_disk_iobuf(iobuf b) {
    iobuf_const_parser parser(b);
    return parse_header(parser);
}

model::record_batch_header
batch_header_from_disk_buf(std::span<const char> data) {
    vassert(
      data.size() == model::packed_record_batch_header_size,
      "disk headers must be of static size {}, but got {}",
      model::packed_record_batch_header_size,
      data.size());
    buffer_parser parser(data);
    return parse_header(parser);
}

} // namespace storage
