// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_utils.h"

#include "hashing/crc32c.h"
#include "model/record.h"
#include "model/record_fields.h"
#include "utils/vint.h"

#include <type_traits>

namespace model {

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void crc_extend_cpu_to_le(crc::crc32c& crc, T i) {
    auto j = ss::cpu_to_le(i);
    crc.extend(j);
}

template<typename... T>
void crc_extend_all_cpu_to_le(crc::crc32c& crc, T... t) {
    ((crc_extend_cpu_to_le(crc, t)), ...);
}

/// \brief uint32_t because that's what crc32c uses
/// it is *only* record_batch_header.header_crc;
uint32_t internal_header_only_crc(const record_batch_header& header) {
    auto c = crc::crc32c();
    crc_extend_all_cpu_to_le(
      c,
      /*Additional fields*/
      header.size_bytes,
      header.base_offset(),
      static_cast<std::underlying_type_t<record_batch_type>>(header.type),
      header.crc,

      /*Below are same fields as kafka - but at no cost on x86 since they are
         hashed as little endian*/
      header.attrs.value(),
      header.last_offset_delta,
      header.first_timestamp.value(),
      header.max_timestamp.value(),
      header.producer_id,
      header.producer_epoch,
      header.base_sequence,
      header.record_count);
    return c.value();
}

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void crc_extend_cpu_to_be(crc::crc32c& crc, T i) {
    auto j = ss::cpu_to_be(i);
    crc.extend(j);
}

template<typename... T>
void crc_extend_all_cpu_to_be(crc::crc32c& crc, T... t) {
    ((crc_extend_cpu_to_be(crc, t)), ...);
}

void crc_record_batch_header(
  crc::crc32c& crc, const record_batch_header& header) {
    crc_extend_all_cpu_to_be(
      crc,
      header.attrs.value(),
      header.last_offset_delta,
      header.first_timestamp.value(),
      header.max_timestamp.value(),
      header.producer_id,
      header.producer_epoch,
      header.base_sequence,
      header.record_count);
}

uint32_t
crc_record_batch(const record_batch_header& hdr, const iobuf& records) {
    auto crc = crc::crc32c();
    crc_record_batch_header(crc, hdr);
    crc_extend_iobuf(crc, records);
    return crc.value();
}

uint32_t crc_record_batch(const record_batch& b) {
    return crc_record_batch(b.header(), b.data());
}

namespace {

// Materializes the record's key/value/headers zero-copy (via `share`) when
// given an `iobuf_parser`, and by copying when given an `iobuf_const_parser`.
template<typename Parser>
model::record do_parse_one_record(Parser& parser) {
    auto pr = parse_record_fields<
      model::record_field::size_bytes,
      model::record_field::attributes,
      model::record_field::timestamp_delta,
      model::record_field::offset_delta,
      model::record_field::key_size,
      model::record_field::key,
      model::record_field::value_size,
      model::record_field::value,
      model::record_field::headers>(parser);
    return {
      pr.size_bytes,
      pr.attributes,
      pr.timestamp_delta,
      pr.offset_delta,
      pr.key_size,
      std::move(pr.key),
      pr.value_size,
      std::move(pr.value),
      std::move(pr.headers)};
}

} // namespace

model::record parse_one_record_from_buffer(iobuf_parser& parser) {
    return do_parse_one_record(parser);
}

model::record parse_one_record_copy_from_buffer(iobuf_const_parser& parser) {
    return do_parse_one_record(parser);
}

static inline void append_vint_to_iobuf(iobuf& b, int64_t v) {
    auto vb = vint::to_bytes(v);
    b.append(vb.data(), vb.size());
}

void append_record_to_buffer(iobuf& a, const model::record& r) {
    a.reserve_memory(vint::max_length * 6);
    append_vint_to_iobuf(a, r.size_bytes());

    const auto attrs = ss::cpu_to_be(r.attributes().value());
    // NOLINTNEXTLINE
    a.append(reinterpret_cast<const char*>(&attrs), sizeof(attrs));

    append_vint_to_iobuf(a, r.timestamp_delta());
    append_vint_to_iobuf(a, r.offset_delta());

    auto key_bytes = std::max(r.key_size(), 0);
    auto val_bytes = std::max(r.value_size(), 0);
    a.reserve_memory(key_bytes + val_bytes);
    append_vint_to_iobuf(a, r.key_size());
    if (r.key_size() > 0) {
        for (auto& f : r.key()) {
            a.append(f.get(), f.size());
        }
    }
    append_vint_to_iobuf(a, r.value_size());
    if (r.value_size() > 0) {
        for (auto& f : r.value()) {
            a.append(f.get(), f.size());
        }
    }

    auto& hdrs = r.headers();
    append_vint_to_iobuf(a, hdrs.size());
    for (auto& h : hdrs) {
        append_vint_to_iobuf(a, h.key_size());
        a.reserve_memory(h.memory_usage());
        if (h.key_size() > 0) {
            for (auto& f : h.key()) {
                a.append(f.get(), f.size());
            }
        }
        append_vint_to_iobuf(a, h.value_size());
        if (h.value_size() > 0) {
            for (auto& f : h.value()) {
                a.append(f.get(), f.size());
            }
        }
    }
}

model::record_key_metadata parse_record_key_from_buffer(iobuf_const_parser& p) {
    auto pr = parse_record_fields<
      model::record_field::offset_delta,
      model::record_field::key_bytes,
      model::record_field::is_tombstone>(p);
    return {
      .offset_delta = pr.offset_delta,
      .is_tombstone = pr.is_tombstone,
      .key = std::move(pr.key_bytes)};
}

namespace {

template<bool FullyParse>
model::record_metadata do_parse_record_metadata(iobuf_const_parser& p) {
    auto pr = parse_record_fields<
      FullyParse,
      model::record_field::size_bytes,
      model::record_field::attributes,
      model::record_field::timestamp_delta,
      model::record_field::offset_delta>(p);
    return {pr.size_bytes, pr.attributes, pr.timestamp_delta, pr.offset_delta};
}

} // namespace

model::record_metadata parse_record_metadata_from_buffer(
  iobuf_const_parser& p, bool fully_parse_record) {
    // With `fully_parse_record` the entire record structure (key, value and
    // every header) is walked and validated without being materialized;
    // otherwise those fields are skipped over wholesale.
    return fully_parse_record ? do_parse_record_metadata<true>(p)
                              : do_parse_record_metadata<false>(p);
}

} // namespace model
