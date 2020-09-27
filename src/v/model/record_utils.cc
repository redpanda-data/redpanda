#include "model/record_utils.h"

#include "bytes/utils.h"
#include "model/record.h"
#include "reflection/adl.h"
#include "utils/vint.h"

namespace model {
static inline void crc_extend_vint(crc32& crc, int64_t v) {
    auto b = vint::to_bytes(v);
    crc.extend(b.data(), b.size());
}

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void crc_extend_cpu_to_le(crc32& crc, T i) {
    auto j = ss::cpu_to_le(i);
    crc.extend(j);
}

template<typename... T>
void crc_extend_all_cpu_to_le(crc32& crc, T... t) {
    ((crc_extend_cpu_to_le(crc, t)), ...);
}

/// \brief uint32_t because that's what crc32c uses
/// it is *only* record_batch_header.header_crc;
uint32_t internal_header_only_crc(const record_batch_header& header) {
    auto c = crc32();
    crc_extend_all_cpu_to_le(
      c,
      /*Additional fields*/
      header.size_bytes,
      header.base_offset(),
      header.type(),
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
void crc_extend_cpu_to_be(crc32& crc, T i) {
    auto j = ss::cpu_to_be(i);
    crc.extend(j);
}

template<typename... T>
void crc_extend_all_cpu_to_be(crc32& crc, T... t) {
    ((crc_extend_cpu_to_be(crc, t)), ...);
}

void crc_record_batch_header(crc32& crc, const record_batch_header& header) {
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

void crc_record(crc32& crc, const record& r) {
    crc_extend_vint(crc, r.size_bytes());
    crc_extend_vint(crc, r.attributes().value());
    crc_extend_vint(crc, r.timestamp_delta());
    crc_extend_vint(crc, r.offset_delta());
    crc_extend_vint(crc, r.key_size());
    crc_extend_iobuf(crc, r.key());
    crc_extend_vint(crc, r.value_size());
    crc_extend_iobuf(crc, r.value());
    crc_extend_vint(crc, r.headers().size());
    for (auto& h : r.headers()) {
        crc_extend_vint(crc, h.key_size());
        crc_extend_iobuf(crc, h.key());
        crc_extend_vint(crc, h.value_size());
        crc_extend_iobuf(crc, h.value());
    }
}

void crc_record_batch(crc32& crc, const record_batch& b) {
    crc_record_batch_header(crc, b.header());
    if (b.compressed()) {
        crc_extend_iobuf(crc, b.get_compressed_records());
    } else {
        for (auto& r : b) {
            crc_record(crc, r);
        }
    }
}

int32_t crc_record_batch(const record_batch& b) {
    auto c = crc32();
    crc_record_batch(c, b);
    return c.value();
}

int32_t recompute_record_batch_size(const record_batch& b) {
    int32_t retval = model::packed_record_batch_header_size;
    if (b.compressed()) {
        return retval + b.get_compressed_records().size_bytes();
    }
    for (auto& r : b) {
        retval += r.size_bytes();
        retval += vint::vint_size(r.size_bytes());
    }
    return retval;
}

static std::vector<model::record_header>
parse_record_headers(iobuf_parser& parser) {
    std::vector<model::record_header> headers;
    auto [header_count, _] = parser.read_varlong();
    headers.reserve(header_count);
    for (int i = 0; i < header_count; ++i) {
        auto [key_length, kv] = parser.read_varlong();
        iobuf key;
        if (key_length > 0) {
            key = parser.share(key_length);
        }
        auto [value_length, vv] = parser.read_varlong();
        iobuf value;
        if (value_length > 0) {
            value = parser.share(value_length);
        }
        headers.emplace_back(model::record_header(
          key_length, std::move(key), value_length, std::move(value)));
    }
    return headers;
}

static model::record do_parse_one_record_from_buffer(
  iobuf_parser& parser,
  int32_t record_size,
  model::record_attributes::type attr) {
    auto [timestamp_delta, tv] = parser.read_varlong();
    auto [offset_delta, ov] = parser.read_varlong();
    auto [key_length, kv] = parser.read_varlong();
    iobuf key;
    if (key_length > 0) {
        key = parser.share(key_length);
    }
    auto [value_length, vv] = parser.read_varlong();
    iobuf value;
    if (value_length > 0) {
        value = parser.share(value_length);
    }
    auto headers = parse_record_headers(parser);
    return model::record(
      record_size,
      model::record_attributes(attr),
      static_cast<int64_t>(timestamp_delta),
      static_cast<int32_t>(offset_delta),
      key_length,
      std::move(key),
      value_length,
      std::move(value),
      std::move(headers));
}

model::record parse_one_record_from_buffer(iobuf_parser& parser) {
    /*
     * require that record attributes be unaffected by endianness. all of the
     * other record fields are properly handled by virtue of their types being
     * either blobs or variable length integers.
     */
    static_assert(
      sizeof(model::record_attributes::type) == 1,
      "model attributes expected to be one byte");
    auto [record_size, rv] = parser.read_varlong();
    auto attr = parser.consume_type<model::record_attributes::type>();
    return do_parse_one_record_from_buffer(parser, record_size, attr);
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

    a.reserve_memory(r.key_size() + r.value_size());
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

} // namespace model
