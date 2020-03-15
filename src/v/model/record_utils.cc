#include "model/record_utils.h"

#include "utils/vint.h"

namespace model {
static inline void crc_extend_iobuf(crc32& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
static inline void crc_extend_vint(crc32& crc, vint::value_type v) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer{};
    const auto size = vint::serialize(v, encoding_buffer.begin());
    // NOLINTNEXTLINE
    crc.extend(reinterpret_cast<const uint8_t*>(encoding_buffer.data()), size);
}

template<typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>, T>>
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

template<typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>, T>>
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
} // namespace model
