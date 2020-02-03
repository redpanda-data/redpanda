#include "model/record_utils.h"

namespace model {

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
    crc.extend_vint(r.size_bytes());
    crc.extend_vint(r.attributes().value());
    crc.extend_vint(r.timestamp_delta());
    crc.extend_vint(r.offset_delta());
    crc.extend_vint(r.key_size());
    crc.extend(r.key());
    crc.extend_vint(r.value_size());
    crc.extend(r.value());
    crc.extend_vint(r.headers().size());
    for (auto& h : r.headers()) {
        crc.extend_vint(h.key_size());
        crc.extend(h.key());
        crc.extend_vint(h.value_size());
        crc.extend(h.value());
    }
}

void crc_record_batch(crc32& crc, const record_batch& b) {
    crc_record_batch_header(crc, b.header());
    if (b.compressed()) {
        crc.extend(b.get_compressed_records());
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
