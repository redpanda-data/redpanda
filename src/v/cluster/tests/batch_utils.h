#pragma once
#include "hashing/crc32c.h"
#include "model/record.h"

inline int32_t checksum_batch(model::record_batch& batch) {
    crc32 crc;
    storage::crc_batch_header(
      crc, batch.get_header_for_testing(), batch.size());
    for (auto const& r : batch.get_uncompressed_records_for_testing()) {
        storage::crc_record_header_and_key(
          crc,
          r.size_bytes(),
          r.attributes(),
          r.timestamp_delta(),
          r.offset_delta(),
          r.key());
        crc.extend(r.packed_value_and_headers());
    }
    return crc.value();
}