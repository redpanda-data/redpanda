#pragma once

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "utils/fragbuf.h"
#include "utils/vint.h"

namespace kafka::requests {

namespace internal {

constexpr size_t kafka_header_size = sizeof(int64_t) + // base offset
                                         sizeof(int32_t) + // batch length
                                         sizeof(int32_t)
                                         + // partition leader epoch
                                         sizeof(int8_t) +  // magic
                                         sizeof(int32_t) + // crc
                                         sizeof(int16_t) + // attributes
                                         sizeof(int32_t) + // last offset delta
                                         sizeof(int64_t) + // first timestamp
                                         sizeof(int64_t) + // max timestamp
                                         sizeof(int64_t) + // producer id
                                         sizeof(int16_t) + // producer epoch
                                         sizeof(int32_t) + // base sequence
                                         sizeof(int32_t);  // num records

constexpr size_t kafka_header_overhead =
sizeof(int32_t) + // The batch length
sizeof(int32_t) + // The partition leader epoch
sizeof(int8_t) + // The magic value
sizeof(int64_t) + // The producer id
sizeof(int16_t) + // The producer epoch
sizeof(int32_t);  // The base sequence

} // namespace internal

model::record_batch_reader reader_from_kafka_batch(fragbuf&&);

} // namespace kafka::requests