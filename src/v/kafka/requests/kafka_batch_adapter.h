#pragma once

#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "utils/vint.h"

namespace kafka {

namespace internal {

constexpr size_t kafka_header_size = sizeof(int64_t) + // base offset
                                     sizeof(int32_t) + // batch length
                                     sizeof(int32_t) + // partition leader epoch
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

} // namespace internal

/**
 * Usage:
 *
 *   kafka_batch_adapter kba;
 *   kba.adapt(std::move(batch));
 *
 * 1. require that v2_format is true
 * 2. require that valid_crc is true
 * 3. if batch is empty then it signals that more data was on the wire than the
 *    expected single batch. otherwise, its good to go.
 *
 * Note that the default constructed batch adapter is in an undefined state.
 */
class kafka_batch_adapter {
public:
    void adapt(iobuf&&);

    bool v2_format;
    bool valid_crc;

    std::optional<model::record_batch> batch;

private:
    void verify_crc(int32_t, iobuf_parser);
    model::record_batch_header read_header(iobuf_parser&);
};

} // namespace kafka
