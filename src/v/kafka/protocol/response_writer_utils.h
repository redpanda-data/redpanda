/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/protocol/response_writer.h"
#include "model/record.h"

#include <cstdint>

namespace kafka {

inline void
writer_serialize_batch(response_writer& w, model::record_batch&& batch) {
    /*
     * calculate batch size expected by kafka client.
     *
     * 1. records_size = batch.size_bytes() - RP header size;
     * 2. kafka_total = records_size + kafka header size;
     * 3. batch_size = kafka_total - sizeof(offset) - sizeof(length);
     *
     * The records size in (1) is computed correctly because RP batch size
     * is defined as the RP header size plus the size of the records. Unlike
     * the kafka batch size described below, RP batch size includes the size
     * of the length field itself.
     *
     * The adjustment in (3) is because the batch size given in the kafka
     * header does not include the offset preceeding the length field nor
     * the size of the length field itself.
     */
    auto size = batch.size_bytes() - model::packed_record_batch_header_size
                + internal::kafka_header_size - sizeof(int64_t)
                - sizeof(int32_t);

    w.write(int64_t(batch.base_offset()));
    w.write(int32_t(size)); // batch length
    w.write(int32_t(0));    // partition leader epoch
    w.write(int8_t(2));     // magic
    w.write(batch.header().crc);
    w.write(int16_t(batch.header().attrs.value()));
    w.write(int32_t(batch.header().last_offset_delta));
    w.write(int64_t(batch.header().first_timestamp.value()));
    w.write(int64_t(batch.header().max_timestamp.value()));
    w.write(int64_t(batch.header().producer_id));
    w.write(int16_t(batch.header().producer_epoch));
    w.write(int32_t(batch.header().base_sequence));
    w.write(int32_t(batch.record_count()));
    w.write_direct(std::move(batch).release_data());
}

} // namespace kafka
