/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf_parser.h"
#include "kafka/protocol/types.h"
#include "kafka/types.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
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
 *   iobuf record_set; // May contain multiple batches, e.g., fetch_response
 *   while(!record_set.empty()) {
 *       kafka_batch_adapter kba;
 *       record_set = kba.adapt(std::move(record_set));
 *       do_something_with(kba.batch);
 *   }
 *
 * 1. require that v2_format is true
 * 2. require that valid_crc is true
 * 3. if kba.batch is empty then it signals that decoding failed. otherwise,
 *    it's good to go.
 * 4. if returned batch is not empty then it signals that more data was on the
 *    wire than a single batch.
 *
 * Note that the default constructed batch adapter is in an undefined state.
 */
class kafka_batch_adapter {
public:
    iobuf adapt(iobuf&&);

    bool v2_format;
    bool valid_crc;
    bool legacy_error{false};

    std::optional<model::record_batch> batch;

    void adapt_with_version(iobuf, api_version);

private:
    void verify_crc(int32_t, iobuf_parser);
    model::record_batch_header read_header(iobuf_parser&);
    void convert_message_set(storage::record_batch_builder&, iobuf, bool);
};

/*
 * Helper wrapper type that handles conversion when encoding/decoding produce
 * requests. This type replaces iobuf in the generated code for kafka request
 * types.
 */
struct produce_request_record_data {
    explicit produce_request_record_data(
      std::optional<iobuf>&& data, api_version version) {
        if (data) {
            adapter.adapt_with_version(std::move(*data), version);
        }
    }

    explicit produce_request_record_data(model::record_batch&& batch) {
        adapter.v2_format = true;
        adapter.valid_crc = true;
        adapter.batch = std::move(batch);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const produce_request_record_data& data) {
        // NOTE: this stream is intentially devoid of user data.
        fmt::print(
          os,
          "batch {} v2_format {} valid_crc {}",
          data.adapter.batch ? data.adapter.batch->size_bytes() : -1,
          data.adapter.v2_format,
          data.adapter.valid_crc);
        return os;
    }
    kafka_batch_adapter adapter;
};

} // namespace kafka
