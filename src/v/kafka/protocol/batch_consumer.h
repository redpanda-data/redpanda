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
#include "bytes/iobuf.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/protocol/response_writer.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"

namespace kafka {

/**
 * A record batch reader consumer that serializes a stream of batches to the
 * Kafka on-wire format. The primary use case for this is the fetch api which
 * returns a set of batches read from a redpanda log back to a kafka client.
 */
class kafka_batch_serializer {
public:
    struct result {
        iobuf data;
        uint32_t record_count;
        model::offset base_offset;
        model::offset last_offset;
    };

    kafka_batch_serializer() noexcept
      : _wr(_buf) {}

    kafka_batch_serializer(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(const kafka_batch_serializer& o) = delete;
    kafka_batch_serializer& operator=(kafka_batch_serializer&& o) = delete;

    kafka_batch_serializer(kafka_batch_serializer&& o) noexcept
      : _buf(std::move(o._buf))
      , _wr(_buf) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        if (unlikely(record_count_ == 0)) {
            _base_offset = batch.base_offset();
        }
        _last_offset = batch.last_offset();
        record_count_ += batch.record_count();
        write_batch(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    result end_of_stream() {
        return result{
          .data = std::move(_buf),
          .record_count = record_count_,
          .base_offset = _base_offset,
          .last_offset = _last_offset,
        };
    }

private:
    void write_batch(model::record_batch&& batch) {
        writer_serialize_batch(_wr, std::move(batch));
    }

private:
    iobuf _buf;
    response_writer _wr;
    model::offset _base_offset;
    model::offset _last_offset;
    uint32_t record_count_ = 0;
};

} // namespace kafka
