/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/serializer.h"

#include "cloud_topics/level_zero/common/extent_meta.h"
#include "model/timeout_clock.h"
#include "storage/record_batch_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace experimental::cloud_topics::l0 {

serialized_chunk::serialized_chunk(
  iobuf payload, chunked_vector<extent_meta> extents) noexcept
  : payload(std::move(payload))
  , extents(std::move(extents)) {}

/// Construct iobuf out of record_batch_reader.
/// Works for single ntp.
struct serializing_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto offset = _output.size_bytes();
        // The offset is supposed to be translated but it's still
        // represented using model::offset instead of kafka::offset.
        auto base = model::offset_cast(batch.base_offset());
        auto last = model::offset_cast(batch.last_offset());
        // vlog(cd_log.trace, "serializing consumer batch: {}", batch);
        auto hdr_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
        auto rec_iobuf = std::move(batch).release_data();

        // Propagate to the output
        _output.append(std::move(hdr_iobuf));
        _output.append(std::move(rec_iobuf));
        vassert(
          _output.size_bytes() >= offset,
          "Output size {} is smaller than offset {}",
          _output.size_bytes(),
          offset);
        auto extent_size = _output.size_bytes() - offset;
        _extents.emplace_back(extent_meta{
          .id = object_id{},
          .first_byte_offset = first_byte_offset_t(offset),
          .byte_range_size = byte_range_size_t(extent_size),
          .base_offset = base,
          .last_offset = last,
        });

        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    serialized_chunk end_of_stream() {
        return {std::move(_output), std::move(_extents)};
    }

    iobuf _output;
    chunked_vector<extent_meta> _extents;
};

ss::future<serialized_chunk>
serialize_batches(chunked_vector<model::record_batch> batches) {
    serializing_consumer consumer;
    for (auto& batch : batches) {
        auto stop = co_await consumer(std::move(batch));
        if (stop) {
            break;
        }
    }
    co_return consumer.end_of_stream();
}

} // namespace experimental::cloud_topics::l0
