/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/serializer.h"

#include "cloud_topics/logger.h"
#include "model/timeout_clock.h"
#include "storage/record_batch_utils.h"

namespace experimental::cloud_topics::details {

/// Construct iobuf out of record_batch_reader.
/// Works for single ntp.
struct serializing_consumer {
    explicit serializing_consumer(
      iobuf* output_buf, chunked_vector<lw_placeholder>* batches)
      : _output(output_buf)
      , _batches(batches) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto offset = _output->size_bytes();
        auto base = batch.base_offset();
        auto num_records = batch.header().record_count;
        auto tmp_copy = batch.copy();
        vlog(cd_log.trace, "serializing consumer batch: {}", batch);
        auto hdr_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
        auto rec_iobuf = std::move(batch).release_data();

        // Propagate to the output
        _output->append(std::move(hdr_iobuf));
        _output->append(std::move(rec_iobuf));
        auto batch_size = _output->size_bytes() - offset;
        _batches->push_back({
          .num_records = num_records,
          .base = base,
          .size_bytes = batch_size,
          .physical_offset = offset,
        });

        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    bool end_of_stream() const { return false; }

    iobuf* _output;
    chunked_vector<lw_placeholder>* _batches;
};

ss::future<serialized_chunk>
serialize_in_memory_record_batch_reader(model::record_batch_reader rdr) {
    iobuf payload;
    chunked_vector<lw_placeholder> batches;
    serializing_consumer cons(&payload, &batches);
    // The result can be ignored because 'serializing_consumer::end_of_stream'
    // always returns 'false'
    std::ignore = co_await std::move(rdr).consume(cons, model::no_timeout);
    co_return serialized_chunk{
      .payload = std::move(payload),
      .batches = std::move(batches),
    };
}

} // namespace experimental::cloud_topics::details
