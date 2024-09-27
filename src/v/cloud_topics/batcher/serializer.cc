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
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto offset = _output.size_bytes();
        auto base = batch.base_offset();
        auto num_records = batch.header().record_count;
        auto tmp_copy = batch.copy();
        vlog(cd_log.trace, "serializing consumer batch: {}", batch);
        auto hdr_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
        auto rec_iobuf = std::move(batch).release_data();

        // Propagate to the output
        _output.append(std::move(hdr_iobuf));
        _output.append(std::move(rec_iobuf));
        auto batch_size = _output.size_bytes() - offset;
        _batches.push_back({
          .num_records = num_records,
          .base = base,
          .size_bytes = batch_size,
          .physical_offset = offset,
        });

        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    serialized_chunk end_of_stream() {
        return {
          .payload = std::move(_output),
          .batches = std::move(_batches),
        };
    }

    iobuf _output;
    chunked_vector<lw_placeholder> _batches;
};

ss::future<serialized_chunk>
serialize_in_memory_record_batch_reader(model::record_batch_reader rdr) {
    co_return co_await std::move(rdr).consume(
      serializing_consumer{}, model::no_timeout);
}

} // namespace experimental::cloud_topics::details
