// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/async_adl_serde.h"

#include "model/adl_serde.h"
#include "model/record_batch_reader.h"

namespace reflection {
/**
 * My moving the individual record_batches into a std::vector, we can take
 * advantage of the async_adl<std::vector<T>> template specializtion.
 */
ss::future<> async_adl<model::record_batch_reader>::to(
  iobuf& out, model::record_batch_reader&& reader) {
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .then([&out](ss::circular_buffer<model::record_batch> data) {
          std::vector<model::record_batch> batches;
          batches.reserve(data.size());
          std::transform(
            std::make_move_iterator(data.begin()),
            std::make_move_iterator(data.end()),
            std::back_inserter(batches),
            [](model::record_batch&& rb) { return std::move(rb); });
          return async_adl<std::vector<model::record_batch>>{}.to(
            out, std::move(batches));
      });
}

ss::future<model::record_batch_reader>
async_adl<model::record_batch_reader>::from(iobuf_parser& in) {
    return async_adl<std::vector<model::record_batch>>{}.from(in).then(
      [](std::vector<model::record_batch> batches) {
          ss::circular_buffer<model::record_batch> cbuffer;
          cbuffer.reserve(batches.size());
          std::transform(
            std::make_move_iterator(batches.begin()),
            std::make_move_iterator(batches.end()),
            std::back_inserter(cbuffer),
            [](model::record_batch&& rb) { return std::move(rb); });
          return model::make_memory_record_batch_reader(std::move(cbuffer));
      });
}

} // namespace reflection
