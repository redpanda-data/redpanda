// Copyright 2020 Redpanda Data, Inc.
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
#include "reflection/seastar/circular_buffer.h"

namespace reflection {

ss::future<> async_adl<model::record_batch_reader>::to(
  iobuf& out, model::record_batch_reader&& reader) {
    return model::consume_reader_to_memory(std::move(reader), model::no_timeout)
      .then([&out](ss::circular_buffer<model::record_batch> data) {
          return async_adl<ss::circular_buffer<model::record_batch>>{}.to(
            out, std::move(data));
      });
}

ss::future<model::record_batch_reader>
async_adl<model::record_batch_reader>::from(iobuf_parser& in) {
    return async_adl<ss::circular_buffer<model::record_batch>>{}.from(in).then(
      [](ss::circular_buffer<model::record_batch> batches) {
          return model::make_memory_record_batch_reader(std::move(batches));
      });
}

} // namespace reflection
