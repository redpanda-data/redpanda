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

#include <seastar/coroutine/maybe_yield.hh>

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

ss::future<>
async_adl<model::record_batch>::to(iobuf& out, model::record_batch&& batch) {
    batch_header hdr{
      .bhdr = batch.header(),
      .is_compressed = static_cast<int8_t>(batch.compressed() ? 1 : 0)};
    reflection::serialize(out, hdr);
    if (batch.compressed()) {
        // This path isn't really async: we don't have a list to
        // iterate over, just a single buffer to read.
        reflection::serialize(out, std::move(batch).release_data());
        return ss::now();
    } else {
        return ss::do_with(std::move(batch), [&out](model::record_batch& b) {
            return b.for_each_record_async(
              [&out](model::record r) -> ss::future<> {
                  return reflection::async_adl<model::record>{}.to(
                    out, std::move(r));
              });
        });
    }
}

ss::future<model::record_batch>
async_adl<model::record_batch>::from(iobuf_parser& in) {
    auto hdr = reflection::adl<batch_header>{}.from(in);
    if (hdr.is_compressed == 1) {
        // This path isn't really async: we don't have a list to
        // iterate over, just a single buffer to read.
        auto io = reflection::adl<iobuf>{}.from(in);
        co_return model::record_batch(
          hdr.bhdr, std::move(io), model::record_batch::tag_ctor_ng());
    }
    auto recs = std::vector<model::record>{};
    recs.reserve(hdr.bhdr.record_count);
    for (int i = 0; i < hdr.bhdr.record_count; ++i) {
        auto rec = adl<model::record>{}.from(in);
        recs.push_back(std::move(rec));
        co_await ss::coroutine::maybe_yield();
    }
    co_return model::record_batch(hdr.bhdr, std::move(recs));
}

} // namespace reflection
