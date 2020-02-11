#pragma once

#include "bytes/iobuf.h"
#include "model/record_batch_reader.h"
#include "raft/types.h"

namespace raft::tron {
struct stats_request {};
struct stats_reply {};
struct put_reply {
    bool success;
    ss::sstring failure_reason;
};
} // namespace raft::tron
namespace reflection {

template<>
struct async_adl<model::record_batch_reader> {
    ss::future<> to(iobuf& out, model::record_batch_reader&& request) {
        return model::consume_reader_to_memory(
                 std::move(request), model::no_timeout)
          .then([&out](ss::circular_buffer<model::record_batch> batches) {
              reflection::adl<uint32_t>{}.to(out, batches.size());
              for (auto& batch : batches) {
                  reflection::serialize(out, std::move(batch));
              }
          });
    }

    ss::future<model::record_batch_reader> from(iobuf_parser& in) {
        auto batchCount = reflection::adl<uint32_t>{}.from(in);
        auto batches = ss::circular_buffer<model::record_batch>{};
        batches.reserve(batchCount);
        for (int i = 0; i < batchCount; ++i) {
            batches.push_back(adl<model::record_batch>{}.from(in));
        }
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(reader));
    }
};

} // namespace reflection
