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
struct adl<model::record_batch_reader> {
    void to(iobuf& out, model::record_batch_reader&& rdr) {
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get0();
        reflection::adl<uint32_t>{}.to(out, batches.size());
        for (auto& batch : batches) {
            reflection::serialize(out, std::move(batch));
        }
    }

    model::record_batch_reader from(iobuf io) {
        return reflection::from_iobuf<model::record_batch_reader>(
          std::move(io));
    }

    model::record_batch_reader from(iobuf_parser& in) {
        auto batchCount = reflection::adl<uint32_t>{}.from(in);
        auto batches = ss::circular_buffer<model::record_batch>{};
        batches.reserve(batchCount);
        for (int i = 0; i < batchCount; ++i) {
            batches.push_back(adl<model::record_batch>{}.from(in));
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }
};
} // namespace reflection
