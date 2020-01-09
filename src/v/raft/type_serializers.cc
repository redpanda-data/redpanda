#include "model/timeout_clock.h"
#include "raft/types.h"
#include "rpc/models.h"

#include <boost/range/irange.hpp>

namespace rpc {

struct rpc_model_reader_consumer {
    explicit rpc_model_reader_consumer(iobuf& oref)
      : ref(oref) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        rpc::serialize(ref, batch.release_header(), batch.size());
        if (!batch.compressed()) {
            rpc::serialize<int8_t>(ref, 0);
            for (model::record& r : batch) {
                rpc::serialize(ref, std::move(r));
            }
        } else {
            rpc::serialize<int8_t>(ref, 1);
            rpc::serialize(ref, std::move(batch).release().release());
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    void end_of_stream(){};
    iobuf& ref;
};

struct entry_header {
    model::record_batch_type etype;
    uint32_t batch_count;
};

template<>
void serialize(iobuf& out, raft::entry&& r) {
    auto batches = r.reader().release_buffered_batches();
    rpc::serialize(
      out,
      entry_header{.etype = r.entry_type(),
                   .batch_count = static_cast<uint32_t>(batches.size())});
    for (auto& batch : batches) {
        serialize(out, std::move(batch));
    }
}

template<>
ss::future<raft::entry> deserialize(source& in) {
    return rpc::deserialize<entry_header>(in).then(
      [&in](entry_header e_hdr) mutable {
          return ss::do_with(
                   boost::irange<uint32_t>(0, e_hdr.batch_count),
                   [&in](boost::integer_range<uint32_t>& r) mutable {
                       return copy_range<std::vector<model::record_batch>>(
                         r, [&in](int) {
                             return deserialize<model::record_batch>(in);
                         });
                   })
            .then([batch_type = e_hdr.etype](
                    std::vector<model::record_batch> batches) mutable {
                auto rdr = model::make_memory_record_batch_reader(
                  std::move(batches));
                return raft::entry(batch_type, std::move(rdr));
            });
      });
}

} // namespace rpc
