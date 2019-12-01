#include "model/timeout_clock.h"
#include "raft/types.h"
#include "rpc/models.h"

#include <boost/range/irange.hpp>

namespace rpc {

struct rpc_model_reader_consumer {
    explicit rpc_model_reader_consumer(iobuf& oref)
      : ref(oref) {}
    future<stop_iteration> operator()(model::record_batch batch) {
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
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    void end_of_stream(){};
    iobuf& ref;
};
template<>
void serialize(iobuf& out, raft::entry&& r) {
    rpc::serialize(out, r.entry_type());
    auto batches = r.reader().release_buffered_batches();
    for (auto& batch : batches) {
        rpc::serialize(out, batch.release_header(), batch.size());
        if (!batch.compressed()) {
            rpc::serialize<int8_t>(out, 0);
            for (model::record& r : batch) {
                rpc::serialize(out, std::move(r));
            }
        } else {
            rpc::serialize<int8_t>(out, 1);
            rpc::serialize(out, std::move(batch).release().release());
        }
    }
}

template<>
future<raft::entry> deserialize(source& in) {
    struct e_header {
        model::record_batch_type etype;
        model::record_batch_header bhdr;
        uint32_t batch_size;
        int8_t is_compressed;
    };
    return rpc::deserialize<e_header>(in).then([&in](e_header hdr) {
        if (hdr.is_compressed == 1) {
            return rpc::deserialize<iobuf>(in).then(
              [hdr = std::move(hdr)](iobuf f) {
                  auto batch = model::record_batch(
                    std::move(hdr.bhdr),
                    model::record_batch::compressed_records(
                      hdr.batch_size, std::move(f)));
                  std::vector<model::record_batch> batches;
                  batches.reserve(1);
                  batches.push_back(std::move(batch));
                  auto rdr = model::make_memory_record_batch_reader(
                    std::move(batches));

                  return raft::entry(hdr.etype, std::move(rdr));
              });
        }
        // not compressed
        return do_with(
                 boost::irange(0, static_cast<int>(hdr.batch_size)),
                 [&in](auto& r) {
                     return copy_range<std::vector<model::record>>(
                       r,
                       [&in](int) { return deserialize<model::record>(in); });
                 })
          .then([hdr = std::move(hdr)](std::vector<model::record> recs) {
              auto batch = model::record_batch(
                std::move(hdr.bhdr), std::move(recs));
              std::vector<model::record_batch> batches;
              batches.reserve(1);
              batches.push_back(std::move(batch));
              auto rdr = model::make_memory_record_batch_reader(
                std::move(batches));
              return raft::entry(hdr.etype, std::move(rdr));
          });
    });
}

} // namespace rpc
