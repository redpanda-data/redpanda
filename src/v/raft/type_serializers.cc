#include "model/timeout_clock.h"
#include "raft/types.h"
#include "rpc/serialize.h"
#include "rpc/types.h"
#include "utils/vint.h"

#include <boost/range/irange.hpp>

namespace rpc {

template<>
void serialize(bytes_ostream& out, model::broker&& r) {
    rpc::serialize(out, r.id()(), sstring(r.host()), r.port());
}

// from wire
template<>
future<model::offset> deserialize(source& in) {
    using type = model::offset::type;
    return deserialize<type>(in).then(
      [](type v) { return model::offset{std::move(v)}; });
}
template<>
future<model::broker> deserialize(source& in) {
    struct broker_contents {
        model::node_id id;
        sstring host;
        int32_t port;
        std::optional<sstring> rack;
    };
    return deserialize<broker_contents>(in).then([](broker_contents res) {
        return model::broker(
          std::move(res.id),
          std::move(res.host),
          res.port,
          std::move(res.rack));
    });
}
template<>
void serialize(bytes_ostream& ref, model::record&& record) {
    rpc::serialize(
      ref,
      record.size_bytes(),
      record.timestamp_delta(),
      record.offset_delta(),
      record.release_key(),
      record.release_packed_value_and_headers());
}

template<>
future<model::record> deserialize(source& in) {
    struct simple_record {
        uint32_t size_bytes;
        int32_t timestamp_delta;
        int32_t offset_delta;
        fragbuf key;
        fragbuf value_and_headers;
    };
    return rpc::deserialize<simple_record>(in).then([](simple_record r) {
        return model::record(
          r.size_bytes,
          r.timestamp_delta,
          r.offset_delta,
          std::move(r.key),
          std::move(r.value_and_headers));
    });
}

struct rpc_model_reader_consumer {
    explicit rpc_model_reader_consumer(bytes_ostream& oref)
      : ref(oref) {
    }
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
    bytes_ostream& ref;
};
template<>
void serialize(bytes_ostream& out, std::unique_ptr<raft::entry>&& r) {
    rpc::serialize(out, r->entry_type());
    (void)r->reader()
      .consume(rpc_model_reader_consumer(out), model::no_timeout)
      .get();
}

template<>
future<std::unique_ptr<raft::entry>> deserialize(source& in) {
    struct e_header {
        raft::entry::type etype;
        model::record_batch_header bhdr;
        uint32_t batch_size;
        int8_t is_compressed;
    };
    return rpc::deserialize<e_header>(in).then([&in](e_header hdr) {
        if (hdr.is_compressed == 1) {
            return rpc::deserialize<fragbuf>(in).then([hdr = std::move(hdr)](
                                                        fragbuf f) {
                auto batch = model::record_batch(
                  std::move(hdr.bhdr),
                  model::record_batch::compressed_records(
                    hdr.batch_size, std::move(f)));
                std::vector<model::record_batch> batches;
                batches.reserve(1);
                batches.push_back(std::move(batch));
                auto rdr = model::make_memory_record_batch_reader(
                  std::move(batches));

                return std::make_unique<raft::entry>(hdr.etype, std::move(rdr));
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
              return std::make_unique<raft::entry>(hdr.etype, std::move(rdr));
          });
    });
}

} // namespace rpc
