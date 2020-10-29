#include "coproc/tests/supervisor.h"

#include "coproc/logger.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "storage/record_batch_builder.h"

#include <type_traits>

namespace coproc {

std::vector<model::record_batch>
batch_to_vector(model::record_batch_reader::data_t&& data) {
    std::vector<model::record_batch> batches;
    batches.reserve(data.size());
    std::transform(
      std::make_move_iterator(data.begin()),
      std::make_move_iterator(data.end()),
      std::back_inserter(batches),
      [](model::record_batch&& rb) { return std::move(rb); });
    return batches;
}

/// In this case we must use the record_batch_builder as to ensure that the
/// record-batch_header header_crc and crc data fields are properly recomputed
model::record_batch_reader::data_t
vector_to_batch(std::vector<model::record_batch>&& data) {
    model::record_batch_reader::data_t new_batch;
    for (auto& batch : data) {
        storage::record_batch_builder rbb(
          batch.header().type, batch.header().base_offset);
        batch.for_each_record([&rbb](model::record r) {
            rbb.add_raw_kv(r.release_key(), r.release_value());
        });
        new_batch.push_back(std::move(rbb).build());
    }
    return new_batch;
}

void supervisor::invoke_coprocessor(
  const model::ntp& ntp,
  const script_id sid,
  const std::vector<model::record_batch>& batches,
  std::vector<process_batch_reply::data>& rs) {
    auto found = _coprocessors.local().find(sid);
    if (found == _coprocessors.local().end()) {
        vlog(coproclog.warn, "Script id: {} not found", sid);
        return;
    }
    auto& copro = found->second;
    auto rmap = copro->apply(ntp.tp.topic, batches);
    if (rmap.empty()) {
        // Must send at least one response so server can update the offset
        // This can be seen as a type of ack
        // TODO(rob) Maybe in this case we can adjust the type system, possibly
        // an optional response
        rs.emplace_back(process_batch_reply::data{
          .id = sid,
          .ntp = ntp,
          .reader = model::make_memory_record_batch_reader(
            model::record_batch_reader::data_t())});
        return;
    }
    for (auto& p : rmap) {
        auto data_batch = vector_to_batch(std::move(p.second));
        rs.emplace_back(process_batch_reply::data{
          .id = sid,
          .ntp = model::ntp(
            ntp.ns,
            to_materialized_topic(ntp.tp.topic, p.first),
            ntp.tp.partition),
          .reader = model::make_memory_record_batch_reader(
            std::move(data_batch))});
    }
}

ss::future<std::vector<process_batch_reply::data>>
supervisor::invoke_coprocessors(process_batch_request::data d) {
    return model::consume_reader_to_memory(
             std::move(d.reader), model::no_timeout)
      .then([this, script_ids = std::move(d.ids), ntp = std::move(d.ntp)](
              auto rbr) {
          std::vector<process_batch_reply::data> results;
          auto vdata = batch_to_vector(std::move(rbr));
          return ss::do_with(
            std::move(results),
            std::move(script_ids),
            std::move(vdata),
            std::move(ntp),
            [this](
              auto& results,
              const auto& sids,
              const auto& vdata,
              const auto& ntp) {
                return ss::do_for_each(
                         sids,
                         [this, &ntp, &vdata, &results](const auto& sid) {
                             invoke_coprocessor(ntp, sid, vdata, results);
                         })
                  .then([&results]() { return std::move(results); });
            });
      });
}

ss::future<process_batch_reply>
supervisor::process_batch(process_batch_request&& r, rpc::streaming_context&) {
    return ss::with_gate(_gate, [this, r = std::move(r)]() mutable {
        if (r.reqs.empty()) {
            vlog(coproclog.error, "Error with redpanda, request is of 0 size");
        }
        std::vector<process_batch_reply::data> ds;
        ds.reserve(r.reqs.size()); // At-least as big as input arr
        return ss::do_with(
          std::move(ds), std::move(r), [this](auto& ds, auto& r) {
              return ss::map_reduce(
                       r.reqs,
                       [this](auto&& d) {
                           return invoke_coprocessors(std::move(d));
                       },
                       std::move(ds),
                       [](auto acc, auto x) {
                           for (auto&& e : x) {
                               acc.emplace_back(std::move(e));
                           }
                           return acc;
                       })
                .then([](auto replies) {
                    return process_batch_reply{.resps = std::move(replies)};
                });
          });
    });
}

} // namespace coproc
