/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/utils/supervisor.h"

#include "coproc/logger.h"
#include "coproc/tests/utils/helpers.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"

#include <type_traits>

namespace coproc {

ss::future<model::record_batch_reader::data_t>
copy_batch(const model::record_batch_reader::data_t& data) {
    return ss::map_reduce(
      data.cbegin(),
      data.cend(),
      [](const model::record_batch& rb) {
          return ss::make_ready_future<model::record_batch>(rb.copy());
      },
      model::record_batch_reader::data_t(),
      reduce::push_back());
}

ss::future<std::vector<process_batch_reply::data>> resultmap_to_vector(
  script_id id, const model::ntp& ntp, coprocessor::result rmap) {
    return ss::do_with(std::move(rmap), [id, ntp](coprocessor::result& rmap) {
        return ssx::async_transform(
          rmap, [id, ntp](coprocessor::result::value_type& vt) {
              return process_batch_reply::data{
                .id = id,
                .ntp = model::ntp(
                  ntp.ns,
                  to_materialized_topic(ntp.tp.topic, vt.first),
                  ntp.tp.partition),
                .reader = model::make_memory_record_batch_reader(
                  std::move(vt.second))};
          });
    });
}

ss::future<std::vector<process_batch_reply::data>>
empty_response(script_id id, const model::ntp& ntp) {
    /// Redpanda will special case respones with empty readers as an ack.
    /// This has the affect of an implied 'filter' transformation. The
    /// supervisor acks a request with an empty response, so redpanda just moves
    /// the input topic read head offset forward without a corresponding
    /// materialzied_topic write
    std::vector<process_batch_reply::data> eresp;
    eresp.emplace_back(process_batch_reply::data{
      .id = id,
      .ntp = ntp,
      .reader = model::make_memory_record_batch_reader(
        model::record_batch_reader::data_t())});
    return ss::make_ready_future<std::vector<process_batch_reply::data>>(
      std::move(eresp));
}

ss::future<std::vector<process_batch_reply::data>>
supervisor::invoke_coprocessor(
  const model::ntp& ntp,
  const script_id id,
  ss::circular_buffer<model::record_batch>&& batches) {
    auto found = _coprocessors.local().find(id);
    if (found == _coprocessors.local().end()) {
        vlog(coproclog.warn, "Script id: {} not found", id);
        return ss::make_ready_future<std::vector<process_batch_reply::data>>();
    }
    vassert(!batches.empty(), "Shouldn't expect empty batches from redpanda");
    auto& copro = found->second;
    return copro->apply(ntp.tp.topic, std::move(batches))
      .then([id, ntp](coprocessor::result rmap) {
          if (rmap.empty()) {
              return empty_response(id, ntp);
          }
          return resultmap_to_vector(id, ntp, std::move(rmap));
      });
}

ss::future<std::vector<process_batch_reply::data>>
supervisor::invoke_coprocessors(process_batch_request::data d) {
    return model::consume_reader_to_memory(
             std::move(d.reader), model::no_timeout)
      .then([this, ids = std::move(d.ids), ntp = std::move(d.ntp)](
              model::record_batch_reader::data_t rbr) mutable {
          return ss::do_with(
            std::move(rbr),
            std::move(ids),
            [this, ntp = std::move(ntp)](
              const model::record_batch_reader::data_t& rbr,
              const std::vector<script_id>& ids) {
                return ssx::async_flat_transform(
                  ids, [this, ntp, &rbr](script_id id) {
                      return copy_batch(rbr).then(
                        [this, id, ntp](
                          model::record_batch_reader::data_t batch) {
                            return invoke_coprocessor(
                              ntp, id, std::move(batch));
                        });
                  });
            });
      });
}

ss::future<process_batch_reply>
supervisor::process_batch(process_batch_request&& r, rpc::streaming_context&) {
    return ss::with_gate(_gate, [this, r = std::move(r)]() mutable {
        vassert(!r.reqs.empty(), "Cannot expect empty request from redpanda");
        return ss::do_with(std::move(r), [this](process_batch_request& r) {
            return ssx::async_flat_transform(
                     r.reqs,
                     [this](process_batch_request::data& d) {
                         return invoke_coprocessors(std::move(d));
                     })
              .then([](std::vector<process_batch_reply::data> replies) {
                  return process_batch_reply{.resps = std::move(replies)};
              });
        });
    });
}

} // namespace coproc
