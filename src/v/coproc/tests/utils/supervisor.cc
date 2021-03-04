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

#include "bytes/iobuf_parser.h"
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "model/errc.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/validation.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>

#include <type_traits>

namespace coproc {

supervisor::supervisor(
  ss::scheduling_group sc,
  ss::smp_service_group ssg,
  ss::sharded<script_map_t>& coprocessors)
  : supervisor_service(sc, ssg)
  , _coprocessors(coprocessors) {}

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

static enable_response_code
parse_and_validate_payload(iobuf_parser& p, wasm::cpp_enable_payload& payload) {
    try {
        payload = reflection::adl<wasm::cpp_enable_payload>{}.from(p);
    } catch (const std::exception& e) {
        vlog(
          coproclog.error,
          "Error deserializing wasm::cpp_enable_payload: {}",
          e.what());
        return enable_response_code::internal_error;
    }
    if (payload.tid == registry::type_identifier::none) {
        vlog(coproclog.error, "none value for registry::type_id detected");
        return enable_response_code::internal_error;
    }
    if (payload.topics.empty()) {
        return enable_response_code::script_contains_no_topics;
    }
    bool all_topics_valid = std::all_of(
      payload.topics.begin(), payload.topics.end(), [](const model::topic t) {
          return model::validate_kafka_topic_name(t)
                 == make_error_code(model::errc::success);
      });
    return all_topics_valid
             ? enable_response_code::success
             : enable_response_code::script_contains_invalid_topic;
}

ss::future<enable_copros_reply::data>
supervisor::enable_coprocessor(script_id id, iobuf src) {
    bool id_exists = co_await _coprocessors.map_reduce0(
      [id](const script_map_t& copros) {
          return copros.find(id) != copros.end();
      },
      false,
      std::logical_or<>());
    if (id_exists) {
        co_return enable_copros_reply::data{
          .ack = enable_response_code::script_id_already_exists,
          .script_meta{.id = id}};
    }
    iobuf_parser p(std::move(src));
    wasm::cpp_enable_payload payload;
    enable_response_code erc = parse_and_validate_payload(p, payload);
    if (erc != enable_response_code::success) {
        co_return enable_copros_reply::data{.ack = erc, .script_meta{.id = id}};
    }
    std::vector<enable_copros_reply::topic_policy> enriched_topics;
    enriched_topics.reserve(payload.topics.size());
    std::transform(
      std::make_move_iterator(payload.topics.begin()),
      std::make_move_iterator(payload.topics.end()),
      std::back_inserter(enriched_topics),
      [](model::topic&& t) {
          return std::make_pair(std::move(t), topic_ingestion_policy::latest);
      });
    vlog(
      coproclog.info,
      "Enabling coprocessor {} with topics: {}",
      id,
      enriched_topics.size());
    co_return co_await launch(id, std::move(enriched_topics), payload.tid);
}

ss::future<enable_copros_reply::data> supervisor::launch(
  script_id id,
  std::vector<enable_copros_reply::topic_policy> enriched_topics,
  registry::type_identifier tid) {
    return _coprocessors
      .invoke_on_all([id, enriched_topics, tid](script_map_t& coprocs) {
          auto copro = registry::make_coprocessor(tid, id, enriched_topics);
          vassert(copro != nullptr, "make_coprocessor returned nullptr");
          coprocs.emplace(id, std::move(copro));
      })
      .then([id, enriched_topics] {
          return enable_copros_reply::data{
            .ack = enable_response_code::success,
            .script_meta{.id = id, .input_topics = enriched_topics}};
      });
}

ss::future<enable_copros_reply> supervisor::enable_coprocessors(
  enable_copros_request&& r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, r = std::move(r)]() mutable {
          return ss::do_with(
            std::move(r.inputs),
            [this](std::vector<enable_copros_request::data>& inputs) {
                return ssx::async_transform(
                         inputs,
                         [this](auto& item) {
                             return enable_coprocessor(
                               item.id, std::move(item.source_code));
                         })
                  .then([](std::vector<enable_copros_reply::data> acks) {
                      return enable_copros_reply{.acks = std::move(acks)};
                  });
            });
      });
}

ss::future<disable_copros_reply::ack>
supervisor::disable_coprocessor(script_id id) {
    vlog(coproclog.info, "Disabling coprocessor with id: {}", id);
    return _coprocessors
      .map_reduce0(
        [id](script_map_t& copros) { return copros.erase(id); },
        script_map_t::size_type(0),
        std::plus<>())
      .then([id](script_map_t::size_type total) {
          disable_response_code drc
            = total > 0 ? disable_response_code::success
                        : disable_response_code::script_id_does_not_exist;
          return std::make_pair(id, drc);
      });
}

ss::future<disable_copros_reply> supervisor::disable_coprocessors(
  disable_copros_request&& r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, r = std::move(r)] {
          return ssx::async_transform(
                   r.ids,
                   [this](script_id id) { return disable_coprocessor(id); })
            .then([](std::vector<disable_copros_reply::ack> acks) {
                return disable_copros_reply{.acks = std::move(acks)};
            });
      });
}

ss::future<disable_copros_reply>
supervisor::disable_all_coprocessors(empty_request&&, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this] {
        return _coprocessors
          .map_reduce0(
            [](const script_map_t& copros) {
                std::set<script_id> all_keys;
                for (const auto& p : copros) {
                    all_keys.insert(p.first);
                }
                return all_keys;
            },
            std::set<script_id>(),
            [](std::set<script_id> set, std::set<script_id> x) {
                set.merge(std::move(x));
                return std::move(set);
            })
          .then([this](std::set<script_id> ids) {
              return ssx::async_transform(
                       ids,
                       [this](script_id id) { return disable_coprocessor(id); })
                .then([](std::vector<disable_copros_reply::ack> acks) {
                    return disable_copros_reply{.acks = std::move(acks)};
                });
          });
    });
}

ss::future<process_batch_reply>
supervisor::process_batch(process_batch_request&& r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, r = std::move(r)]() mutable {
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
