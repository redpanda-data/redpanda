// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/produce.h"

#include "bytes/iobuf.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/server/replicated_partition.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"
#include "raft/types.h"
#include "storage/shard_assignment.h"
#include "utils/remote.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <boost/container_hash/extensions.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

produce_response produce_request::make_error_response(error_code error) const {
    produce_response response;

    response.data.responses.reserve(data.topics.size());
    for (const auto& topic : data.topics) {
        produce_response::topic t;
        t.name = topic.name;

        t.partitions.reserve(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            t.partitions.emplace_back(produce_response::partition{
              .partition_index = partition.partition_index,
              .error_code = error});
        }

        response.data.responses.push_back(std::move(t));
    }

    return response;
}

struct produce_ctx {
    request_context rctx;
    produce_request request;
    produce_response response;
    ss::smp_service_group ssg;

    produce_ctx(
      request_context&& rctx,
      produce_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

static raft::replicate_options acks_to_replicate_options(int16_t acks) {
    switch (acks) {
    case -1:
        return raft::replicate_options(raft::consistency_level::quorum_ack);
    case 0:
        return raft::replicate_options(raft::consistency_level::no_ack);
    case 1:
        return raft::replicate_options(raft::consistency_level::leader_ack);
    default:
        throw std::invalid_argument("Not supported ack level");
    };
}

static inline model::record_batch_reader
reader_from_lcore_batch(model::record_batch&& batch) {
    /*
     * The remainder of work for this partition is handled on its home
     * core. The foreign memory record batch reader requires that once the
     * reader is sent to the foreign core that it has exclusive access to the
     * data in reader. That is true here and is generally trivial with readers
     * that hold a copy of their data in memory.
     */
    return model::make_foreign_memory_record_batch_reader(std::move(batch));
}

static const failure_type<error_code>
  not_leader_for_partition(error_code::not_leader_for_partition);
static const failure_type<error_code>
  out_of_order_sequence_number(error_code::out_of_order_sequence_number);

/*
 * Caller is expected to catch errors that may be thrown while the kafka
 * batch is being deserialized (see reader_from_kafka_batch).
 */
static ss::future<produce_response::partition> partition_append(
  model::partition_id id,
  ss::lw_shared_ptr<replicated_partition> partition,
  model::batch_identity bid,
  model::record_batch_reader reader,
  int16_t acks,
  int32_t num_records) {
    return partition
      ->replicate(bid, std::move(reader), acks_to_replicate_options(acks))
      .then_wrapped([partition, id, num_records = num_records](
                      ss::future<checked<model::offset, kafka::error_code>> f) {
          produce_response::partition p{.partition_index = id};
          try {
              auto r = f.get0();
              if (r.has_value()) {
                  // have to subtract num_of_records - 1 as base_offset
                  // is inclusive
                  p.base_offset = model::offset(r.value() - (num_records - 1));
                  p.error_code = error_code::none;
                  partition->probe().add_records_produced(num_records);
              } else if (r == not_leader_for_partition) {
                  p.error_code = error_code::not_leader_for_partition;
              } else if (r == out_of_order_sequence_number) {
                  p.error_code = error_code::out_of_order_sequence_number;
              } else {
                  p.error_code = error_code::unknown_server_error;
              }
          } catch (...) {
              p.error_code = error_code::unknown_server_error;
          }
          return p;
      });
}

/**
 * \brief handle writing to a single topic partition.
 */
static ss::future<produce_response::partition> produce_topic_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part) {
    auto ntp = model::ntp(
      model::kafka_namespace, topic.name, part.partition_index);

    /*
     * A single produce request may contain record batches for many
     * different partitions that are managed different cores.
     */
    auto shard = octx.rctx.shards().shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<produce_response::partition>(
          produce_response::partition{
            .partition_index = ntp.tp.partition,
            .error_code = error_code::unknown_topic_or_partition});
    }

    // steal the batch from the adapter
    auto batch = std::move(part.records->adapter.batch.value());

    /*
     * grab timestamp type topic configuration option out of the
     * metadata cache. For append time setting we have to recalculate
     * the CRC.
     */
    auto timestamp_type
      = octx.rctx.metadata_cache()
          .get_topic_timestamp_type(
            model::topic_namespace_view(model::kafka_namespace, topic.name))
          .value_or(octx.rctx.metadata_cache().get_default_timestamp_type());

    if (timestamp_type == model::timestamp_type::append_time) {
        batch.set_max_timestamp(
          model::timestamp_type::append_time, model::timestamp::now());
    }

    const auto& hdr = batch.header();
    auto bid = model::batch_identity::from(hdr);

    auto num_records = batch.record_count();
    auto reader = reader_from_lcore_batch(std::move(batch));
    auto start = std::chrono::steady_clock::now();
    auto f = octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [reader = std::move(reader),
       ntp = std::move(ntp),
       num_records,
       bid,
       acks = octx.request.data.acks](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = ntp.tp.partition,
                  .error_code = error_code::unknown_topic_or_partition});
          }
          if (unlikely(!partition->is_leader())) {
              return ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = ntp.tp.partition,
                  .error_code = error_code::not_leader_for_partition});
          }
          return partition_append(
            ntp.tp.partition,
            ss::make_lw_shared<replicated_partition>(std::move(partition)),
            bid,
            std::move(reader),
            acks,
            num_records);
      });
    return f.then([&octx, start](produce_response::partition p) {
        if (p.error_code == error_code::none) {
            auto dur = std::chrono::steady_clock::now() - start;
            octx.rctx.connection()->server().update_produce_latency(dur);
        }
        return p;
    });
}

/**
 * \brief Dispatch and collect topic partition produce responses
 */
static ss::future<produce_response::topic>
produce_topic(produce_ctx& octx, produce_request::topic& topic) {
    std::vector<ss::future<produce_response::partition>> partitions;
    partitions.reserve(topic.partitions.size());

    for (auto& part : topic.partitions) {
        if (!octx.rctx.authorized(security::acl_operation::write, topic.name)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::topic_authorization_failed}));
            continue;
        }

        if (!octx.rctx.metadata_cache().contains(
              model::topic_namespace_view(model::kafka_namespace, topic.name),
              part.partition_index)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::unknown_topic_or_partition}));
            continue;
        }

        // the record data on the wire was null value
        if (unlikely(!part.records)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::invalid_record}));
            continue;
        }

        if (unlikely(!part.records->adapter.valid_crc)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::corrupt_message}));
            continue;
        }

        // produce version >= 3 (enforced for all produce requests)
        // requires exactly one record batch per request and it must use
        // the v2 format.
        if (unlikely(
              !part.records->adapter.v2_format
              || !part.records->adapter.batch)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition{
                  .partition_index = part.partition_index,
                  .error_code = error_code::invalid_record}));
            continue;
        }

        auto pr = produce_topic_partition(octx, topic, part);
        partitions.push_back(std::move(pr));
    }

    // collect partition responses and build the topic response
    return when_all_succeed(partitions.begin(), partitions.end())
      .then([name = std::move(topic.name)](
              std::vector<produce_response::partition> parts) mutable {
          return produce_response::topic{
            .name = std::move(name),
            .partitions = std::move(parts),
          };
      });
}

/**
 * \brief Dispatch and collect topic produce responses
 */
static std::vector<ss::future<produce_response::topic>>
produce_topics(produce_ctx& octx) {
    std::vector<ss::future<produce_response::topic>> topics;
    topics.reserve(octx.request.data.topics.size());

    for (auto& topic : octx.request.data.topics) {
        auto tr = produce_topic(octx, topic);
        topics.push_back(std::move(tr));
    }

    return topics;
}

template<>
ss::future<response_ptr>
produce_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    produce_request request;
    request.decode(ctx.reader(), ctx.header().version);

    // determine if the request has transactional / idemponent batches
    for (auto& topic : request.data.topics) {
        for (auto& part : topic.partitions) {
            if (part.records) {
                if (part.records->adapter.batch) {
                    const auto& hdr = part.records->adapter.batch->header();
                    request.has_transactional = request.has_transactional
                                                || hdr.attrs.is_transactional();
                    request.has_idempotent = request.has_idempotent
                                             || hdr.producer_id >= 0;
                }
            }
        }
    }

    /*
     * Authorization
     *
     * Note that in kafka authorization is performed based on
     * transactional id, producer id, and idempotency. Redpanda does not
     * yet support these features, so we reject all such requests as if
     * authorization failed.
     */
    if (request.has_transactional) {
        // will be removed when all of transactions are implemented
        return ctx.respond(request.make_error_response(
          error_code::transactional_id_authorization_failed));

        if (
          !request.data.transactional_id
          || !ctx.authorized(
            security::acl_operation::write,
            transactional_id(*request.data.transactional_id))) {
            return ctx.respond(request.make_error_response(
              error_code::transactional_id_authorization_failed));
        }
        // <kafka>Note that authorization to a transactionalId implies
        // ProducerId authorization</kafka>

    } else if (request.has_idempotent) {
        if (!ctx.authorized(
              security::acl_operation::idempotent_write,
              security::default_cluster_name)) {
            return ctx.respond(request.make_error_response(
              error_code::cluster_authorization_failed));
        }

        if (!ctx.is_idempotence_enabled()) {
            return ctx.respond(request.make_error_response(
              error_code::cluster_authorization_failed));
        }

    } else if (request.data.acks < -1 || request.data.acks > 1) {
        // from kafka source: "if required.acks is outside accepted
        // range, something is wrong with the client Just return an
        // error and don't handle the request at all"
        klog.error(
          "unsupported acks {} see "
          "https://docs.confluent.io/current/installation/"
          "configuration/"
          "producer-configs.html",
          request.data.acks);
        return ctx.respond(
          request.make_error_response(error_code::invalid_required_acks));
    }

    return ss::do_with(
      produce_ctx(std::move(ctx), std::move(request), ssg),
      [](produce_ctx& octx) {
          vlog(klog.trace, "handling produce request {}", octx.request);

          // dispatch produce requests for each topic
          auto topics = produce_topics(octx);

          // collect topic responses
          return when_all_succeed(topics.begin(), topics.end())
            .then([&octx](std::vector<produce_response::topic> topics) {
                octx.response.data.responses = std::move(topics);
            })
            .then([&octx] {
                // send response immediately
                if (octx.request.data.acks != 0) {
                    return octx.rctx.respond(std::move(octx.response));
                }

                // acks = 0 is handled separately. first, check for
                // errors
                bool has_error = false;
                for (const auto& topic : octx.response.data.responses) {
                    for (const auto& p : topic.partitions) {
                        if (p.error_code != error_code::none) {
                            has_error = true;
                            break;
                        }
                    }
                }

                // in the absense of errors, acks = 0 results in the
                // response being dropped, as the client does not expect
                // a response. here we mark the response as noop, but
                // let it flow back so that it can be accounted for in
                // quota and stats tracking. it is dropped later during
                // processing.
                if (!has_error) {
                    return octx.rctx.respond(std::move(octx.response))
                      .then([](response_ptr resp) {
                          resp->mark_noop();
                          return resp;
                      });
                }

                // errors in a response from an acks=0 produce request
                // result in the connection being dropped to signal an
                // issue to the client
                return ss::make_exception_future<response_ptr>(
                  std::runtime_error(fmt::format(
                    "Closing connection due to error in produce "
                    "response: {}",
                    octx.response)));
            });
      });
}

} // namespace kafka
