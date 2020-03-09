#include "kafka/requests/produce_request.h"

#include "bytes/iobuf.h"
#include "cluster/namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/kafka_batch_adapter.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"
#include "storage/shard_assignment.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/execution_stage.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

void produce_request::encode(
  const request_context& ctx, response_writer& writer) {
    auto version = ctx.header().version;

    writer.write(transactional_id);
    writer.write(int16_t(acks));
    writer.write(int32_t(timeout.count()));
    writer.write_array(topics, [](topic& t, response_writer& writer) {
        writer.write(t.name);
        writer.write_array(
          t.partitions, [](partition& part, response_writer& writer) {
              writer.write(part.id());
              writer.write(std::move(part.data));
          });
    });
}

void produce_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    transactional_id = reader.read_nullable_string();
    acks = reader.read_int16();
    timeout = std::chrono::milliseconds(reader.read_int32());
    topics = reader.read_array([](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([](request_reader& reader) {
              return partition{
                .id = model::partition_id(reader.read_int32()),
                .data = reader.read_fragmented_nullable_bytes(),
              };
          }),
        };
    });

    for (auto& topic : topics) {
        for (auto& part : topic.partitions) {
            if (part.data) {
                part.adapter.adapt(std::move(part.data.value()));
                if (part.adapter.batch) {
                    const auto& hdr = part.adapter.batch->header();
                    has_transactional = has_transactional
                                        || hdr.attrs.is_transactional();
                    has_idempotent = has_idempotent || hdr.producer_id >= 0;
                }
            }
        }
    }
}

produce_response produce_request::make_error_response(error_code error) const {
    produce_response response;

    response.topics.reserve(topics.size());
    for (const auto& topic : topics) {
        produce_response::topic t;
        t.name = topic.name;

        t.partitions.reserve(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            t.partitions.emplace_back(partition.id, error);
        }

        response.topics.push_back(std::move(t));
    }

    return response;
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::partition& p) {
    return ss::fmt_print(o, "id {} payload {}", p.id, p.data);
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::topic& t) {
    return ss::fmt_print(o, "name {} data {}", t.name, t.partitions);
}

std::ostream& operator<<(std::ostream& o, const produce_request& r) {
    return ss::fmt_print(
      o,
      "txn_id {} acks {} timeout {} topics {}",
      r.transactional_id,
      r.acks,
      r.timeout,
      r.topics);
}

void produce_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    writer.write_array(topics, [version](topic& t, response_writer& writer) {
        writer.write(t.name);
        writer.write_array(
          t.partitions, [version](partition& p, response_writer& writer) {
              writer.write(p.id);
              writer.write(p.error);

              int64_t base_offset = p.base_offset();
              int64_t log_append_time = p.log_append_time.value();
              int64_t log_start_offset = p.log_start_offset();

              // TODO: we can unify this into the error response encoding when
              // we've fully switched over to signed model offsets.
              if (p.error != error_code::none) {
                  base_offset = -1;
                  log_append_time = -1;
                  log_start_offset = -1;
              }

              writer.write(base_offset);
              writer.write(log_append_time);
              if (version >= api_version(5)) {
                  writer.write(log_start_offset);
              }
          });
    });
    writer.write(int32_t(throttle.count()));
}

struct produce_ctx {
    request_context rctx;
    ss::smp_service_group ssg;
    produce_request request;
    produce_response response;

    produce_ctx(
      request_context&& rctx,
      produce_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

/*
 * Caller is expected to catch errors that may be thrown while the kafka batch
 * is being deserialized (see reader_from_kafka_batch).
 */
static ss::future<produce_response::partition> partition_append(
  model::partition_id id,
  ss::lw_shared_ptr<cluster::partition> partition,
  model::record_batch batch) {
    auto num_records = batch.record_count();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    return partition->replicate(std::move(reader))
      .then_wrapped([id, num_records = num_records](
                      ss::future<result<raft::replicate_result>> f) {
          produce_response::partition p(id);
          try {
              auto r = f.get0();
              if (r) {
                  p.base_offset = model::offset(
                    r.value().last_offset() - num_records);
                  p.error = error_code::none;
              } else {
                  p.error = error_code::unknown_server_error;
              }
          } catch (...) {
              p.error = error_code::unknown_server_error;
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
    auto ntp = model::ntp{
      .ns = cluster::kafka_namespace,
      .tp = model::topic_partition{
        .topic = topic.name,
        .partition = part.id,
      },
    };

    /*
     * A single produce request may contain record batches for many different
     * partitions that are managed different cores.
     */
    auto shard = octx.rctx.shards().shard_for(ntp);
    if (!shard) {
        return ss::make_ready_future<produce_response::partition>(
          produce_response::partition(
            ntp.tp.partition, error_code::unknown_topic_or_partition));
    }

    /*
     * The remainder of work for this partition is handled on its home core.
     */
    auto batch = ss::engine().cpu_id() != *shard
                   ? part.adapter.batch->foreign_share()
                   : part.adapter.batch->share();
    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [batch = std::move(batch),
       ntp = std::move(ntp)](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<produce_response::partition>(
                produce_response::partition(
                  ntp.tp.partition, error_code::unknown_topic_or_partition));
          }
          if (unlikely(!partition->is_leader())) {
              return ss::make_ready_future<produce_response::partition>(
                produce_response::partition(
                  ntp.tp.partition, error_code::not_leader_for_partition));
          }
          return partition_append(
            ntp.tp.partition, partition, std::move(batch));
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
        if (!octx.rctx.metadata_cache().contains(topic.name, part.id)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition(
                  part.id, error_code::unknown_topic_or_partition)));
            continue;
        }

        if (unlikely(!part.adapter.valid_crc)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition(
                  part.id, error_code::corrupt_message)));
            continue;
        }

        // produce version >= 3 (enforced for all produce requests) requires
        // exactly one record batch per request and it must use the v2 format.
        if (unlikely(!part.adapter.v2_format || !part.adapter.batch)) {
            partitions.push_back(
              ss::make_ready_future<produce_response::partition>(
                produce_response::partition(
                  part.id, error_code::invalid_record)));
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
    topics.reserve(octx.request.topics.size());

    for (auto& topic : octx.request.topics) {
        auto tr = produce_topic(octx, topic);
        topics.push_back(std::move(tr));
    }

    return topics;
}

ss::future<response_ptr>
produce_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    produce_request request(ctx);

    /*
     * Authorization
     *
     * Note that in kafka authorization is performed based on transactional id,
     * producer id, and idempotency. Redpanda does not yet support these
     * features, so we reject all such requests as if authorization failed.
     */
    if (request.has_transactional) {
        return ctx.respond(request.make_error_response(
          error_code::transactional_id_authorization_failed));

    } else if (request.has_idempotent) {
        return ctx.respond(request.make_error_response(
          error_code::cluster_authorization_failed));

    } else if (request.acks < -1 || request.acks > 1) {
        // from kafka source: "if required.acks is outside accepted range,
        // something is wrong with the client Just return an error and don't
        // handle the request at all"
        kreq_log.error(
          "unsupported acks {} see "
          "https://docs.confluent.io/current/installation/configuration/"
          "producer-configs.html",
          request.acks);
        return ctx.respond(
          request.make_error_response(error_code::invalid_required_acks));
    }

    return ss::do_with(
      produce_ctx(std::move(ctx), std::move(request), ssg),
      [](produce_ctx& octx) {
          kreq_log.debug("handling produce request {}", octx.request);

          // dispatch produce requests for each topic
          auto topics = produce_topics(octx);

          // collect topic responses and build the final response message
          return when_all_succeed(topics.begin(), topics.end())
            .then([&octx](std::vector<produce_response::topic> topics) {
                octx.response.topics = std::move(topics);
                return octx.rctx.respond(std::move(octx.response));
            });
      });
}

} // namespace kafka
