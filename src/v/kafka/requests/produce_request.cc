#include "kafka/requests/produce_request.h"

#include "bytes/iobuf.h"
#include "kafka/default_namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "model/metadata.h"
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
        writer.write_array(t.data, [](topic_data& td, response_writer& writer) {
            writer.write(td.id());
            writer.write(std::move(td.data));
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
          .data = reader.read_array([](request_reader& reader) {
              return topic_data{
                .id = model::partition_id(reader.read_int32()),
                .data = reader.read_fragmented_nullable_bytes(),
              };
          }),
        };
    });
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::topic_data& d) {
    return fmt_print(o, "id {} payload {}", d.id, d.data);
}

static std::ostream&
operator<<(std::ostream& o, const produce_request::topic& t) {
    return fmt_print(o, "name {} data {}", t.name, t.data);
}

std::ostream& operator<<(std::ostream& o, const produce_request& r) {
    return fmt_print(
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
              writer.write(int64_t(p.base_offset()));
              writer.write(p.log_append_time.value());
              if (version >= api_version(5)) {
                  writer.write(int64_t(p.log_start_offset()));
              }
          });
    });
    writer.write(int32_t(throttle.count()));
}

struct op_context {
    request_context rctx;
    smp_service_group ssg;
    produce_request request;
    produce_response response;

    op_context(request_context&& rctx, smp_service_group ssg)
      : rctx(std::move(rctx))
      , ssg(ssg) {
        request.decode(rctx);
    }
};

static future<produce_response::partition>
make_partition_response_error(model::partition_id id, error_code error) {
    return make_ready_future<produce_response::partition>(
      produce_response::partition{
        .id = id,
        .error = error,
      });
}

/*
 * Caller is expected to catch errors that may be thrown while the kafka batch
 * is being deserialized (see reader_from_kafka_batch).
 */
static future<produce_response::partition> partition_append(
  model::partition_id id,
  lw_shared_ptr<cluster::partition> partition,
  std::optional<iobuf> data) {
    // parses and validates the record batch and prepares it for being
    // replicated by raft.
    auto reader = reader_from_kafka_batch(std::move(data.value()));
    raft::entry e(raft::data_batch_type, std::move(reader));

#if 0
    partition->replicate(std::move(e));
#else
#warning "missing raft::replicate()"
    return make_ready_future<>()
#endif
    .then_wrapped([id](future<> f) {
        try {
            f.get();
            /*
             * TODO: grab metadata from replication result like the offset at
             * which the data was written in the log.
             */
            return produce_response::partition{
              .id = id,
              .error = error_code::none,
            };
        } catch (...) {
            /*
             * TODO: convert raft errors into kafka errors
             */
            return produce_response::partition{
              .id = id,
              .error = error_code::unknown_server_error,
            };
        }
    });
}

/**
 * \brief handle writing to a single topic partition.
 */
static future<produce_response::partition> produce_topic_partition(
  op_context& octx,
  produce_request::topic& topic,
  produce_request::topic_data& data) {
    auto ntp = model::ntp{
      .ns = default_namespace(),
      .tp = model::topic_partition{
        .topic = topic.name,
        .partition = data.id,
      },
    };

    /*
     * lookup the home shard for this ntp. the caller should check for
     * the tp in the metadata cache so that this condition is unlikely
     * to pass.
     */
    if (__builtin_expect(!octx.rctx.shards().contains(ntp), false)) {
        return make_partition_response_error(
          ntp.tp.partition, error_code::unknown_topic_or_partition);
    }
    auto shard = octx.rctx.shards().shard_for(ntp);

    return octx.rctx.partition_manager().invoke_on(
      shard,
      octx.ssg,
      [&data, ntp = std::move(ntp)](cluster::partition_manager& mgr) {
          /*
           * look up partition on the remote shard
           */
          if (!mgr.contains(ntp)) {
              return make_partition_response_error(
                ntp.tp.partition, error_code::unknown_topic_or_partition);
          }
          auto partition = mgr.get(ntp);

          // produce version >= 3 requires a message batch be present
          if (!data.data || data.data->size_bytes() == 0) {
              return make_partition_response_error(
                ntp.tp.partition, error_code::corrupt_message);
          }

          try {
              return partition_append(
                ntp.tp.partition, partition, std::move(data.data));
          } catch (const invalid_record_exception& e) {
              kreq_log.error(e.what());
              return make_partition_response_error(
                ntp.tp.partition, error_code::corrupt_message);
          }
      });
}

/**
 * \brief Dispatch and collect topic partition produce responses
 */
static future<produce_response::topic>
produce_topic(op_context& octx, produce_request::topic& topic) {
    // partition response placeholders
    std::vector<future<produce_response::partition>> partitions;
    partitions.reserve(topic.data.size());

    for (auto& topic_data : topic.data) {
        auto pr = produce_topic_partition(octx, topic, topic_data);
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
static std::vector<future<produce_response::topic>>
produce_topics(op_context& octx) {
    // topic response placeholders
    std::vector<future<produce_response::topic>> topics;
    topics.reserve(octx.request.topics.size());

    for (auto& topic : octx.request.topics) {
        auto tr = produce_topic(octx, topic);
        topics.push_back(std::move(tr));
    }

    return std::move(topics);
}

future<response_ptr>
produce_api::process(request_context&& ctx, smp_service_group ssg) {
    return do_with(op_context(std::move(ctx), ssg), [](op_context& octx) {
        kreq_log.debug("handling produce request {}", octx.request);

        // dispatch produce requests for each topic
        auto topics = produce_topics(octx);

        // collect topic responses and build the final response message
        return when_all_succeed(topics.begin(), topics.end())
          .then([&octx](std::vector<produce_response::topic> topics) {
              octx.response.topics = std::move(topics);

              auto resp = std::make_unique<response>();
              octx.response.encode(octx.rctx, *resp.get());
              return make_ready_future<response_ptr>(std::move(resp));
          });
    });
}

} // namespace kafka
