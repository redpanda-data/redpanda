#include "kafka/requests/list_offsets_request.h"

#include "cluster/namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "resource_mgmt/io_priority.h"

namespace kafka {

void list_offsets_request::encode(
  response_writer& writer, api_version version) {
    writer.write(replica_id);
    if (version >= api_version(2)) {
        writer.write(int8_t(isolation_level));
    }
    writer.write_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](partition& partition, response_writer& writer) {
                writer.write(partition.id);
                writer.write(partition.timestamp());
            });
      });
}

void list_offsets_request::decode(request_context& ctx) {
    const auto version = ctx.header().version;
    auto& reader = ctx.reader();

    replica_id = model::node_id(reader.read_int32());
    if (version >= api_version(2)) {
        isolation_level = reader.read_int8();
    }
    topics = reader.read_array([version](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([version](request_reader& reader) {
              return partition{
                .id = model::partition_id(reader.read_int32()),
                .timestamp = model::timestamp(reader.read_int64()),
              };
          }),
        };
    });

    /*
     * compute a set of duplicate topic partitions. kafka has special error
     * handling if a client requests list offsets with duplicate topic
     * partitions. TODO(noah) this may not actually matter. we should take a
     * closer look at clients and how this would manifest as an issue.
     */
    absl::btree_set<model::topic_partition> seen;
    for (const auto& topic : topics) {
        for (const auto& part : topic.partitions) {
            model::topic_partition tp{
              .topic = topic.name,
              .partition = part.id,
            };
            if (!seen.insert(tp).second) {
                tp_dups.insert(std::move(tp));
            }
        }
    }
}

void list_offsets_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    const auto version = ctx.header().version;

    if (version >= api_version(2)) {
        writer.write(int32_t(throttle_time_ms.count()));
    }
    writer.write_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](partition& partition, response_writer& writer) {
                writer.write(partition.id);
                writer.write(partition.error);
                writer.write(partition.timestamp());
                writer.write(partition.offset);
            });
      });
}

void list_offsets_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    if (version >= api_version(2)) {
        throttle_time_ms = std::chrono::milliseconds(reader.read_int32());
    }
    topics = reader.read_array([version](request_reader& reader) {
        kreq_log.info("VVV c");
        auto name = model::topic(reader.read_string());
        auto partitions = reader.read_array([version](request_reader& reader) {
            auto id = model::partition_id(reader.read_int32());
            auto error = error_code(reader.read_int16());
            auto time = model::timestamp(reader.read_int64());
            auto offset = model::offset(reader.read_int64());
            return partition{id, error, time, offset};
        });
        return topic{std::move(name), std::move(partitions)};
    });
}

struct list_offsets_ctx {
    request_context rctx;
    ss::smp_service_group ssg;
    list_offsets_request request;
    list_offsets_response response;

    list_offsets_ctx(
      request_context&& rctx,
      list_offsets_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

static ss::future<list_offsets_response::partition> list_offsets_partition(
  list_offsets_ctx& octx,
  model::timestamp timestamp,
  list_offsets_request::topic& topic,
  list_offsets_request::partition& part) {
    auto ntp = model::ntp{
      .ns = cluster::kafka_namespace,
      .tp = model::topic_partition{
        .topic = topic.name,
        .partition = part.id,
      },
    };

    auto shard = octx.rctx.shards().shard_for(ntp);
    if (!shard) {
        return ss::make_ready_future<list_offsets_response::partition>(
          list_offsets_response::partition(
            ntp.tp.partition, error_code::unknown_topic_or_partition));
    }

    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [timestamp, ntp = std::move(ntp)](cluster::partition_manager& mgr) {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  ntp.tp.partition, error_code::unknown_topic_or_partition));
          }

          if (!partition->is_leader()) {
              return ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  ntp.tp.partition, error_code::not_leader_for_partition));
          }

          /*
           * the responses for earliest/latest timestamp queries do not require
           * that the actual timestamp be returned. only the offset is required.
           */
          if (timestamp == list_offsets_request::earliest_timestamp) {
              return ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  ntp.tp.partition,
                  model::timestamp(-1),
                  partition->start_offset()));

          } else if (timestamp == list_offsets_request::latest_timestamp) {
              return ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  ntp.tp.partition,
                  model::timestamp(-1),
                  partition->committed_offset()));
          }

          return partition->timequery(timestamp, kafka_read_priority())
            .then([partition, id = ntp.tp.partition](
                    std::optional<storage::timequery_result> res) {
                if (res) {
                    return ss::make_ready_future<
                      list_offsets_response::partition>(
                      list_offsets_response::partition(
                        id, res->time, res->offset));
                }
                return ss::make_ready_future<list_offsets_response::partition>(
                  list_offsets_response::partition(
                    id, model::timestamp(-1), partition->committed_offset()));
            });
      });
}

static ss::future<list_offsets_response::topic>
list_offsets_topic(list_offsets_ctx& octx, list_offsets_request::topic& topic) {
    std::vector<ss::future<list_offsets_response::partition>> partitions;
    partitions.reserve(topic.partitions.size());

    for (auto& part : topic.partitions) {
        if (octx.request.duplicate_tp(topic.name, part.id)) {
            partitions.push_back(
              ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  part.id, error_code::invalid_request)));
            continue;
        }

        if (!octx.rctx.metadata_cache().contains(topic.name, part.id)) {
            partitions.push_back(
              ss::make_ready_future<list_offsets_response::partition>(
                list_offsets_response::partition(
                  part.id, error_code::unknown_topic_or_partition)));
            continue;
        }

        auto pr = list_offsets_partition(octx, part.timestamp, topic, part);
        partitions.push_back(std::move(pr));
    }

    return when_all_succeed(partitions.begin(), partitions.end())
      .then([name = std::move(topic.name)](
              std::vector<list_offsets_response::partition> parts) mutable {
          return list_offsets_response::topic{
            .name = std::move(name),
            .partitions = std::move(parts),
          };
      });
}

static std::vector<ss::future<list_offsets_response::topic>>
list_offsets_topics(list_offsets_ctx& octx) {
    std::vector<ss::future<list_offsets_response::topic>> topics;
    topics.reserve(octx.request.topics.size());

    for (auto& topic : octx.request.topics) {
        auto tr = list_offsets_topic(octx, topic);
        topics.push_back(std::move(tr));
    }

    return topics;
}

ss::future<response_ptr>
list_offsets_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    list_offsets_request request(ctx);
    return ss::do_with(
      list_offsets_ctx(std::move(ctx), std::move(request), ssg),
      [](list_offsets_ctx& octx) {
          auto topics = list_offsets_topics(octx);
          return when_all_succeed(topics.begin(), topics.end())
            .then([&octx](std::vector<list_offsets_response::topic> topics) {
                octx.response.topics = std::move(topics);
                return octx.rctx.respond(std::move(octx.response));
            });
      });
}

} // namespace kafka
