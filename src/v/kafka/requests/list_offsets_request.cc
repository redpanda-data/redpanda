#include "kafka/requests/list_offsets_request.h"

#include "cluster/namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "resource_mgmt/io_priority.h"

namespace kafka {

void list_offsets_request::compute_duplicate_topics() {
    /*
     * compute a set of duplicate topic partitions. kafka has special error
     * handling if a client requests list offsets with duplicate topic
     * partitions. TODO(noah) this may not actually matter. we should take a
     * closer look at clients and how this would manifest as an issue.
     */
    absl::btree_set<model::topic_partition> seen;
    for (const auto& topic : data.topics) {
        for (const auto& part : topic.partitions) {
            model::topic_partition tp(topic.name, part.partition_index);
            if (!seen.insert(tp).second) {
                tp_dups.insert(std::move(tp));
            }
        }
    }
}

struct list_offsets_ctx {
    request_context rctx;
    list_offsets_request request;
    list_offsets_response response;
    ss::smp_service_group ssg;

    list_offsets_ctx(
      request_context&& rctx,
      list_offsets_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

static ss::future<list_offset_partition_response> list_offsets_partition(
  list_offsets_ctx& octx,
  model::timestamp timestamp,
  list_offset_topic& topic,
  list_offset_partition& part) {
    auto ntp = model::ntp(
      cluster::kafka_namespace, topic.name, part.partition_index);

    auto shard = octx.rctx.shards().shard_for(ntp);
    if (!shard) {
        return ss::make_ready_future<list_offset_partition_response>(
          list_offsets_response::make_partition(
            ntp.tp.partition, error_code::unknown_topic_or_partition));
    }

    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [timestamp, ntp = std::move(ntp)](cluster::partition_manager& mgr) {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  ntp.tp.partition, error_code::unknown_topic_or_partition));
          }

          if (!partition->is_leader()) {
              return ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  ntp.tp.partition, error_code::not_leader_for_partition));
          }

          /*
           * the responses for earliest/latest timestamp queries do not require
           * that the actual timestamp be returned. only the offset is required.
           */
          if (timestamp == list_offsets_request::earliest_timestamp) {
              return ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  ntp.tp.partition,
                  model::timestamp(-1),
                  partition->start_offset()));

          } else if (timestamp == list_offsets_request::latest_timestamp) {
              return ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  ntp.tp.partition,
                  model::timestamp(-1),
                  partition->committed_offset()));
          }

          return partition->timequery(timestamp, kafka_read_priority())
            .then([partition, id = ntp.tp.partition](
                    std::optional<storage::timequery_result> res) {
                if (res) {
                    return ss::make_ready_future<
                      list_offset_partition_response>(
                      list_offsets_response::make_partition(
                        id, res->time, res->offset));
                }
                return ss::make_ready_future<list_offset_partition_response>(
                  list_offsets_response::make_partition(
                    id, model::timestamp(-1), partition->committed_offset()));
            });
      });
}

static ss::future<list_offset_topic_response>
list_offsets_topic(list_offsets_ctx& octx, list_offset_topic& topic) {
    std::vector<ss::future<list_offset_partition_response>> partitions;
    partitions.reserve(topic.partitions.size());

    for (auto& part : topic.partitions) {
        if (octx.request.duplicate_tp(topic.name, part.partition_index)) {
            partitions.push_back(
              ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  part.partition_index, error_code::invalid_request)));
            continue;
        }

        if (!octx.rctx.metadata_cache().contains(
              model::topic_namespace_view(cluster::kafka_namespace, topic.name),
              part.partition_index)) {
            partitions.push_back(
              ss::make_ready_future<list_offset_partition_response>(
                list_offsets_response::make_partition(
                  part.partition_index,
                  error_code::unknown_topic_or_partition)));
            continue;
        }

        auto pr = list_offsets_partition(octx, part.timestamp, topic, part);
        partitions.push_back(std::move(pr));
    }

    return when_all_succeed(partitions.begin(), partitions.end())
      .then([name = std::move(topic.name)](
              std::vector<list_offset_partition_response> parts) mutable {
          return list_offset_topic_response{
            .name = std::move(name),
            .partitions = std::move(parts),
          };
      });
}

static std::vector<ss::future<list_offset_topic_response>>
list_offsets_topics(list_offsets_ctx& octx) {
    std::vector<ss::future<list_offset_topic_response>> topics;
    topics.reserve(octx.request.data.topics.size());

    for (auto& topic : octx.request.data.topics) {
        auto tr = list_offsets_topic(octx, topic);
        topics.push_back(std::move(tr));
    }

    return topics;
}

ss::future<response_ptr>
list_offsets_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    list_offsets_request request;
    request.decode(ctx.reader(), ctx.header().version);
    request.compute_duplicate_topics();

    return ss::do_with(
      list_offsets_ctx(std::move(ctx), std::move(request), ssg),
      [](list_offsets_ctx& octx) {
          auto topics = list_offsets_topics(octx);
          return when_all_succeed(topics.begin(), topics.end())
            .then([&octx](std::vector<list_offset_topic_response> topics) {
                octx.response.data.topics = std::move(topics);
                return octx.rctx.respond(std::move(octx.response));
            });
      });
}

} // namespace kafka
