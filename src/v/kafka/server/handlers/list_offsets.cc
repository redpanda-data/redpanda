// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/list_offsets.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/namespace.h"
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

void list_offsets_response::encode(const request_context& ctx, response& resp) {
    // convert to version zero in which the data model supported returning
    // multiple offsets instead of just one
    if (ctx.header().version == api_version(0)) {
        for (auto& topic : data.topics) {
            for (auto& partition : topic.partitions) {
                partition.old_style_offsets.push_back(partition.offset());
            }
        }
    }
    data.encode(resp.writer(), ctx.header().version);
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
      model::kafka_namespace,
      model::get_source_topic(topic.name),
      part.partition_index);

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
                  partition->last_stable_offset()));
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
                    id, model::timestamp(-1), partition->last_stable_offset()));
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
              model::topic_namespace_view(
                model::kafka_namespace, model::get_source_topic(topic.name)),
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

template<>
ss::future<response_ptr>
list_offsets_handler::handle(request_context&& ctx, ss::smp_service_group ssg) {
    list_offsets_request request;
    request.decode(ctx.reader(), ctx.header().version);
    request.compute_duplicate_topics();
    klog.trace("Handling request {}", request);

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
