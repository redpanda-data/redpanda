// Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/materialized_partition.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
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

struct list_offsets_ctx {
    request_context rctx;
    list_offsets_request request;
    list_offsets_response response;
    ss::smp_service_group ssg;
    std::vector<list_offset_topic> unauthorized_topics;

    list_offsets_ctx(
      request_context&& rctx,
      list_offsets_request&& request,
      ss::smp_service_group ssg,
      std::vector<list_offset_topic> unauthorized_topics)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg)
      , unauthorized_topics(std::move(unauthorized_topics)) {}
};

static ss::future<list_offset_partition_response> list_offsets_partition(
  list_offsets_ctx& octx,
  model::timestamp timestamp,
  model::ntp ntp,
  model::isolation_level isolation_lvl,
  kafka::leader_epoch current_leader_epoch,
  cluster::partition_manager& mgr) {
    auto kafka_partition = make_partition_proxy(
      ntp, mgr, octx.rctx.coproc_partition_manager().local());
    if (!kafka_partition) {
        co_return list_offsets_response::make_partition(
          ntp.tp.partition, error_code::unknown_topic_or_partition);
    }

    // using linearizable_barrier instead of is_leader to check that
    // current node is/was a leader at the moment it received the request
    // since the former uses cache and may return stale data
    auto err = co_await kafka_partition->linearizable_barrier();
    if (err) {
        co_return list_offsets_response::make_partition(
          ntp.tp.partition, error_code::not_leader_for_partition);
    }

    /**
     * validate leader epoch. for more details see KIP-320
     */
    auto leader_epoch_err = details::check_leader_epoch(
      current_leader_epoch, *kafka_partition);
    if (leader_epoch_err != error_code::none) {
        co_return list_offsets_response::make_partition(
          ntp.tp.partition, leader_epoch_err);
    }

    auto offset = kafka_partition->high_watermark();
    if (isolation_lvl == model::isolation_level::read_committed) {
        auto maybe_lso = kafka_partition->last_stable_offset();
        if (unlikely(!maybe_lso)) {
            co_return list_offsets_response::make_partition(
              ntp.tp.partition, maybe_lso.error());
        }
        offset = maybe_lso.value();
    }

    /*
     * the responses for earliest/latest timestamp queries do not require
     * that the actual timestamp be returned. only the offset is required.
     */
    if (timestamp == list_offsets_request::earliest_timestamp) {
        co_return list_offsets_response::make_partition(
          ntp.tp.partition,
          model::timestamp(-1),
          kafka_partition->start_offset(),
          kafka_partition->leader_epoch());

    } else if (timestamp == list_offsets_request::latest_timestamp) {
        co_return list_offsets_response::make_partition(
          ntp.tp.partition,
          model::timestamp(-1),
          offset,
          kafka_partition->leader_epoch());
    }
    auto res = co_await kafka_partition->timequery(storage::timequery_config{
      timestamp,
      offset,
      kafka_read_priority(),
      {model::record_batch_type::raft_data}});
    auto id = ntp.tp.partition;
    if (res) {
        co_return list_offsets_response::make_partition(
          id, res->time, res->offset, kafka_partition->leader_epoch());
    }
    co_return list_offsets_response::make_partition(id, error_code::none);
}

static ss::future<list_offset_partition_response> list_offsets_partition(
  list_offsets_ctx& octx,
  model::timestamp timestamp,
  list_offset_topic& topic,
  list_offset_partition& part) {
    model::ntp ntp(model::kafka_namespace, topic.name, part.partition_index);

    auto shard = octx.rctx.shards().shard_for(ntp);
    if (!shard) {
        return ss::make_ready_future<list_offset_partition_response>(
          list_offsets_response::make_partition(
            ntp.tp.partition, error_code::unknown_topic_or_partition));
    }

    return octx.rctx.partition_manager().invoke_on(
      *shard,
      octx.ssg,
      [timestamp,
       &octx,
       ntp = std::move(ntp),
       isolation_lvl = model::isolation_level(
         octx.request.data.isolation_level),
       current_leader_epoch = part.current_leader_epoch](
        cluster::partition_manager& mgr) mutable {
          return list_offsets_partition(
            octx,
            timestamp,
            std::move(ntp),
            isolation_lvl,
            current_leader_epoch,
            mgr);
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
              model::topic_namespace_view(model::kafka_namespace, topic.name),
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

/*
 * Prepare unauthorized error response for each unauthorized topic
 */
static void handle_unauthorized(list_offsets_ctx& octx) {
    octx.response.data.topics.reserve(
      octx.response.data.topics.size() + octx.unauthorized_topics.size());
    for (auto& topic : octx.unauthorized_topics) {
        std::vector<list_offset_partition_response> partitions;
        partitions.reserve(topic.partitions.size());
        for (auto& partition : topic.partitions) {
            partitions.push_back(list_offset_partition_response(
              list_offsets_response::make_partition(
                partition.partition_index,
                error_code::topic_authorization_failed)));
        }
        octx.response.data.topics.push_back(list_offset_topic_response{
          .name = std::move(topic.name),
          .partitions = std::move(partitions),
        });
    }
}

template<>
ss::future<response_ptr>
list_offsets_handler::handle(request_context ctx, ss::smp_service_group ssg) {
    list_offsets_request request;
    request.decode(ctx.reader(), ctx.header().version);
    request.compute_duplicate_topics();
    log_request(ctx.header(), request);

    auto unauthorized_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const list_offset_topic& topic) {
          return ctx.authorized(security::acl_operation::describe, topic.name);
      });

    std::vector<list_offset_topic> unauthorized_topics(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics.end()));

    request.data.topics.erase(unauthorized_it, request.data.topics.end());

    list_offsets_ctx octx(
      std::move(ctx), std::move(request), ssg, std::move(unauthorized_topics));

    return ss::do_with(std::move(octx), [](list_offsets_ctx& octx) {
        auto topics = list_offsets_topics(octx);
        return when_all_succeed(topics.begin(), topics.end())
          .then([&octx](std::vector<list_offset_topic_response> topics) {
              octx.response.data.topics = std::move(topics);
              handle_unauthorized(octx);
              return octx.rctx.respond(std::move(octx.response));
          });
    });
}

} // namespace kafka
