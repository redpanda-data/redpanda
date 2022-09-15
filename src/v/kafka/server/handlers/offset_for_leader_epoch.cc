// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/offset_for_leader_epoch.h"

#include "cluster/shard_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_response.h"
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/request_context.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "security/acl.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <vector>

namespace kafka {

using response_t = offset_for_leader_epoch_response;

struct ntp_last_offset_request {
    model::ntp ntp;
    kafka::leader_epoch requested_epoch;
    kafka::leader_epoch current_epoch;
};

struct shard_op_ctx {
    std::vector<ntp_last_offset_request> requests;
    std::vector<std::reference_wrapper<epoch_end_offset>> responses;
};
/**
 * Epoch end offset is an offset passed the last record in when current epoch is
 * requested, or first offset of leader epoch next to the one requested
 */

model::offset
get_epoch_end_offset(kafka::leader_epoch epoch, const partition_proxy& p) {
    return p.get_leader_epoch_last_offset(epoch).value_or(model::offset{-1});
}

static ss::future<std::vector<epoch_end_offset>> fetch_offsets_from_shard(
  request_context& ctx,
  ss::shard_id shard,
  std::vector<ntp_last_offset_request> requests) {
    return ss::smp::submit_to(shard, [&ctx, requests = std::move(requests)] {
        std::vector<epoch_end_offset> ret;
        ret.reserve(requests.size());
        for (auto& r : requests) {
            auto p = make_partition_proxy(
              r.ntp,
              ctx.partition_manager().local(),
              ctx.coproc_partition_manager().local());
            // offsets_for_leader_epoch request should only be answered by
            // leader
            if (!p || !p->is_leader()) {
                ret.push_back(response_t::make_epoch_end_offset(
                  r.ntp.tp.partition, error_code::not_leader_for_partition));
                continue;
            }

            auto l_epoch_error = details::check_leader_epoch(
              r.current_epoch, *p);
            if (l_epoch_error != error_code::none) {
                ret.push_back(response_t::make_epoch_end_offset(
                  r.ntp.tp.partition, l_epoch_error));
                continue;
            }

            ret.push_back(response_t::make_epoch_end_offset(
              r.ntp.tp.partition,
              get_epoch_end_offset(r.requested_epoch, *p),
              p->leader_epoch()));
        }
        return ret;
    });
}

static ss::future<> fetch_offsets_from_shards(
  request_context& ctx,
  absl::flat_hash_map<ss::shard_id, shard_op_ctx> requests_per_shard) {
    using value_t = absl::flat_hash_map<ss::shard_id, shard_op_ctx>::value_type;
    return ss::parallel_for_each(
      std::move(requests_per_shard), [&ctx](value_t& p) {
          return fetch_offsets_from_shard(
                   ctx, p.first, std::move(p.second.requests))
            .then([responses = std::move(p.second.responses)](
                    std::vector<epoch_end_offset> results) mutable {
                auto it = responses.begin();
                vassert(
                  results.size() == responses.size(),
                  "expected to have end epoch result for each requested "
                  "partition. Requested partitions: {}, results: {}",
                  results.size(),
                  responses.size());
                for (auto& r : results) {
                    it->get() = std::move(r);
                    ++it;
                }
            });
      });
}

static ss::future<std::vector<offset_for_leader_topic_result>>
get_offsets_for_leader_epochs(
  request_context& ctx, std::vector<offset_for_leader_topic> topics) {
    std::vector<offset_for_leader_topic_result> result;
    result.reserve(topics.size());

    absl::flat_hash_map<ss::shard_id, shard_op_ctx> requests_per_shard;

    for (auto& request_topic : topics) {
        result.push_back(
          offset_for_leader_topic_result{.topic = request_topic.topic});
        result.back().partitions.reserve(request_topic.partitions.size());

        for (auto& request_partition : request_topic.partitions) {
            // add response placeholder
            result.back().partitions.push_back(epoch_end_offset{});
            // we are reserving both topics and partitions, reference to
            // response is stable and we can capture it
            auto& partition_response = result.back().partitions.back();

            auto ntp = model::ntp(
              model::kafka_namespace,
              request_topic.topic,
              request_partition.partition);

            if (!ctx.metadata_cache().contains(ntp)) {
                partition_response = response_t::make_epoch_end_offset(
                  request_partition.partition,
                  error_code::unknown_topic_or_partition);
                continue;
            }

            auto shard = ctx.shards().shard_for(ntp);
            // no shard found, we may be in the middle of partition move, return
            // not leader for partition error
            if (!shard) {
                partition_response = response_t::make_epoch_end_offset(
                  request_partition.partition,
                  error_code::not_leader_for_partition);
                continue;
            }

            ntp_last_offset_request req{
              .ntp = std::move(ntp),
              .requested_epoch = request_partition.leader_epoch,
              .current_epoch = request_partition.current_leader_epoch,
            };
            auto& per_shard = requests_per_shard[*shard];
            per_shard.requests.push_back(std::move(req));
            per_shard.responses.push_back(std::ref(partition_response));
        }
    }
    co_await fetch_offsets_from_shards(ctx, std::move(requests_per_shard));
    co_return result;
}

template<>
ss::future<response_ptr> offset_for_leader_epoch_handler::handle(
  request_context ctx, ss::smp_service_group ssg) {
    offset_for_leader_epoch_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    std::vector<offset_for_leader_topic_result> unauthorized;

    // authorize
    if (!ctx.authorized(
          security::acl_operation::cluster_action,
          security::default_cluster_name)) {
        auto it = std::stable_partition(
          request.data.topics.begin(),
          request.data.topics.end(),
          [&ctx](const offset_for_leader_topic& topic) {
              return ctx.authorized(
                security::acl_operation::describe, topic.topic);
          });

        unauthorized.reserve(std::distance(it, request.data.topics.end()));
        std::transform(
          it,
          request.data.topics.end(),
          std::back_inserter(unauthorized),
          [](offset_for_leader_topic& topic) {
              offset_for_leader_topic_result res;
              res.partitions.reserve(topic.partitions.size());
              for (auto& p : topic.partitions) {
                  res.partitions.push_back(response_t::make_epoch_end_offset(
                    p.partition, error_code::topic_authorization_failed));
              }
              return res;
          });
        // remove unauthorized topics
        request.data.topics.erase(it, request.data.topics.end());
    }

    // fetch offsets
    auto results = co_await get_offsets_for_leader_epochs(
      ctx, std::move(request.data.topics));

    offset_for_leader_epoch_response response;
    response.data.topics = std::move(results);
    response.data.throttle_time_ms = std::chrono::milliseconds(
      ctx.throttle_delay_ms());

    // merge with unauthorized topics
    std::move(
      unauthorized.begin(),
      unauthorized.end(),
      std::back_inserter(response.data.topics));

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
