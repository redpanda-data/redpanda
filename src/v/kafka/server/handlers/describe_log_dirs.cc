/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/describe_log_dirs.h"

#include "cluster/partition_manager.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

namespace kafka {

using partition_dir_set
  = absl::flat_hash_map<model::topic, std::vector<describe_log_dirs_partition>>;

static describe_log_dirs_partition describe_partition(cluster::partition& p) {
    return describe_log_dirs_partition{
      .partition_index = p.ntp().tp.partition(),
      .partition_size = static_cast<int64_t>(p.size_bytes()),
      .offset_lag = std::max(
        p.high_watermark()() - p.dirty_offset()(), int64_t(0)),
      .is_future_key = false,
    };
}

static partition_dir_set collect_mapper(
  cluster::partition_manager& pm,
  const std::optional<std::vector<describable_log_dir_topic>>& topics) {
    partition_dir_set ret;

    /*
     * return all partitions
     */
    if (!topics) {
        for (const auto& partition : pm.partitions()) {
            if (partition.first.ns != model::kafka_namespace) {
                continue;
            }
            ret[partition.first.tp.topic].push_back(
              describe_partition(*partition.second));
        }
        return ret;
    }

    /*
     * return only partition matching request
     */
    for (const auto& topic : *topics) {
        for (auto p_id : topic.partition_index) {
            model::ntp ntp(model::kafka_namespace, topic.topic, p_id);
            if (auto p = pm.get(ntp); p) {
                ret[topic.topic].push_back(describe_partition(*p));
            }
        }
    }
    return ret;
}

/*
 * collect log directory information for partitions
 */
static ss::future<partition_dir_set> collect(
  request_context& ctx,
  std::optional<std::vector<describable_log_dir_topic>> filter) {
    return ctx.partition_manager().map_reduce0(
      [filter = std::move(filter)](cluster::partition_manager& pm) {
          return collect_mapper(pm, filter);
      },
      partition_dir_set{},
      [](partition_dir_set acc, const partition_dir_set& update) {
          for (auto& topic : update) {
              for (auto partition : topic.second) {
                  acc[topic.first].push_back(partition);
              }
          }
          return acc;
      });
}

template<>
ss::future<response_ptr> describe_log_dirs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_log_dirs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_log_dirs_response response;

    // redpanda only supports a single data directory right now
    response.data.results.push_back(describe_log_dirs_result{
      .error_code = error_code::none,
      .log_dir = config::node().data_directory().as_sstring(),
    });

    if (!ctx.authorized(
          security::acl_operation::describe, security::default_cluster_name)) {
        // in this case kafka returns no authorization error
        co_return co_await ctx.respond(std::move(response));
    }

    auto partitions = co_await collect(ctx, std::move(request.data.topics));

    while (!partitions.empty()) {
        auto node = partitions.extract(partitions.begin());
        response.data.results.back().topics.push_back(describe_log_dirs_topic{
          .name = std::move(node.key()),
          .partitions = std::move(node.mapped()),
        });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
