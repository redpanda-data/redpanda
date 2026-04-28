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
#include "container/chunked_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace kafka {

namespace describe_log_dirs::detail {

using partition_dir_set
  = chunked_hash_map<model::topic, chunked_vector<log_partition_data>>;

ss::future<log_partition_data>
describe_partition(kafka::partition_proxy& p, bool include_remote) {
    auto result = log_partition_data{
      .local = describe_log_dirs_partition{
        .partition_index = p.ntp().tp.partition(),
        .partition_size = static_cast<int64_t>(p.local_size_bytes()),
        .offset_lag = std::max(p.offset_lag()(), int64_t(0)),
        .is_future_key = false,
      }};

    auto cloud_space = co_await p.cloud_size_bytes();
    if (cloud_space.has_value() && include_remote) {
        result.remote = describe_log_dirs_partition{
          .partition_index = p.ntp().tp.partition(),
          .partition_size = static_cast<int64_t>(cloud_space.value()),
          .offset_lag = std::max(p.offset_lag()(), int64_t(0)),
          .is_future_key = false,
        };
    }

    co_return result;
}

static ss::future<partition_dir_set> collect_mapper(
  cluster::partition_manager& pm,
  const chunked_vector<describable_log_dir_topic>* topics,
  bool include_remote) {
    partition_dir_set ret;

    /*
     * return all partitions
     */
    if (topics == nullptr) {
        for (const auto& partition : pm.partitions()) {
            auto ktp = model::ktp(
              partition.second->ntp().tp.topic,
              partition.second->ntp().tp.partition);
            auto proxy = make_partition_proxy(ktp, pm);
            if (proxy) {
                ret[partition.first.tp.topic].push_back(
                  co_await describe_partition(*proxy, include_remote));
            }
        }
        co_return ret;
    }

    /*
     * return only partition matching request
     */
    for (const auto& topic : *topics) {
        for (auto p_id : topic.partition_index) {
            auto ktp = model::ktp(topic.topic, p_id);
            auto proxy = make_partition_proxy(ktp, pm);
            if (proxy) {
                ret[topic.topic].push_back(
                  co_await describe_partition(*proxy, include_remote));
            }
        }
    }
    co_return ret;
}

/*
 * collect log directory information for partitions
 *
 * filter is borrowed: it is owned by the awaiting handle() coroutine,
 * which outlives this map_reduce0. Read-only sharing across shards is
 * safe.
 */
static ss::future<partition_dir_set> collect(
  request_context& ctx,
  const chunked_vector<describable_log_dir_topic>* filter,
  bool include_remote) {
    return ctx.partition_manager().map_reduce0(
      [filter, include_remote](cluster::partition_manager& pm) {
          return collect_mapper(pm, filter, include_remote);
      },
      partition_dir_set{},
      [](partition_dir_set acc, const partition_dir_set& update) {
          for (auto& topic : update) {
              for (auto partition : topic.second) {
                  acc[topic.first].push_back(std::move(partition));
              }
          }
          return acc;
      });
}

} // namespace describe_log_dirs::detail

template<>
ss::future<response_ptr> describe_log_dirs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_log_dirs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_log_dirs_response response;

    // redpanda only supports a single data directory right now
    response.data.results.push_back(
      describe_log_dirs_result{
        .error_code = error_code::none,
        .log_dir = config::node().data_directory().as_sstring(),
      });

    // We don't know until we iterate over topics whether any of them are
    // using cloud storage, so create the result structure speculatively.
    auto bucket_name
      = config::shard_local_cfg().cloud_storage_bucket().value_or("");
    response.data.results.push_back(
      describe_log_dirs_result{
        .error_code = error_code::none,
        .log_dir = fmt::format("remote://{}", bucket_name),
      });

    auto authz = ctx.authorized(
      security::acl_operation::describe, security::default_cluster_name);

    if (!ctx.audit()) {
        response.data.error_code = error_code::broker_not_available;
        co_return co_await ctx.respond(std::move(response));
    }

    if (!authz) {
        // in this case kafka returns no authorization error
        co_return co_await ctx.respond(std::move(response));
    }

    auto& local_results = response.data.results.at(0);
    auto& remote_results = response.data.results.at(1);

    auto include_remote = config::shard_local_cfg()
                            .kafka_enable_describe_log_dirs_remote_storage();
    const auto* topics_filter = request.data.topics ? &*request.data.topics
                                                    : nullptr;
    auto partitions = co_await describe_log_dirs::detail::collect(
      ctx, topics_filter, include_remote);
    while (!partitions.empty()) {
        auto node = partitions.extract(partitions.begin());

        chunked_vector<describe_log_dirs_partition> local_partitions;
        chunked_vector<describe_log_dirs_partition> remote_partitions;
        for (const auto& i : node.second) {
            local_partitions.push_back(i.local);
            if (i.remote.has_value()) {
                remote_partitions.push_back(i.remote.value());
            }
        }

        local_results.topics.push_back(
          describe_log_dirs_topic{
            .name = node.first,
            .partitions = std::move(local_partitions),
          });
        if (!remote_partitions.empty()) {
            remote_results.topics.push_back(
              describe_log_dirs_topic{
                .name = std::move(node.first),
                .partitions = std::move(remote_partitions),
              });
        }
    }

    if (remote_results.topics.empty()) {
        // Drop the remote storage part of the response if we had no
        // partitions with remote data to report
        response.data.results.pop_back();
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
