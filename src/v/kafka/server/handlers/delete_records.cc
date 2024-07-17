/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/delete_records.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "container/fragmented_vector.h"
#include "kafka/server/partition_proxy.h"
#include "model/fundamental.h"
#include "model/ktp.h"

#include <vector>

namespace kafka {

namespace {

/// Returned in responses where kafka::error_code is anything else then a value
/// of error_code::none
constexpr auto invalid_low_watermark = model::offset(-1);

/// Compare against user provided value of truncation offset, this value will
/// indicate to truncate at  the current partition high watermark
constexpr auto at_current_high_watermark = model::offset(-1);

chunked_vector<delete_records_partition_result>
make_partition_errors(const delete_records_topic& t, error_code ec) {
    chunked_vector<delete_records_partition_result> r;
    for (const auto& p : t.partitions) {
        r.push_back(delete_records_partition_result{
          .partition_index = p.partition_index,
          .low_watermark = invalid_low_watermark,
          .error_code = ec});
    }
    return r;
}

/// Performs validation of topics, any failures will result in a list of
/// partitions that all contain the identical error codes
chunked_vector<delete_records_partition_result>
validate_at_topic_level(request_context& ctx, const delete_records_topic& t) {
    if (ctx.recovery_mode_enabled()) {
        return make_partition_errors(t, error_code::policy_violation);
    }

    const auto is_deletable = [](const cluster::topic_configuration& cfg) {
        if (cfg.is_read_replica()) {
            return false;
        }
        /// Immitates the logic in ntp_config::is_collectable
        if (
          !cfg.properties.has_overrides()
          || !cfg.properties.cleanup_policy_bitflags) {
            return true;
        }
        const auto& bitflags = cfg.properties.cleanup_policy_bitflags;
        return model::is_deletion_enabled(*bitflags);
    };
    const auto is_nodelete_topic = [](const delete_records_topic& t) {
        const auto& nodelete_topics
          = config::shard_local_cfg().kafka_nodelete_topics();
        return std::find_if(
                 nodelete_topics.begin(),
                 nodelete_topics.end(),
                 [&t](const ss::sstring& name) { return name == t.name; })
               != nodelete_topics.end();
    };

    const auto cfg = ctx.metadata_cache().get_topic_cfg(
      model::topic_namespace_view(model::kafka_namespace, t.name));
    if (!cfg) {
        return make_partition_errors(t, error_code::unknown_topic_or_partition);
    } else if (!is_deletable(*cfg)) {
        return make_partition_errors(t, error_code::policy_violation);
    } else if (is_nodelete_topic(t)) {
        return make_partition_errors(t, error_code::invalid_topic_exception);
    }
    return {};
}

/// Result set includes topic for later group-by topic logic
using result_t = std::tuple<model::topic, delete_records_partition_result>;

result_t make_partition_error(const model::ktp& ktp, error_code err) {
    return std::make_tuple(
      ktp.get_topic(),
      delete_records_partition_result{
        .partition_index = ktp.get_partition(),
        .low_watermark = invalid_low_watermark,
        .error_code = err});
}

result_t
make_partition_response(const model::ktp& ktp, model::offset low_watermark) {
    return std::make_tuple(
      ktp.get_topic(),
      delete_records_partition_result{
        .partition_index = ktp.get_partition(),
        .low_watermark = low_watermark,
        .error_code = error_code::none});
}

/// If validation passes, attempts to prefix truncate the raft log at the given
/// offset. Returns a response that includes the new low watermark
ss::future<result_t> prefix_truncate(
  cluster::partition_manager& pm,
  model::ktp ktp,
  model::offset kafka_truncation_offset,
  std::chrono::milliseconds timeout_ms) {
    auto raw_partition = pm.get(ktp);
    if (!raw_partition) {
        co_return make_partition_error(
          ktp, error_code::unknown_topic_or_partition);
    }
    auto partition = make_partition_proxy(ktp, pm);
    if (!partition->is_leader()) {
        co_return make_partition_error(
          ktp, error_code::not_leader_for_partition);
    }
    if (kafka_truncation_offset == at_current_high_watermark) {
        /// User is requesting to truncate all data
        kafka_truncation_offset = partition->high_watermark();
    }
    if (kafka_truncation_offset < model::offset(0)) {
        co_return make_partition_error(ktp, error_code::offset_out_of_range);
    }

    /// Perform truncation at the requested offset. A special batch will be
    /// written to the log, eventually consumed by replicas via the
    /// new_log_eviction_stm, which will perform a prefix truncation at the
    /// given offset
    auto errc = co_await partition->prefix_truncate(
      kafka_truncation_offset, ss::lowres_clock::now() + timeout_ms);
    if (errc != error_code::none) {
        vlog(
          klog.info,
          "Possible failed attempted to truncate partition: {} error: {}",
          ktp,
          errc);
        co_return make_partition_error(ktp, errc);
    }
    /// Its ok to not call sync_start_offset() over start_offset() here because
    /// prefix_truncate() was called on the leader (this node) and it waits
    /// until the local start offset was updated
    const auto kafka_start_offset = partition->start_offset();
    vlog(
      klog.info,
      "Truncated partition: {} to offset: {}",
      ktp,
      kafka_start_offset);
    /// prefix_truncate() will return when the start_offset has been incremented
    /// to the desired new low watermark. No other guarantees about the system
    /// are made at this point. (i.e. if data on disk on this node or a replica
    /// has yet been deleted)
    co_return make_partition_response(ktp, kafka_start_offset);
}

} // namespace

template<>
ss::future<response_ptr>
delete_records_handler::handle(request_context ctx, ss::smp_service_group) {
    delete_records_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    delete_records_response response;

    auto begin = request.data.topics.begin();
    auto valid_range_end = request.data.topics.end();

    const auto is_authorized = [&ctx](const delete_records_topic& t) {
        return ctx.authorized(security::acl_operation::remove, t.name);
    };

    auto unauthorized_it = std::partition(
      begin, valid_range_end, is_authorized);

    if (!ctx.audit()) {
        response.data.topics.reserve(request.data.topics.size());
        std::transform(
          request.data.topics.begin(),
          request.data.topics.end(),
          std::back_inserter(response.data.topics),
          [](const delete_records_topic& t) {
              auto errs = make_partition_errors(
                t, error_code::broker_not_available);
              return delete_records_topic_result{
                .name = t.name, .partitions = std::move(errs)};
          });

        co_return co_await ctx.respond(std::move(response));
    }

    std::transform(
      unauthorized_it,
      valid_range_end,
      std::back_inserter(response.data.topics),
      [](const delete_records_topic& t) {
          auto errs = make_partition_errors(
            t, error_code::topic_authorization_failed);
          return delete_records_topic_result{
            .name = t.name, .partitions = std::move(errs)};
      });
    valid_range_end = unauthorized_it;

    std::vector<ss::future<result_t>> fs;

    std::for_each(
      begin,
      valid_range_end,
      [&fs, &ctx, &response, &request](const delete_records_topic& topic) {
          /// Topic level validation, errors will be all the same for each
          /// partition under the topic. Validation for individual partitions
          /// may happen in the inner for loop below.
          auto topic_level_errors = validate_at_topic_level(ctx, topic);
          if (!topic_level_errors.empty()) {
              response.data.topics.push_back(delete_records_topic_result{
                .name = topic.name,
                .partitions = std::move(topic_level_errors)});
              return;
          }

          const auto* disabled_set
            = ctx.metadata_cache().get_topic_disabled_set(
              model::topic_namespace_view{model::kafka_namespace, topic.name});

          for (auto& partition : topic.partitions) {
              auto ktp = model::ktp(topic.name, partition.partition_index);
              if (
                disabled_set
                && disabled_set->is_disabled(partition.partition_index)) {
                  fs.push_back(
                    ss::make_ready_future<result_t>(make_partition_error(
                      ktp, error_code::replica_not_available)));
                  continue;
              }
              auto shard = ctx.shards().shard_for(ktp);
              if (!shard) {
                  fs.push_back(
                    ss::make_ready_future<result_t>(make_partition_error(
                      ktp, error_code::unknown_topic_or_partition)));
                  return;
              }
              auto f
                = ctx.partition_manager()
                    .invoke_on(
                      *shard,
                      [ktp,
                       timeout = request.data.timeout_ms,
                       o = partition.offset](cluster::partition_manager& pm) {
                          return prefix_truncate(pm, ktp, o, timeout);
                      })
                    .handle_exception([ktp](std::exception_ptr eptr) {
                        vlog(
                          klog.error, "Caught unexpected exception: {}", eptr);
                        return make_partition_error(
                          ktp, error_code::unknown_server_error);
                    });
              fs.push_back(std::move(f));
          }
      });

    /// Perform prefix truncation on partitions
    auto results = co_await ss::when_all_succeed(fs.begin(), fs.end());

    /// Group results by topic
    using partition_results = chunked_vector<delete_records_partition_result>;
    absl::flat_hash_map<model::topic, partition_results> group_by_topic;
    for (auto& [name, partitions] : results) {
        group_by_topic[name].push_back(std::move(partitions));
    }

    /// Map to kafka response type
    for (auto& [topic, partition_results] : group_by_topic) {
        response.data.topics.push_back(delete_records_topic_result{
          .name = topic, .partitions = std::move(partition_results)});
    }
    co_return co_await ctx.respond(std::move(response));
}
} // namespace kafka
