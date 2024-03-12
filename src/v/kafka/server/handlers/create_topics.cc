// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/create_topics.h"

#include "cluster/cluster_utils.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/timeout.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/quota_manager.h"
#include "kafka/types.h"
#include "model/metadata.h"
#include "security/acl.h"
#include "utils/to_string.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <array>
#include <chrono>
#include <optional>
#include <string_view>

namespace kafka {

static constexpr auto supported_configs = std::to_array(
  {topic_property_compression,
   topic_property_cleanup_policy,
   topic_property_timestamp_type,
   topic_property_segment_size,
   topic_property_compaction_strategy,
   topic_property_retention_bytes,
   topic_property_retention_duration,
   topic_property_recovery,
   topic_property_remote_write,
   topic_property_remote_read,
   topic_property_remote_delete,
   topic_property_read_replica,
   topic_property_max_message_bytes,
   topic_property_retention_local_target_bytes,
   topic_property_retention_local_target_ms,
   topic_property_segment_ms,
   topic_property_record_key_schema_id_validation,
   topic_property_record_key_schema_id_validation_compat,
   topic_property_record_key_subject_name_strategy,
   topic_property_record_key_subject_name_strategy_compat,
   topic_property_record_value_schema_id_validation,
   topic_property_record_value_schema_id_validation_compat,
   topic_property_record_value_subject_name_strategy,
   topic_property_record_value_subject_name_strategy_compat,
   topic_property_initial_retention_local_target_bytes,
   topic_property_initial_retention_local_target_ms});

bool is_supported(std::string_view name) {
    return std::any_of(
      supported_configs.begin(),
      supported_configs.end(),
      [name](std::string_view p) { return name == p; });
}

using validators = make_validator_types<
  creatable_topic,
  custom_partition_assignment_negative_partition_count,
  partition_count_must_be_positive,
  replication_factor_must_be_positive,
  replication_factor_must_be_odd,
  replicas_diversity,
  compression_type_validator,
  compaction_strategy_validator,
  timestamp_type_validator,
  cleanup_policy_validator,
  remote_read_and_write_are_not_supported_for_read_replica,
  batch_max_bytes_limits,
  subject_name_strategy_validator,
  replication_factor_must_be_greater_or_equal_to_minimum>;

static void
append_topic_configs(request_context& ctx, create_topics_response& response) {
    for (auto& ct_result : response.data.topics) {
        if (ct_result.error_code != kafka::error_code::none) {
            ct_result.topic_config_error_code = ct_result.error_code;
            continue;
        }
        auto cfg = ctx.metadata_cache().get_topic_cfg(
          model::topic_namespace_view{model::kafka_namespace, ct_result.name});
        if (cfg) {
            ct_result.configs = std::make_optional(
              make_configs(ctx.metadata_cache(), cfg->properties));
            ct_result.topic_config_error_code = kafka::error_code::none;
        } else {
            // Topic was sucessfully created but metadata request did not
            // succeed, if possible, could mean topic was deleted just after
            // creation
            ct_result.topic_config_error_code
              = kafka::error_code::unknown_server_error;
        }
    }
}

static void append_topic_properties(
  request_context& ctx, create_topics_response& response) {
    for (auto& ct_result : response.data.topics) {
        auto cfg = ctx.metadata_cache().get_topic_cfg(
          model::topic_namespace_view{model::kafka_namespace, ct_result.name});
        if (cfg) {
            ct_result.num_partitions = cfg->partition_count;
            ct_result.replication_factor = cfg->replication_factor;
        }
    };
}

template<>
ss::future<response_ptr> create_topics_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    kafka::create_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    create_topics_response response;
    auto begin = request.data.topics.begin();

    // Duplicated topic names are not accepted
    auto valid_range_end = validate_range_duplicates(
      begin,
      request.data.topics.end(),
      std::back_inserter(response.data.topics));

    if (ctx.recovery_mode_enabled()) {
        for (const auto& t :
             boost::make_iterator_range(begin, valid_range_end)) {
            response.data.topics.push_back(generate_error(
              t, error_code::policy_violation, "Forbidden in recovery mode"));
        }
        co_return co_await ctx.respond(std::move(response));
    }

    auto additional_resources_func = [&request]() {
        std::vector<model::topic> topics;
        topics.reserve(request.data.topics.size());
        std::transform(
          request.data.topics.begin(),
          request.data.topics.end(),
          std::back_inserter(topics),
          [](const creatable_topic& t) { return t.name; });

        return topics;
    };

    const auto has_cluster_auth = ctx.authorized(
      security::acl_operation::create,
      security::default_cluster_name,
      std::move(additional_resources_func));

    if (!has_cluster_auth) {
        auto unauthorized_it = std::partition(
          begin, valid_range_end, [&ctx](const creatable_topic& t) {
              return ctx.authorized(security::acl_operation::create, t.name);
          });
        std::transform(
          unauthorized_it,
          valid_range_end,
          std::back_inserter(response.data.topics),
          [](const creatable_topic& t) {
              return generate_error(
                t, error_code::topic_authorization_failed, "Unauthorized");
          });
        valid_range_end = unauthorized_it;
    }

    if (!ctx.audit()) {
        request.data.topics.erase_to_end(valid_range_end);
        create_topics_response err_resp(
          error_code::broker_not_available,
          "Broker not available - audit system failure",
          std::move(response),
          std::move(request));

        if (ctx.header().version >= api_version(5)) {
            append_topic_configs(ctx, err_resp);
        }

        co_return co_await ctx.respond(err_resp);
    }

    // fill in defaults if necessary
    for (auto& r : boost::make_iterator_range(begin, valid_range_end)) {
        // skip custom assigned topics
        if (!r.assignments.empty()) {
            continue;
        }
        if (r.num_partitions == -1) {
            r.num_partitions
              = config::shard_local_cfg().default_topic_partitions();
        }
        if (r.replication_factor == -1) {
            r.replication_factor
              = config::shard_local_cfg().default_topic_replication();
        }
    }

    // Validate with validators
    valid_range_end = validate_requests_range(
      begin,
      valid_range_end,
      std::back_inserter(response.data.topics),
      validators{});

    // Print log if not supported configuration options are present
    for (auto& r : boost::make_iterator_range(begin, valid_range_end)) {
        for (auto c : r.configs) {
            if (!is_supported(c.name)) {
                vlog(
                  klog.info,
                  "topic {} not supported configuration {}={} property "
                  "will be ignored",
                  r.name,
                  c.name,
                  c.value);
            }
        }
    }

    if (request.data.validate_only) {
        // We do not actually create the topics, only validate the
        // request
        // Generate successes for topics that passed the
        // validation.
        std::transform(
          begin,
          valid_range_end,
          std::back_inserter(response.data.topics),
          [&ctx](const creatable_topic& t) {
              auto result = generate_successfull_result(t);
              if (ctx.metadata_cache().contains(model::topic_namespace_view{
                    model::kafka_namespace, t.name})) {
                  result.error_code = error_code::topic_already_exists;
                  return result;
              }
              result.num_partitions = t.num_partitions;
              result.replication_factor = t.replication_factor;
              if (ctx.header().version >= api_version(5)) {
                  // TODO(Rob): it looks like get_default_properties is used
                  // only there so there is a high chance of diverging
                  // dry run's default_properties upposed to be used on
                  // topic creation and the real deal
                  auto default_properties
                    = ctx.metadata_cache().get_default_properties();
                  result.configs = std::make_optional(
                    make_configs(ctx.metadata_cache(), default_properties));
              }
              return result;
          });
        co_return co_await ctx.respond(std::move(response));
    }

    // Record the number of partition mutations in each requested topic,
    // calulcating throttle delay if necessary
    auto quota_exceeded_it = co_await ssx::partition(
      begin, valid_range_end, [&ctx, &response](const creatable_topic& t) {
          /// Capture before next scheduling point below
          auto& response_ref = response;
          return ctx.quota_mgr()
            .record_partition_mutations(
              ctx.header().client_id, t.num_partitions)
            .then([&response_ref](std::chrono::milliseconds delay) {
                response_ref.data.throttle_time_ms = std::max(
                  response_ref.data.throttle_time_ms, delay);
                return delay == 0ms;
            });
      });

    std::transform(
      quota_exceeded_it,
      valid_range_end,
      std::back_inserter(response.data.topics),
      [version = ctx.header().version](const creatable_topic& t) {
          // For those create topic requests that exceeded a quota,
          // return the appropriate error code, only for clients that
          // made the create topics request at 6 or greater, as older
          // clients will not expect this error code in the response
          return generate_error(
            t,
            ((version >= api_version(6)) ? error_code::throttling_quota_exceeded
                                         : error_code::unknown_server_error),
            "Too many partition mutations requested");
      });
    valid_range_end = quota_exceeded_it;

    auto to_create = to_cluster_type(begin, valid_range_end);

    // Create the topics with controller on core 0
    auto c_res = co_await ctx.topics_frontend().create_topics(
      std::move(to_create), to_timeout(request.data.timeout_ms));
    co_await wait_for_topics(
      ctx.metadata_cache(),
      c_res,
      ctx.controller_api(),
      to_timeout(request.data.timeout_ms));
    // Append controller results to validation errors
    append_cluster_results(c_res, response.data.topics);
    append_topic_properties(ctx, response);
    if (ctx.header().version >= api_version(5)) {
        append_topic_configs(ctx, response);
    }

    co_return co_await ctx.respond(response);
}

} // namespace kafka
