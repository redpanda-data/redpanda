// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/create_topics.h"

#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/timeout.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/types.h"
#include "model/metadata.h"
#include "security/acl.h"
#include "utils/to_string.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

static constexpr std::array<std::string_view, 8> supported_configs{
  {"compression.type",
   "cleanup.policy",
   "message.timestamp.type",
   "segment.bytes",
   "compaction.strategy",
   "retention.bytes",
   "retention.ms",
   "x-redpanda-recovery"}};

bool is_supported(std::string_view name) {
    return std::any_of(
      supported_configs.begin(),
      supported_configs.end(),
      [name](std::string_view p) { return name == p; });
}

using validators = make_validator_types<
  creatable_topic,
  no_custom_partition_assignment,
  partition_count_must_be_positive,
  replication_factor_must_be_positive,
  replication_factor_must_be_odd>;

template<>
ss::future<response_ptr> create_topics_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    kafka::create_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(
      klog.debug, "Handling {} request: {}", create_topics_api::name, request);
    return ss::do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          create_topics_response response;
          auto begin = request.data.topics.begin();
          // Duplicated topic names are not accepted
          auto valid_range_end = validate_range_duplicates(
            begin,
            request.data.topics.end(),
            std::back_inserter(response.data.topics));

          const auto has_cluster_auth = ctx.authorized(
            security::acl_operation::create, security::default_cluster_name);

          if (!has_cluster_auth) {
              auto unauthorized_it = std::partition(
                begin, valid_range_end, [&ctx](const creatable_topic& t) {
                    return ctx.authorized(
                      security::acl_operation::create, t.name);
                });
              std::transform(
                unauthorized_it,
                valid_range_end,
                std::back_inserter(response.data.topics),
                [](const creatable_topic& t) {
                    return generate_error(
                      t,
                      error_code::topic_authorization_failed,
                      "Unauthorized");
                });
              valid_range_end = unauthorized_it;
          }

          // fill in defaults if necessary
          for (auto& r : boost::make_iterator_range(begin, valid_range_end)) {
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
                [](const creatable_topic& t) {
                    return generate_successfull_result(t);
                });
              return ctx.respond(std::move(response));
          }
          auto to_create = to_cluster_type(begin, valid_range_end);
          /**
           * We always override cleanup policy. i.e. topic cleanup policy will
           * stay the same even if it was changed in defaults (broker
           * configuration) and there was no override passed by client while
           * creating a topic. The the same policy is applied in Kafka.
           */
          for (auto& tp : to_create) {
              if (!tp.properties.cleanup_policy_bitflags.has_value()) {
                  tp.properties.cleanup_policy_bitflags
                    = ctx.metadata_cache()
                        .get_default_cleanup_policy_bitflags();
              }
          }
          // Create the topics with controller on core 0
          return ctx.topics_frontend()
            .create_topics(
              std::move(to_create), to_timeout(request.data.timeout_ms))
            .then([&ctx, tout = to_timeout(request.data.timeout_ms)](
                    std::vector<cluster::topic_result> c_res) mutable {
                return wait_for_topics(c_res, ctx.controller_api(), tout)
                  .then([c_res = std::move(c_res)]() mutable { return c_res; });
            })
            .then([&ctx,
                   response = std::move(response),
                   tout = to_timeout(request.data.timeout_ms)](
                    std::vector<cluster::topic_result> c_res) mutable {
                // Append controller results to validation errors
                append_cluster_results(c_res, response.data.topics);
                return ctx.respond(response);
            });
      });
}

} // namespace kafka
