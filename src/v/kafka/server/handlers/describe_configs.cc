// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_configs.h"

#include "cluster/metadata_cache.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

template<typename T>
static void add_config(
  describe_configs_result& result,
  const char* name,
  T value,
  describe_configs_source source) {
    result.configs.push_back(describe_configs_resource_result{
      .name = name,
      .value = fmt::format("{}", value),
      .config_source = source,
    });
}

template<>
ss::future<response_ptr> describe_configs_handler::handle(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    describe_configs_response response;
    response.data.results.reserve(request.data.resources.size());

    for (auto& resource : request.data.resources) {
        response.data.results.push_back(describe_configs_result{
          .error_code = error_code::none,
          .resource_type = resource.resource_type,
          .resource_name = resource.resource_name,
        });

        auto& result = response.data.results.back();

        switch (resource.resource_type) {
        case config_resource_type::topic: {
            model::topic_namespace topic(
              model::kafka_namespace, model::topic(resource.resource_name));

            auto err = model::validate_kafka_topic_name(topic.tp);
            if (err) {
                result.error_code = error_code::invalid_topic_exception;
                continue;
            }

            auto topic_config = ctx.metadata_cache().get_topic_cfg(topic);
            if (!topic_config) {
                result.error_code = error_code::unknown_topic_or_partition;
                continue;
            }

            add_config(
              result,
              "partition_count",
              topic_config->partition_count,
              describe_configs_source::topic);

            add_config(
              result,
              "replication_factor",
              topic_config->replication_factor,
              describe_configs_source::topic);

            add_config(
              result,
              "cleanup.policy",
              describe_topic_cleanup_policy(topic_config),
              describe_configs_source::topic);

            break;
        }

        // resource types not yet handled
        case config_resource_type::broker:
            [[fallthrough]];
        case config_resource_type::broker_logger:
            result.error_code = error_code::invalid_request;
        }
    }

    return ctx.respond(std::move(response));
}

} // namespace kafka
