// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_configs.h"

#include "cluster/metadata_cache.h"
#include "config/configuration.h"
#include "config/data_directory_path.h"
#include "config/node_config.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "reflection/type_traits.h"
#include "security/acl.h"
#include "ssx/sformat.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ranges.h>

#include <charconv>
#include <string_view>
#include <type_traits>

namespace kafka {

template<>
ss::future<response_ptr> describe_configs_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_configs_response response;
    response.data.results.reserve(request.data.resources.size());
    bool cluster_authorized = ctx.authorized(
      security::acl_operation::describe_configs,
      security::default_cluster_name);

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

            if (!ctx.authorized(
                  security::acl_operation::describe_configs, topic.tp)) {
                result.error_code = error_code::topic_authorization_failed;
                continue;
            }

            report_topic_config(
              resource,
              result,
              ctx.metadata_cache(),
              topic_config->properties,
              request.data.include_synonyms,
              request.data.include_documentation);
            break;
        }

        case config_resource_type::broker:
            if (!cluster_authorized) {
                result.error_code = error_code::cluster_authorization_failed;
                continue;
            }
            report_broker_config(
              resource,
              result,
              request.data.include_synonyms,
              request.data.include_documentation);
            break;

        // resource types not yet handled
        case config_resource_type::broker_logger:
            result.error_code = error_code::invalid_request;
        }
    }

    return ctx.respond(std::move(response));
}

} // namespace kafka
