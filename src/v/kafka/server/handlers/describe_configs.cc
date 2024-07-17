// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_configs.h"

#include "base/type_traits.h"
#include "cluster/metadata_cache.h"
#include "cluster/types.h"
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

static void report_topic_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  bool include_synonyms,
  bool include_documentation) {
    auto res = make_topic_configs(
      metadata_cache,
      topic_properties,
      resource.configuration_keys,
      include_synonyms,
      include_documentation);

    result.configs.reserve(res.size());
    for (auto& conf : res) {
        result.configs.push_back(conf.to_describe_config());
    }
}

static void report_broker_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  bool include_synonyms,
  bool include_documentation) {
    if (!result.resource_name.empty()) {
        int32_t broker_id = -1;
        auto res = std::from_chars(
          result.resource_name.data(),
          result.resource_name.data() + result.resource_name.size(), // NOLINT
          broker_id);
        if (res.ec == std::errc()) {
            if (broker_id != *config::node().node_id()) {
                result.error_code = error_code::invalid_request;
                result.error_message = ssx::sformat(
                  "Unexpected broker id {} expected {}",
                  broker_id,
                  *config::node().node_id());
                return;
            }
        } else {
            result.error_code = error_code::invalid_request;
            result.error_message = ssx::sformat(
              "Broker id must be an integer but received {}",
              result.resource_name);
            return;
        }
    }

    auto res = make_broker_configs(
      resource.configuration_keys, include_synonyms, include_documentation);

    result.configs.reserve(res.size());
    for (auto& conf : res) {
        result.configs.push_back(conf.to_describe_config());
    }
}

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

    // If unable to audit, remove any trace of the response data
    if (!ctx.audit()) {
        describe_configs_response failure_resp;
        failure_resp.data.results.reserve(response.data.results.size());
        std::transform(
          response.data.results.begin(),
          response.data.results.end(),
          std::back_inserter(failure_resp.data.results),
          [](const describe_configs_result& r) {
              return describe_configs_result{
                .error_code = error_code::broker_not_available,
                .error_message = "Broker not available - audit system failure",
                .resource_type = r.resource_type,
                .resource_name = r.resource_name,
              };
          });

        return ctx.respond(std::move(failure_resp));
    }

    return ctx.respond(std::move(response));
}

} // namespace kafka
