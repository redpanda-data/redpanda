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
#include "config/configuration.h"
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

#include <boost/algorithm/string/join.hpp>
#include <fmt/ostream.h>

#include <charconv>
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

static ss::sstring
kafka_endpoint_format(const std::vector<model::broker_endpoint>& endpoints) {
    std::vector<ss::sstring> uris;
    uris.reserve(endpoints.size());
    std::transform(
      endpoints.cbegin(),
      endpoints.cend(),
      std::back_inserter(uris),
      [](const model::broker_endpoint& ep) {
          return fmt::format(
            "{}://{}:{}",
            (ep.name.empty() ? "plain" : ep.name),
            ep.address.host(),
            ep.address.port());
      });
    return boost::algorithm::join(uris, ",");
}

static void report_broker_config(describe_configs_result& result) {
    if (!result.resource_name.empty()) {
        int32_t broker_id = -1;
        auto res = std::from_chars(
          result.resource_name.data(),
          result.resource_name.data() + result.resource_name.size(), // NOLINT
          broker_id);
        if (res.ec == std::errc()) {
            if (broker_id != config::shard_local_cfg().node_id()) {
                result.error_code = error_code::invalid_request;
                result.error_message = fmt::format(
                  "Unexpected broker id {} expected {}",
                  broker_id,
                  config::shard_local_cfg().node_id());
                return;
            }
        } else {
            result.error_code = error_code::invalid_request;
            result.error_message = fmt::format(
              "Broker id must be an integer but received {}",
              result.resource_name);
            return;
        }
    }

    add_config(
      result,
      "listeners",
      kafka_endpoint_format(config::shard_local_cfg().kafka_api()),
      describe_configs_source::static_broker_config);

    add_config(
      result,
      "advertised.listeners",
      kafka_endpoint_format(config::shard_local_cfg().advertised_kafka_api()),
      describe_configs_source::static_broker_config);

    add_config(
      result,
      "log.segment.bytes",
      config::shard_local_cfg().log_segment_size(),
      describe_configs_source::static_broker_config);

    add_config(
      result,
      "log.retention.bytes",
      config::shard_local_cfg().retention_bytes(),
      describe_configs_source::static_broker_config);

    add_config(
      result,
      "log.retention.ms",
      config::shard_local_cfg().delete_retention_ms(),
      describe_configs_source::static_broker_config);
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

        case config_resource_type::broker:
            report_broker_config(result);
            break;

        // resource types not yet handled
        case config_resource_type::broker_logger:
            result.error_code = error_code::invalid_request;
        }
    }

    return ctx.respond(std::move(response));
}

} // namespace kafka
