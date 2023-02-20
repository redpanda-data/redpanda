
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

#pragma once

#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fwd.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/types.h"
#include "outcome.h"
#include "security/acl.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/node_hash_set.h>

#include <optional>

namespace kafka {
template<typename T>
struct groupped_resources {
    std::vector<T> topic_changes;
    std::vector<T> broker_changes;
};

template<typename T>
groupped_resources<T> group_alter_config_resources(std::vector<T> req) {
    groupped_resources<T> ret;
    for (auto& res : req) {
        switch (config_resource_type(res.resource_type)) {
        case config_resource_type::topic:
            ret.topic_changes.push_back(std::move(res));
            break;
        default:
            ret.broker_changes.push_back(std::move(res));
        };
    }
    return ret;
}

template<typename T, typename R>
T assemble_alter_config_response(std::vector<std::vector<R>> responses) {
    T response;
    for (auto& v : responses) {
        std::move(
          v.begin(), v.end(), std::back_inserter(response.data.responses));
    }

    return response;
}
template<typename T, typename R>
T make_error_alter_config_resource_response(
  const R& resource, error_code err, std::optional<ss::sstring> msg = {}) {
    return T{
      .error_code = err,
      .error_message = std::move(msg),
      .resource_type = resource.resource_type,
      .resource_name = resource.resource_name};
}
/**
 * Authorizes groupped alter configuration resources, it returns not authorized
 * responsens and modifies passed in group_resources<T>
 */
template<typename T, typename R>
std::vector<R> authorize_alter_config_resources(
  request_context& ctx, groupped_resources<T>& to_authorize) {
    std::vector<R> not_authorized;
    /**
     * Check broker configuration authorization
     */
    if (
      !to_authorize.broker_changes.empty()
      && !ctx.authorized(
        security::acl_operation::alter_configs,
        security::default_cluster_name)) {
        // not allowed
        std::transform(
          to_authorize.broker_changes.begin(),
          to_authorize.broker_changes.end(),
          std::back_inserter(not_authorized),
          [](T& res) {
              return make_error_alter_config_resource_response<R>(
                res, error_code::cluster_authorization_failed);
          });
        // all broker changes have to be dropped
        to_authorize.broker_changes.clear();
    }

    const auto& kafka_nodelete_topics
      = config::shard_local_cfg().kafka_nodelete_topics();
    const auto& kafka_noproduce_topics
      = config::shard_local_cfg().kafka_noproduce_topics();

    /**
     * Check topic configuration authorization
     */
    auto unauthorized_it = std::partition(
      to_authorize.topic_changes.begin(),
      to_authorize.topic_changes.end(),
      [&ctx, &kafka_nodelete_topics, &kafka_noproduce_topics](const T& res) {
          auto topic = model::topic(res.resource_name);

          auto is_nodelete_topic = std::find(
                                     kafka_nodelete_topics.cbegin(),
                                     kafka_nodelete_topics.cend(),
                                     topic)
                                   != kafka_nodelete_topics.cend();
          if (is_nodelete_topic) {
              return false;
          }

          auto is_noproduce_topic = std::find(
                                      kafka_noproduce_topics.cbegin(),
                                      kafka_noproduce_topics.cend(),
                                      topic)
                                    != kafka_noproduce_topics.cend();
          if (is_noproduce_topic) {
              return false;
          }

          return ctx.authorized(security::acl_operation::alter_configs, topic);
      });

    std::transform(
      unauthorized_it,
      to_authorize.topic_changes.end(),
      std::back_inserter(not_authorized),
      [](T& res) {
          return make_error_alter_config_resource_response<R>(
            res, error_code::topic_authorization_failed);
      });

    to_authorize.topic_changes.erase(
      unauthorized_it, to_authorize.topic_changes.end());

    return not_authorized;
}

template<typename T, typename R, typename Func>
ss::future<std::vector<R>> do_alter_topics_configuration(
  request_context& ctx, std::vector<T> resources, bool validate_only, Func f) {
    std::vector<R> responses;
    responses.reserve(resources.size());

    absl::node_hash_set<ss::sstring> topic_names;
    auto valid_end = std::stable_partition(
      resources.begin(), resources.end(), [&topic_names](T& r) {
          return !topic_names.contains(r.resource_name);
      });

    for (auto& r : boost::make_iterator_range(valid_end, resources.end())) {
        responses.push_back(make_error_alter_config_resource_response<R>(
          r,
          error_code::invalid_config,
          "duplicated topic {} alter config request"));
    }
    std::vector<cluster::topic_properties_update> updates;
    for (auto& r : boost::make_iterator_range(resources.begin(), valid_end)) {
        auto res = f(r);
        if (res.has_error()) {
            responses.push_back(std::move(res.error()));
        } else {
            updates.push_back(std::move(res.value()));
        }
    }

    if (validate_only) {
        // all pending updates are valid, just generate responses
        for (auto& u : updates) {
            responses.push_back(R{
              .error_code = error_code::none,
              .resource_type = static_cast<int8_t>(config_resource_type::topic),
              .resource_name = u.tp_ns.tp,
            });
        }

        co_return responses;
    }

    auto update_results
      = co_await ctx.topics_frontend().update_topic_properties(
        std::move(updates),
        model::timeout_clock::now()
          + config::shard_local_cfg().alter_topic_cfg_timeout_ms());
    for (auto& res : update_results) {
        responses.push_back(R{
          .error_code = map_topic_error_code(res.ec),
          .resource_type = static_cast<int8_t>(config_resource_type::topic),
          .resource_name = res.tp_ns.tp(),
        });
    }
    co_return responses;
}

template<typename T, typename R>
ss::future<std::vector<R>> unsupported_broker_configuration(
  std::vector<T> resources, std::string_view const msg) {
    std::vector<R> responses;
    responses.reserve(resources.size());
    std::transform(
      resources.begin(),
      resources.end(),
      std::back_inserter(responses),
      [msg](T& resource) {
          return make_error_alter_config_resource_response<R>(
            resource, error_code::invalid_config, ss::sstring(msg));
      });

    return ss::make_ready_future<std::vector<R>>(std::move(responses));
}

class validation_error final : std::exception {
public:
    explicit validation_error(ss::sstring what)
      : _what(std::move(what)) {}

    const char* what() const noexcept final { return _what.c_str(); }

private:
    ss::sstring _what;
};

template<typename T>
struct noop_validator {
    std::optional<ss::sstring> operator()(const T&) { return std::nullopt; }
};

struct segment_size_validator {
    std::optional<ss::sstring> operator()(const size_t& value) {
        // use reasonable defaults even if they are not set in configuration
        size_t min
          = config::shard_local_cfg().log_segment_size_min.value().value_or(1);
        size_t max
          = config::shard_local_cfg().log_segment_size_max.value().value_or(
            std::numeric_limits<size_t>::max());

        if (value < min || value > max) {
            return fmt::format(
              "segment size {} is outside of allowed range [{}, {}]",
              value,
              min,
              max);
        }
        return std::nullopt;
    }
};

template<typename T, typename Validator = noop_validator<T>>
requires requires(const T& value, const ss::sstring& str, Validator validator) {
    { boost::lexical_cast<T>(str) } -> std::convertible_to<T>;
    { validator(value) } -> std::convertible_to<std::optional<ss::sstring>>;
}
void parse_and_set_optional(
  cluster::property_update<std::optional<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  Validator validator = noop_validator<T>{}) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value if preset, otherwise do nothing
    if (op == config_resource_operation::set && value) {
        auto v = boost::lexical_cast<T>(*value);
        auto v_error = validator(v);
        if (v_error) {
            throw validation_error(*v_error);
        }
        property.value = std::move(v);
        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

static void parse_and_set_bool(
  cluster::property_update<bool>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op,
  bool default_value) {
    // A remove on a concrete (non-nullable) property is a reset to default,
    // as is an assignment to nullopt.
    if (
      op == config_resource_operation::remove
      || (op == config_resource_operation::set && !value)) {
        property.op = cluster::incremental_update_operation::set;
        property.value = default_value;
        return;
    }

    if (op == config_resource_operation::set && value) {
        try {
            property.value = string_switch<bool>(*value)
                               .match("true", true)
                               .match("false", false);
        } catch (std::runtime_error) {
            // Our callers expect this exception type on malformed values
            throw boost::bad_lexical_cast();
        }
        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

template<typename T>
void parse_and_set_tristate(
  cluster::property_update<tristate<T>>& property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // remove property value
    if (op == config_resource_operation::remove) {
        property.op = cluster::incremental_update_operation::remove;
        return;
    }
    // set property value
    if (op == config_resource_operation::set) {
        auto parsed = boost::lexical_cast<int64_t>(*value);
        if (parsed <= 0) {
            property.value = tristate<T>{};
        } else {
            property.value = tristate<T>(std::make_optional<T>(parsed));
        }

        property.op = cluster::incremental_update_operation::set;
        return;
    }
}

static void parse_and_set_topic_replication_factor(
  cluster::property_update<std::optional<cluster::replication_factor>>&
    property,
  const std::optional<ss::sstring>& value,
  config_resource_operation op) {
    // set property value
    if (op == config_resource_operation::set) {
        property.value = std::nullopt;
        if (value) {
            property.value = cluster::parsing_replication_factor(*value);
        }
        property.op = cluster::incremental_update_operation::set;
    }
    return;
}

} // namespace kafka
