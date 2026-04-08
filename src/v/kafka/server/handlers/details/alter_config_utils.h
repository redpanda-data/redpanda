/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "container/chunked_vector.h"
#include "kafka/server/request_context.h"

namespace kafka {
template<typename T>
struct groupped_resources {
    chunked_vector<T> topic_changes;
    chunked_vector<T> broker_changes;
};

template<typename T>
groupped_resources<T> group_alter_config_resources(chunked_vector<T> req) {
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

template<typename R, typename T>
std::vector<chunked_vector<R>> make_audit_failure_response(
  groupped_resources<T>&& resources, chunked_vector<R> unauthorized_responses) {
    chunked_vector<R> responses;

    auto gen_resp = [](const T& res) {
        return make_error_alter_config_resource_response<R>(
          res,
          error_code::broker_not_available,
          "Broker not available - audit system failure");
    };

    responses.reserve(
      resources.broker_changes.size() + resources.topic_changes.size()
      + unauthorized_responses.size());

    std::transform(
      resources.broker_changes.begin(),
      resources.broker_changes.end(),
      std::back_inserter(responses),
      gen_resp);

    std::transform(
      resources.topic_changes.begin(),
      resources.topic_changes.end(),
      std::back_inserter(responses),
      gen_resp);

    std::for_each(
      unauthorized_responses.begin(), unauthorized_responses.end(), [](R& r) {
          r.error_code = error_code::broker_not_available;
          r.error_message = "Broker not available - audit system failure";
      });

    std::move(
      unauthorized_responses.begin(),
      unauthorized_responses.end(),
      std::back_inserter(responses));

    std::vector<chunked_vector<R>> res;
    res.push_back(std::move(responses));
    return res;
}
/**
 * Authorizes groupped alter configuration resources, it returns not authorized
 * responsens and modifies passed in group_resources<T>
 */
template<typename T, typename R>
chunked_vector<R> authorize_alter_config_resources(
  request_context& ctx, groupped_resources<T>& to_authorize) {
    chunked_vector<R> not_authorized;
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
            res,
            error_code::topic_authorization_failed,
            "Not authorized to alter_configs or topic is protected by "
            "'kafka_nodelete_topics' or "
            "'kafka_noproduce_topics'");
      });

    to_authorize.topic_changes.erase_to_end(unauthorized_it);

    return not_authorized;
}

template<typename T, typename R, typename Func>
ss::future<chunked_vector<R>> do_alter_topics_configuration(
  request_context& ctx,
  chunked_vector<T> resources,
  bool validate_only,
  Func f) {
    chunked_vector<R> responses;
    responses.reserve(resources.size());

    absl::node_hash_set<ss::sstring> topic_names;
    auto valid_end = std::stable_partition(
      resources.begin(), resources.end(), [&topic_names](T& r) {
          return !topic_names.contains(r.resource_name);
      });

    for (auto& r : boost::make_iterator_range(valid_end, resources.end())) {
        responses.push_back(
          make_error_alter_config_resource_response<R>(
            r,
            error_code::invalid_config,
            "duplicated topic {} alter config request"));
    }

    // Constructing a map of requested changes for logging
    chunked_hash_map<model::topic, chunked_vector<ss::sstring>>
      requested_kv_map;
    requested_kv_map.reserve(std::distance(resources.begin(), valid_end));
    for (auto& r : boost::make_iterator_range(resources.begin(), valid_end)) {
        chunked_vector<ss::sstring> kvs;
        kvs.reserve(r.configs.size());
        for (auto& c : r.configs) {
            const auto& key = c.name;
            kvs.emplace_back(
              ssx::sformat(
                "{}={}", key, c.value.value_or(ss::sstring("<null>"))));
        }
        requested_kv_map.emplace(model::topic(r.resource_name), std::move(kvs));
    }

    cluster::topic_properties_update_vector updates;
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
            responses.push_back(
              R{
                .error_code = error_code::none,
                .resource_type = static_cast<int8_t>(
                  config_resource_type::topic),
                .resource_name = u.tp_ns.tp,
              });
        }

        co_return responses;
    }

    auto update_results
      = co_await ctx.topics_frontend().update_topic_properties(
        std::move(updates),
        model::timeout_clock::now()
          + config::shard_local_cfg().internal_rpc_request_timeout_ms());
    for (auto& res : update_results) {
        responses.push_back(
          R{
            .error_code = map_topic_error_code(res.ec),
            .error_message = res.error_message.value_or(
              make_error_code(res.ec).message()),
            .resource_type = static_cast<int8_t>(config_resource_type::topic),
            .resource_name = res.tp_ns.tp(),
          });
    }

    // Group-by error code, value is list of topic names as strings
    absl::flat_hash_map<cluster::errc, std::vector<model::topic_namespace_view>>
      err_map;
    for (const auto& result : update_results) {
        auto [itr, _] = err_map.try_emplace(
          result.ec, std::vector<model::topic_namespace_view>());
        itr->second.emplace_back(result.tp_ns);
    }

    // Log success case
    auto found = err_map.find(cluster::errc::success);
    if (found != err_map.end()) {
        for (const auto& tpv : found->second) {
            const auto& topic = tpv.tp;
            if (
              auto it = requested_kv_map.find(topic);
              it != requested_kv_map.end()) {
                vlog(
                  klog.info,
                  "Successfully altered topic properties for {}: [{}]",
                  topic,
                  fmt::join(it->second, ", "));
            } else {
                vlog(
                  klog.info,
                  "Successfully altered topic properties for {}",
                  topic);
            }
        }
        err_map.erase(found);
    }

    // Log topics that had not successfully been created at warn level
    for (const auto& err : err_map) {
        vlog(
          klog.warn,
          "Failed to alter topic properties of topic(s) {{{}}} error_code "
          "observed: {}",
          fmt::join(err.second, ", "),
          err.first);
    }

    co_return responses;
}
} // namespace kafka
