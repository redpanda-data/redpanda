// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/metadata.h"

#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>

#include <fmt/ostream.h>

namespace kafka {

metadata_response::topic
make_topic_response_from_topic_metadata(model::topic_metadata&& tp_md) {
    metadata_response::topic tp;
    tp.error_code = error_code::none;
    tp.name = std::move(tp_md.tp_ns.tp);
    tp.is_internal = false; // no internal topics yet
    std::transform(
      tp_md.partitions.begin(),
      tp_md.partitions.end(),
      std::back_inserter(tp.partitions),
      [](model::partition_metadata& p_md) {
          std::vector<model::node_id> replicas{};
          replicas.reserve(p_md.replicas.size());
          std::transform(
            std::cbegin(p_md.replicas),
            std::cend(p_md.replicas),
            std::back_inserter(replicas),
            [](const model::broker_shard& bs) { return bs.node_id; });
          metadata_response::partition p;
          p.error_code = error_code::none;
          p.partition_index = p_md.id;
          p.leader_id = p_md.leader_node.value_or(model::node_id(-1));
          p.replica_nodes = std::move(replicas);
          p.isr_nodes = p.replica_nodes;
          p.offline_replicas = {};
          return p;
      });
    return tp;
}

static ss::future<metadata_response::topic>
create_topic(request_context& ctx, model::topic&& topic) {
    // default topic configuration
    cluster::topic_configuration cfg{
      model::kafka_namespace,
      topic,
      config::shard_local_cfg().default_topic_partitions(),
      config::shard_local_cfg().default_topic_replication()};
    auto tout = config::shard_local_cfg().create_topic_timeout_ms();
    return ctx.topics_frontend()
      .autocreate_topics({std::move(cfg)}, tout)
      .then([&ctx, &md_cache = ctx.metadata_cache(), tout](
              std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected single result");

          // error, neither success nor topic exists
          if (!(res[0].ec == cluster::errc::success
                || res[0].ec == cluster::errc::topic_already_exists)) {
              metadata_response::topic t;
              t.name = std::move(res[0].tp_ns.tp);
              t.error_code = map_topic_error_code(res[0].ec);
              return ss::make_ready_future<metadata_response::topic>(t);
          }
          auto tp_md = md_cache.get_topic_metadata(res[0].tp_ns);

          if (!tp_md) {
              metadata_response::topic t;
              t.name = std::move(res[0].tp_ns.tp);
              t.error_code = error_code::invalid_topic_exception;
              return ss::make_ready_future<metadata_response::topic>(t);
          }

          return wait_for_topics(
                   res,
                   ctx.controller_api(),
                   tout + model::timeout_clock::now())
            .then([tp_md = std::move(tp_md)]() mutable {
                return make_topic_response_from_topic_metadata(
                  std::move(tp_md.value()));
            });
      })
      .handle_exception([topic = std::move(topic)](
                          [[maybe_unused]] std::exception_ptr e) mutable {
          metadata_response::topic t;
          t.name = std::move(topic);
          t.error_code = error_code::request_timed_out;
          return t;
      });
}

metadata_response::topic
make_error_topic_response(model::topic tp, error_code ec) {
    return metadata_response::topic{.error_code = ec, .name = std::move(tp)};
}

static metadata_response::topic make_topic_response(
  request_context& ctx, metadata_request& rq, model::topic_metadata md) {
    int32_t auth_operations = 0;
    /**
     * if requested include topic authorized operations
     */
    if (rq.data.include_topic_authorized_operations) {
        auth_operations = details::to_bit_field(
          details::authorized_operations(ctx, md.tp_ns.tp));
    }

    auto res = make_topic_response_from_topic_metadata(std::move(md));
    res.topic_authorized_operations = auth_operations;
    return res;
}

static ss::future<std::vector<metadata_response::topic>>
get_topic_metadata(request_context& ctx, metadata_request& request) {
    std::vector<metadata_response::topic> res;

    // request can be served from whatever happens to be in the cache
    if (request.list_all_topics) {
        auto topics = ctx.metadata_cache().all_topics_metadata();
        // only serve topics from the kafka namespace
        std::erase_if(topics, [](model::topic_metadata& t_md) {
            return t_md.tp_ns.ns != model::kafka_namespace;
        });

        auto unauthorized_it = std::partition(
          topics.begin(),
          topics.end(),
          [&ctx](const model::topic_metadata& t_md) {
              return ctx.authorized(
                security::acl_operation::describe, t_md.tp_ns.tp);
          });
        std::transform(
          topics.begin(),
          unauthorized_it,
          std::back_inserter(res),
          [&ctx, &request](model::topic_metadata& t_md) {
              return make_topic_response(ctx, request, std::move(t_md));
          });
        return ss::make_ready_future<std::vector<metadata_response::topic>>(
          std::move(res));
    }

    std::vector<ss::future<metadata_response::topic>> new_topics;

    for (auto& topic : *request.data.topics) {
        /**
         * Authorize source topic in case if we deal with materialized one
         */
        if (!ctx.authorized(security::acl_operation::describe, topic.name)) {
            // not authorized, return authorization error
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::topic_authorization_failed));
            continue;
        }
        if (auto md = ctx.metadata_cache().get_topic_metadata(
              model::topic_namespace_view(model::kafka_namespace, topic.name));
            md) {
            auto src_topic_response = make_topic_response(
              ctx, request, std::move(*md));
            src_topic_response.name = std::move(topic.name);
            res.push_back(std::move(src_topic_response));
            continue;
        }

        if (
          !config::shard_local_cfg().auto_create_topics_enabled
          || !request.data.allow_auto_topic_creation) {
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::unknown_topic_or_partition));
            continue;
        }
        /**
         * check if authorized to create
         */
        if (!ctx.authorized(security::acl_operation::create, topic.name)) {
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::topic_authorization_failed));
            continue;
        }
        new_topics.push_back(create_topic(ctx, std::move(topic.name)));
    }

    return ss::when_all_succeed(new_topics.begin(), new_topics.end())
      .then([res = std::move(res)](
              std::vector<metadata_response::topic> topics) mutable {
          res.insert(res.end(), topics.begin(), topics.end());
          return res;
      });
}

template<>
ss::future<response_ptr> metadata_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    metadata_response reply;
    auto brokers = ctx.metadata_cache().all_brokers();

    for (const auto& broker : brokers) {
        for (const auto& listener : broker->kafka_advertised_listeners()) {
            // filter broker listeners by active connection
            if (listener.name == ctx.listener()) {
                reply.data.brokers.push_back(metadata_response::broker{
                  .node_id = broker->id(),
                  .host = listener.address.host(),
                  .port = listener.address.port(),
                  .rack = broker->rack()});
            }
        }
    }

    reply.data.cluster_id = config::shard_local_cfg().cluster_id;

    auto leader_id = ctx.metadata_cache().get_controller_leader_id();
    reply.data.controller_id = leader_id.value_or(model::node_id(-1));

    metadata_request request;
    request.decode(ctx.reader(), ctx.header().version);

    reply.data.topics = co_await get_topic_metadata(ctx, request);

    if (
      request.data.include_cluster_authorized_operations
      && ctx.authorized(
        security::acl_operation::describe, security::default_cluster_name)) {
        reply.data.cluster_authorized_operations = details::to_bit_field(
          details::authorized_operations(ctx, security::default_cluster_name));
    }

    co_return co_await ctx.respond(std::move(reply));
}

} // namespace kafka
