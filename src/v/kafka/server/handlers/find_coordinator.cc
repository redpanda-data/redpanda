// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/find_coordinator.h"

#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "model/metadata.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

static ss::future<response_ptr>
handle_leader(request_context& ctx, model::node_id leader) {
    auto broker = ctx.metadata_cache().get_broker(leader);
    if (broker) {
        auto& b = *broker;
        for (const auto& listener : b->kafka_advertised_listeners()) {
            if (listener.name == ctx.listener()) {
                return ctx.respond(find_coordinator_response(
                  b->id(), listener.address.host(), listener.address.port()));
            }
        }
    }
    return ctx.respond(
      find_coordinator_response(error_code::coordinator_not_available));
}

/*
 * map the ntp to it's leader broker connection information. also wait for the
 * leader to be elected if it isn't yet available immediately, like in the case
 * of creating the internal metadata topic on-demand.
 */
static ss::future<response_ptr>
handle_ntp(request_context& ctx, std::optional<model::ntp> ntp) {
    if (!ntp) {
        return ctx.respond(
          find_coordinator_response(error_code::coordinator_not_available));
    }

    auto timeout = ss::lowres_clock::now()
                   + config::shard_local_cfg().wait_for_leader_timeout_ms();

    return ctx.metadata_cache()
      .get_leader(*ntp, timeout)
      .then(
        [&ctx](model::node_id leader) { return handle_leader(ctx, leader); })
      .handle_exception([&ctx]([[maybe_unused]] std::exception_ptr e) {
          return ctx.respond(
            find_coordinator_response(error_code::coordinator_not_available));
      });
}

/*
 * create the internal metadata topic for group membership
 */
static ss::future<error_code>
create_topic(request_context& ctx, cluster::topic_configuration topic) {
    return ctx.topics_frontend()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          /*
           * kindly ask client to retry on error
           */
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              return error_code::coordinator_not_available;
          }
          return error_code::none;
      })
      .handle_exception([]([[maybe_unused]] std::exception_ptr e) {
          // various errors may returned such as a timeout, or if the
          // controller group doesn't have a leader. client will retry.
          return error_code::coordinator_not_available;
      });
}

template<>
ss::future<response_ptr> find_coordinator_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    find_coordinator_request request;
    request.decode(ctx.reader(), ctx.header().version);

    // other types include txn coordinators which are unsupported
    if (request.data.key_type != coordinator_type::group) {
        return ctx.respond(
          find_coordinator_response(error_code::unsupported_version));
    }

    return ss::do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          /*
           * map the group to a target ntp. this may fail because the internal
           * metadata topic doesn't exist. in this case fall through and create
           * the topic on-demand.
           */
          if (auto ntp = ctx.coordinator_mapper().ntp_for(
                kafka::group_id(request.data.key));
              ntp) {
              return handle_ntp(ctx, std::move(ntp));
          }

          // the new internal metadata topic for group membership
          cluster::topic_configuration topic{
            ctx.coordinator_mapper().ns(),
            ctx.coordinator_mapper().topic(),
            config::shard_local_cfg().group_topic_partitions(),
            config::shard_local_cfg().default_topic_replication()};

          topic.properties.cleanup_policy_bitflags
            = model::cleanup_policy_bitflags::compaction;

          return create_topic(ctx, std::move(topic))
            .then([&ctx, request = std::move(request)](error_code error) {
                /*
                 * if the topic is successfully created then the metadata cache
                 * will be updated and we can retry the group-ntp mapping.
                 */
                if (error == error_code::none) {
                    auto ntp = ctx.coordinator_mapper().ntp_for(
                      kafka::group_id(request.data.key));
                    return handle_ntp(ctx, std::move(ntp));
                }
                return ctx.respond(find_coordinator_response(error));
            });
      });
}

} // namespace kafka
