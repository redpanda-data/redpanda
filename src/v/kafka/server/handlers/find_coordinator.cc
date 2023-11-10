// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/find_coordinator.h"

#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/metadata.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

static ss::future<response_ptr>
handle_leader(request_context& ctx, model::node_id leader) {
    auto broker = ctx.metadata_cache().get_node_metadata(leader);
    if (broker) {
        auto& b = *broker;
        for (const auto& listener : b.broker.kafka_advertised_listeners()) {
            if (listener.name == ctx.listener()) {
                return ctx.respond(find_coordinator_response(
                  b.broker.id(),
                  listener.address.host(),
                  listener.address.port()));
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
      .handle_exception([&ctx](const std::exception_ptr&) {
          return ctx.respond(
            find_coordinator_response(error_code::coordinator_not_available));
      });
}

template<>
ss::future<response_ptr> find_coordinator_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    find_coordinator_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (request.data.key_type == coordinator_type::transaction) {
        if (!ctx.are_transactions_enabled()) {
            return ctx.respond(
              find_coordinator_response(error_code::unsupported_version));
        }

        transactional_id tx_id(request.data.key);

        auto authz = ctx.authorized(security::acl_operation::describe, tx_id);

        if (!ctx.audit()) {
            return ctx.respond(find_coordinator_response(
              error_code::broker_not_available,
              "Broker not available - audit system failure"));
        }

        if (!authz) {
            return ctx.respond(find_coordinator_response(
              error_code::transactional_id_authorization_failed));
        }

        return ss::do_with(
          std::move(ctx),
          [request = std::move(request),
           tx_id = std::move(tx_id)](request_context& ctx) mutable {
              return ctx.tx_gateway_frontend().find_coordinator(tx_id).then(
                [&ctx](cluster::find_coordinator_reply r) {
                    if (r.coordinator) {
                        return handle_leader(ctx, *r.coordinator);
                    }
                    return ctx.respond(find_coordinator_response(
                      error_code::coordinator_not_available));
                });
          });
    }

    if (request.data.key_type != coordinator_type::group) {
        return ctx.respond(
          find_coordinator_response(error_code::unsupported_version));
    }

    auto authz = ctx.authorized(
      security::acl_operation::describe, group_id(request.data.key));

    if (!ctx.audit()) {
        return ctx.respond(find_coordinator_response(
          error_code::broker_not_available,
          "Broker not available - audit system failure"));
    }

    if (!authz) {
        return ctx.respond(
          find_coordinator_response(error_code::group_authorization_failed));
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

          return try_create_consumer_group_topic(
                   ctx.coordinator_mapper(),
                   ctx.topics_frontend(),
                   (int16_t)ctx.metadata_cache().node_count())
            .then([&ctx, request = std::move(request)](bool created) {
                /*
                 * if the topic is successfully created then the metadata cache
                 * will be updated and we can retry the group-ntp mapping.
                 */
                if (created) {
                    auto ntp = ctx.coordinator_mapper().ntp_for(
                      kafka::group_id(request.data.key));
                    return handle_ntp(ctx, std::move(ntp));
                }
                return ctx.respond(find_coordinator_response(
                  error_code::coordinator_not_available));
            });
      });
}

} // namespace kafka
