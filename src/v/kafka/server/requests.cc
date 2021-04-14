// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/handlers.h"
#include "kafka/server/request_context.h"
#include "kafka/types.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka {

/**
 * Dispatch request with version bounds checking.
 */
template<typename Request>
CONCEPT(requires(KafkaApiHandler<Request>))
struct process_dispatch {
    static ss::future<response_ptr>
    process(request_context&& ctx, ss::smp_service_group g) {
        if (
          ctx.header().version < Request::min_supported
          || ctx.header().version > Request::max_supported) {
            return ss::make_exception_future<response_ptr>(
              std::runtime_error(fmt::format(
                "Unsupported version {} for {} API",
                ctx.header().version,
                Request::api::name)));
        }
        return Request::handle(std::move(ctx), g);
    }
};

/**
 * Dispatch API versions request without version checks.
 *
 * The version bounds checks are not applied to this request because the client
 * does not yet know what versions this server supports. The api versions
 * request is used by a client to query this information.
 */
template<>
struct process_dispatch<api_versions_handler> {
    static ss::future<response_ptr>
    process(request_context&& ctx, ss::smp_service_group g) {
        return api_versions_handler::handle(std::move(ctx), g);
    }
};

template<typename Request>
CONCEPT(requires(KafkaApiHandler<Request>))
ss::future<response_ptr> do_process(
  request_context&& ctx, ss::smp_service_group g) {
    vlog(
      klog.trace,
      "Processing name:{}, key:{}, version:{} for {}",
      Request::api::name,
      ctx.header().key,
      ctx.header().version,
      ctx.header().client_id.value_or(std::string_view("unset-client-id")));

    return process_dispatch<Request>::process(std::move(ctx), g);
}

/*
 * process a handshake request. if it doesn't result in a sasl mechanism being
 * selected then client negotiation failed. otherwise, move to authentication.
 */
static ss::future<response_ptr>
handle_auth_handshake(request_context&& ctx, ss::smp_service_group g) {
    auto conn = ctx.connection();
    return do_process<sasl_handshake_handler>(std::move(ctx), g)
      .then([conn = std::move(conn)](response_ptr r) {
          if (conn->sasl().has_mechanism()) {
              conn->sasl().set_state(
                security::sasl_server::sasl_state::authenticate);
          } else {
              conn->sasl().set_state(security::sasl_server::sasl_state::failed);
          }
          return r;
      });
}

/*
 * in the initial state the protocol accepts either api version or sasl
 * handshake requests. any other requests are a protocol violation.
 */
static ss::future<response_ptr>
handle_auth_initial(request_context&& ctx, ss::smp_service_group g) {
    switch (ctx.header().key) {
    case api_versions_handler::api::key: {
        auto r = api_versions_handler::handle_raw(ctx);
        if (r.data.error_code == error_code::none) {
            ctx.sasl().set_state(security::sasl_server::sasl_state::handshake);
        }
        return ctx.respond(std::move(r));
    }

    case sasl_handshake_handler::api::key: {
        return handle_auth_handshake(std::move(ctx), g);
    }

    default:
        return ss::make_exception_future<response_ptr>(
          std::runtime_error(fmt::format(
            "Unexpected request during authentication: {}", ctx.header().key)));
    }
}

static ss::future<response_ptr>
handle_auth(request_context&& ctx, ss::smp_service_group g) {
    switch (ctx.sasl().state()) {
    case security::sasl_server::sasl_state::initial:
        return handle_auth_initial(std::move(ctx), g);

    case security::sasl_server::sasl_state::handshake:
        if (unlikely(ctx.header().key != sasl_handshake_handler::api::key)) {
            return ss::make_exception_future<response_ptr>(
              std::runtime_error(fmt::format(
                "Unexpected auth request {} expected handshake",
                ctx.header().key)));
        }
        return handle_auth_handshake(std::move(ctx), g);

    case security::sasl_server::sasl_state::authenticate: {
        if (unlikely(ctx.header().key != sasl_authenticate_handler::api::key)) {
            return ss::make_exception_future<response_ptr>(
              std::runtime_error(fmt::format(
                "Unexpected auth request {} expected authenticate",
                ctx.header().key)));
        }
        auto conn = ctx.connection();
        return do_process<sasl_authenticate_handler>(std::move(ctx), g)
          .then([conn = std::move(conn)](response_ptr r) {
              /*
               * there may be multiple authentication round-trips so it is fine
               * to return without entering an end state like complete/failed.
               */
              if (conn->sasl().mechanism().complete()) {
                  conn->sasl().set_state(
                    security::sasl_server::sasl_state::complete);
              } else if (conn->sasl().mechanism().failed()) {
                  conn->sasl().set_state(
                    security::sasl_server::sasl_state::failed);
              }
              return ss::make_ready_future<response_ptr>(std::move(r));
          });
    }

    /*
     * TODO: we should shut down the connection when authentication failed for
     * simplicity. however, to do this we need to build a few mechanisms that
     * let us build and send a response, and then close the connection. at the
     * moment it is either send a response or close the connection.
     */
    case security::sasl_server::sasl_state::failed:
        return ss::make_exception_future<response_ptr>(std::runtime_error(
          "Authentication failed. Shutting down connection"));

    default:
        return ss::make_exception_future<response_ptr>(
          std::runtime_error(fmt::format(
            "Unexpected request during authentication: {}", ctx.header().key)));
    }
}

ss::future<response_ptr>
process_request(request_context&& ctx, ss::smp_service_group g) {
    /*
     * requests are handled as normal when auth is disabled. otherwise no
     * request is handled until the auth process has completed.
     */
    if (unlikely(!ctx.sasl().complete())) {
        auto conn = ctx.connection();
        return handle_auth(std::move(ctx), g)
          .then_wrapped([conn](ss::future<response_ptr> f) {
              if (f.failed()) {
                  conn->sasl().set_state(
                    security::sasl_server::sasl_state::failed);
              }
              return f;
          });
    }

    switch (ctx.header().key) {
    case api_versions_handler::api::key:
        return do_process<api_versions_handler>(std::move(ctx), g);
    case metadata_handler::api::key:
        return do_process<metadata_handler>(std::move(ctx), g);
    case list_groups_handler::api::key:
        return do_process<list_groups_handler>(std::move(ctx), g);
    case find_coordinator_handler::api::key:
        return do_process<find_coordinator_handler>(std::move(ctx), g);
    case offset_fetch_handler::api::key:
        return do_process<offset_fetch_handler>(std::move(ctx), g);
    case produce_handler::api::key:
        return do_process<produce_handler>(std::move(ctx), g);
    case list_offsets_handler::api::key:
        return do_process<list_offsets_handler>(std::move(ctx), g);
    case offset_commit_handler::api::key:
        return do_process<offset_commit_handler>(std::move(ctx), g);
    case fetch_handler::api::key:
        return do_process<fetch_handler>(std::move(ctx), g);
    case join_group_handler::api::key:
        return do_process<join_group_handler>(std::move(ctx), g);
    case heartbeat_handler::api::key:
        return do_process<heartbeat_handler>(std::move(ctx), g);
    case leave_group_handler::api::key:
        return do_process<leave_group_handler>(std::move(ctx), g);
    case sync_group_handler::api::key:
        return do_process<sync_group_handler>(std::move(ctx), g);
    case create_topics_handler::api::key:
        return do_process<create_topics_handler>(std::move(ctx), g);
    case describe_configs_handler::api::key:
        return do_process<describe_configs_handler>(std::move(ctx), g);
    case alter_configs_handler::api::key:
        return do_process<alter_configs_handler>(std::move(ctx), g);
    case delete_topics_handler::api::key:
        return do_process<delete_topics_handler>(std::move(ctx), g);
    case describe_groups_handler::api::key:
        return do_process<describe_groups_handler>(std::move(ctx), g);
    case sasl_handshake_handler::api::key:
        return ctx.respond(
          sasl_handshake_response(error_code::illegal_sasl_state, {}));
    case sasl_authenticate_handler::api::key: {
        sasl_authenticate_response_data data{
          .error_code = error_code::illegal_sasl_state,
          .error_message = "Authentication process already completed",
        };
        return ctx.respond(sasl_authenticate_response(std::move(data)));
    }
    case init_producer_id_handler::api::key:
        return do_process<init_producer_id_handler>(std::move(ctx), g);
    case incremental_alter_configs_handler::api::key:
        return do_process<incremental_alter_configs_handler>(std::move(ctx), g);
    case delete_groups_handler::api::key:
        return do_process<delete_groups_handler>(std::move(ctx), g);
    case describe_acls_handler::api::key:
        return do_process<describe_acls_handler>(std::move(ctx), g);
    case describe_log_dirs_handler::api::key:
        return do_process<describe_log_dirs_handler>(std::move(ctx), g);
    case create_acls_handler::api::key:
        return do_process<create_acls_handler>(std::move(ctx), g);
    case delete_acls_handler::api::key:
        return do_process<delete_acls_handler>(std::move(ctx), g);
    case txn_offset_commit_handler::api::key:
        return do_process<txn_offset_commit_handler>(std::move(ctx), g);
    };
    return ss::make_exception_future<response_ptr>(
      std::runtime_error(fmt::format("Unsupported API {}", ctx.header().key)));
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    fmt::print(
      os,
      "{{key:{}, version:{}, correlation_id:{}, client_id:{}}}",
      header.key,
      header.version,
      header.correlation,
      header.client_id.value_or(std::string_view("nullopt")));
    return os;
}

std::ostream& operator<<(std::ostream& os, config_resource_type t) {
    switch (t) {
    case config_resource_type::topic:
        return os << "{topic}";
    case config_resource_type::broker:
        [[fallthrough]];
    case config_resource_type::broker_logger:
        break;
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, config_resource_operation t) {
    switch (t) {
    case config_resource_operation::set:
        return os << "set";
    case config_resource_operation::append:
        return os << "append";
    case config_resource_operation::remove:
        return os << "remove";
    case config_resource_operation::subtract:
        return os << "subtract";
    }
    return os << "unknown type";
}

std::ostream& operator<<(std::ostream& os, describe_configs_source s) {
    switch (s) {
    case describe_configs_source::topic:
        return os << "{topic}";
    case describe_configs_source::static_broker_config:
        return os << "{static_broker_config}";
    case describe_configs_source::default_config:
        return os << "{default_config}";
    }
    return os << "{unknown type}";
}

} // namespace kafka
