// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/handlers/api_versions.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/request_context.h"
#include "kafka/types.h"
#include "net/types.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka {

/**
 * Dispatch request with version bounds checking.
 */
// clang-format off
template<typename Request>
requires(KafkaApiHandler<Request> || KafkaApiTwoPhaseHandler<Request>)
struct process_dispatch { // clang-format on
    static process_result_stages
    process(request_context&& ctx, ss::smp_service_group g) {
        return process_result_stages::single_stage(
          Request::handle(std::move(ctx), g));
    }
};

template<typename Request>
requires(KafkaApiHandler<Request> || KafkaApiTwoPhaseHandler<Request>)
  process_result_stages
  do_process(request_context&& ctx, ss::smp_service_group g) {
    vlog(
      klog.trace,
      "[{}:{}] processing name:{}, key:{}, version:{} for {}",
      ctx.connection()->client_host(),
      ctx.connection()->client_port(),
      Request::api::name,
      ctx.header().key,
      ctx.header().version,
      ctx.header().client_id.value_or(std::string_view("unset-client-id")));

    /**
     * Dispatch API versions request without version checks.
     *
     * The version bounds checks are not applied to this request because the
     * client does not yet know what versions this server supports. The api
     * versions request is used by a client to query this information.
     */
    if constexpr (!std::same_as<Request, api_versions_handler>) {
        if (
          ctx.header().version < Request::min_supported
          || ctx.header().version > Request::max_supported) {
            throw kafka_api_version_not_supported_exception(fmt::format(
              "Unsupported version {} for {} API",
              ctx.header().version,
              Request::api::name));
        }
    }

    return process_dispatch<Request>::process(std::move(ctx), g);
}

process_result_stages process_generic(
  handler handler,
  request_context&& ctx,
  ss::smp_service_group g,
  const session_resources& sres) {
    vlog(
      klog.trace,
      "[{}:{}] processing name:{}, key:{}, version:{} for {}, mem_units: {}, "
      "ctx_size: {}",
      ctx.connection()->client_host(),
      ctx.connection()->client_port(),
      handler->name(),
      ctx.header().key,
      ctx.header().version,
      ctx.header().client_id.value_or(std::string_view("unset-client-id")),
      sres.memlocks.count(),
      ctx.reader().bytes_left());

    // We do a version check for most API requests, but for api_version
    // requests we skip them. We do not apply them for api_versions,
    // because the client does not yet know what
    // versions this server supports. The api versions request is used by a
    // client to query this information.
    if (ctx.header().key != api_versions_api::key &&
      (ctx.header().version < handler->min_supported() ||
       ctx.header().version > handler->max_supported())) {
        throw kafka_api_version_not_supported_exception(fmt::format(
          "Unsupported version {} for {} API",
          ctx.header().version,
          handler->name()));
    }

    return handler->handle(std::move(ctx), g);
}

class kafka_authentication_exception : public net::authentication_exception {
public:
    explicit kafka_authentication_exception(const std::string& m)
      : net::authentication_exception(m) {}
};

/*
 * process a handshake request. if it doesn't result in a sasl mechanism being
 * selected then client negotiation failed. otherwise, move to authentication.
 */
static ss::future<response_ptr>
handle_auth_handshake(request_context&& ctx, ss::smp_service_group g) {
    auto conn = ctx.connection();
    if (ctx.header().version == api_version(0)) {
        /*
         * see connection_context::process_one_request for more info. when this
         * is set the input connection is assumed to contain raw authentication
         * tokens not wrapped in a normal kafka request envelope.
         */
        conn->sasl()->set_handshake_v0();
    }
    return do_process<sasl_handshake_handler>(std::move(ctx), g)
      .response.then([conn = std::move(conn)](response_ptr r) {
          if (conn->sasl()->has_mechanism()) {
              conn->sasl()->set_state(
                security::sasl_server::sasl_state::authenticate);
          } else {
              conn->sasl()->set_state(
                security::sasl_server::sasl_state::failed);
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
    case api_versions_api::key: {
        auto r = api_versions_handler::handle_raw(ctx);
        if (r.data.error_code == error_code::none) {
            ctx.sasl()->set_state(security::sasl_server::sasl_state::handshake);
        }
        return ctx.respond(std::move(r));
    }

    case sasl_handshake_handler::api::key: {
        return handle_auth_handshake(std::move(ctx), g);
    }

    default:
        return ss::make_exception_future<response_ptr>(
          kafka_authentication_exception(fmt_with_ctx(
            fmt::format,
            "Unexpected request during authentication: {}",
            ctx.header().key)));
    }
}

static ss::future<response_ptr>
handle_auth(request_context&& ctx, ss::smp_service_group g) {
    switch (ctx.sasl()->state()) {
    case security::sasl_server::sasl_state::initial:
        return handle_auth_initial(std::move(ctx), g);

    case security::sasl_server::sasl_state::handshake:
        if (unlikely(ctx.header().key != sasl_handshake_handler::api::key)) {
            return ss::make_exception_future<response_ptr>(
              kafka_authentication_exception(fmt_with_ctx(
                fmt::format,
                "Unexpected auth request {} expected handshake",
                ctx.header().key)));
        }
        return handle_auth_handshake(std::move(ctx), g);

    case security::sasl_server::sasl_state::authenticate: {
        if (unlikely(ctx.header().key != sasl_authenticate_handler::api::key)) {
            return ss::make_exception_future<response_ptr>(
              kafka_authentication_exception(fmt_with_ctx(
                fmt::format,
                "Unexpected auth request {} expected authenticate",
                ctx.header().key)));
        }
        auto conn = ctx.connection();
        return do_process<sasl_authenticate_handler>(std::move(ctx), g)
          .response.then([conn = std::move(conn)](response_ptr r) {
              /*
               * there may be multiple authentication round-trips so it is fine
               * to return without entering an end state like complete/failed.
               */
              if (conn->sasl()->mechanism().complete()) {
                  conn->sasl()->set_state(
                    security::sasl_server::sasl_state::complete);
              } else if (conn->sasl()->mechanism().failed()) {
                  conn->sasl()->set_state(
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
        return ss::make_exception_future<response_ptr>(
          kafka_authentication_exception(
            "Authentication failed. Shutting down connection"));

    default:
        return ss::make_exception_future<response_ptr>(
          kafka_authentication_exception(fmt_with_ctx(
            fmt::format,
            "Unexpected request during authentication: {}",
            ctx.header().key)));
    }
}

// only track latency for push and fetch requests
bool track_latency(api_key key) {
    switch (key) {
    case fetch_api::key:
    case produce_api::key:
        return true;
    default:
        return false;
    }
}

process_result_stages process_request(
  request_context&& ctx,
  ss::smp_service_group g,
  const session_resources& sres) {
    /*
     * requests are handled as normal when auth is disabled. otherwise no
     * request is handled until the auth process has completed.
     */
    if (unlikely(ctx.sasl() && !ctx.sasl()->complete())) {
        auto conn = ctx.connection();
        return process_result_stages::single_stage(
          handle_auth(std::move(ctx), g)
            .then_wrapped([conn](ss::future<response_ptr> f) {
                if (f.failed()) {
                    conn->sasl()->set_state(
                      security::sasl_server::sasl_state::failed);
                }
                return f;
            }));
    }

    auto& key = ctx.header().key;

    if (key == sasl_handshake_handler::api::key) {
        return process_result_stages::single_stage(ctx.respond(
          sasl_handshake_response(error_code::illegal_sasl_state, {})));
    }

    if (key == sasl_authenticate_handler::api::key) {
        sasl_authenticate_response_data data{
          .error_code = error_code::illegal_sasl_state,
          .error_message = "Authentication process already completed",
        };
        return process_result_stages::single_stage(
          ctx.respond(sasl_authenticate_response(std::move(data))));
    }

    if (auto handler = handler_for_key(key)) {
        return process_generic(*handler, std::move(ctx), g, sres);
    }

    throw std::runtime_error(
      fmt::format("Unsupported API {}", ctx.header().key));
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    fmt::print(
      os,
      "{{key:{}, version:{}, correlation_id:{}, client_id:{}, "
      "number_of_tagged_fields: {}, tags_size_bytes:{}}}",
      header.key,
      header.version,
      header.correlation,
      header.client_id.value_or(std::string_view("nullopt")),
      (header.tags ? (*header.tags)().size() : 0),
      header.tags_size_bytes);
    return os;
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

} // namespace kafka
