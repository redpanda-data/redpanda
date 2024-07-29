// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/handlers/api_versions.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/request_context.h"
#include "net/types.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/utils.h"
#include "utils/to_string.h"

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
      kwire.trace,
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
      kwire.trace,
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
        const auto reason = "Unexpected request during authentication";
        ss::sstring audit_msg;
        if (!ctx.audit_authn_failure(reason)) {
            audit_msg = " - Failed to audit authentication audit message";
        }
        return ss::make_exception_future<response_ptr>(
          kafka_authentication_exception(fmt_with_ctx(
            fmt::format, "{}: {}{}", ctx.header().key, reason, audit_msg)));
    }
}

static ss::future<response_ptr>
handle_auth(request_context&& ctx, ss::smp_service_group g) {
    switch (ctx.sasl()->state()) {
    case security::sasl_server::sasl_state::initial:
        return handle_auth_initial(std::move(ctx), g);

    case security::sasl_server::sasl_state::handshake:
        if (unlikely(ctx.header().key != sasl_handshake_handler::api::key)) {
            if (!ctx.audit_authn_failure(
                  "Unexpected auth request, expected handshake")) {
                return ss::make_exception_future<response_ptr>(
                  kafka_authentication_exception(fmt_with_ctx(
                    fmt::format,
                    "Unexpected auth request {} expected handshake - Failed to "
                    "audit authentication audit message",
                    ctx.header().key)));
            } else {
                return ss::make_exception_future<response_ptr>(
                  kafka_authentication_exception(fmt_with_ctx(
                    fmt::format,
                    "Unexpected auth request {} expected handshake",
                    ctx.header().key)));
            }
        }
        return handle_auth_handshake(std::move(ctx), g);

    case security::sasl_server::sasl_state::authenticate: {
        if (unlikely(ctx.header().key != sasl_authenticate_handler::api::key)) {
            if (!ctx.audit_authn_failure(
                  "Unexpected auth request, expected authenticate")) {
                return ss::make_exception_future<response_ptr>(
                  kafka_authentication_exception(fmt_with_ctx(
                    fmt::format,
                    "Unexpected auth request {} expected authenticate - Failed "
                    "to audit authentication audit message",
                    ctx.header().key)));
            } else {
                return ss::make_exception_future<response_ptr>(
                  kafka_authentication_exception(fmt_with_ctx(
                    fmt::format,
                    "Unexpected auth request {} expected authenticate",
                    ctx.header().key)));
            }
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
    auto key = ctx.header().key;

    if (
      ctx.sasl() && ctx.sasl()->complete()
      && key == sasl_handshake_handler::api::key) [[unlikely]] {
        // This is a client-driven reauthentication
        vlog(
          klog.debug,
          "SASL reauthentication detected - resetting authn server");
        ctx.sasl_probe().session_reauth();
        ctx.sasl()->reset();
    }
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

    if (key == sasl_handshake_handler::api::key) {
        sasl_handshake_response resp(error_code::illegal_sasl_state, {});
        if (!ctx.audit_authn_failure(
              "Unexpected SASL handshake message encountered")) {
            resp.data.error_code = error_code::broker_not_available;
        }

        return process_result_stages::single_stage(
          ctx.respond(std::move(resp)));
    }

    if (key == sasl_authenticate_handler::api::key) {
        sasl_authenticate_response_data data;
        if (!ctx.audit_authn_failure(
              "Authentication process already completed")) {
            data.error_code = error_code::broker_not_available;
            data.error_message = "Broker not availaable - audit system failure";
        } else {
            data.error_code = error_code::illegal_sasl_state;
            data.error_message = "Authentication process already completed";
        }
        return process_result_stages::single_stage(
          ctx.respond(sasl_authenticate_response(std::move(data))));
    }

    if (ctx.sasl() && ctx.sasl()->expired()) [[unlikely]] {
        throw sasl_session_expired_exception(fmt::format(
          "Session for client '{}' expired after {}",
          ctx.header().client_id.value_or(""),
          ctx.sasl()->max_reauth()));
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

} // namespace kafka
