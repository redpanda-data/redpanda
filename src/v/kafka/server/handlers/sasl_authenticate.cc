// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/sasl_authenticate.h"

#include "bytes/bytes.h"
#include "kafka/protocol/errors.h"
#include "security/errc.h"
#include "security/scram_algorithm.h"
#include "vlog.h"

namespace kafka {

template<>
ss::future<response_ptr> sasl_authenticate_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_authenticate_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.debug, "Received SASL_AUTHENTICATE {}", request);

    std::error_code ec;

    try {
        auto result = ctx.sasl()->authenticate(
          std::move(request.data.auth_bytes));
        if (likely(result)) {
            sasl_authenticate_response_data data{
              .error_code = error_code::none,
              .error_message = std::nullopt,
              .auth_bytes = std::move(result.value()),
            };
            return ctx.respond(sasl_authenticate_response(std::move(data)));
        }

        ec = result.error();

    } catch (security::scram_exception& e) {
        vlog(
          klog.warn,
          "[{}:{}] Error processing SASL authentication request for {}: {}",
          ctx.connection()->client_host(),
          ctx.connection()->client_port(),
          ctx.header().client_id.value_or(std::string_view("unset-client-id")),
          e);

        ec = make_error_code(security::errc::invalid_credentials);
    }

    sasl_authenticate_response_data data{
      .error_code = error_code::sasl_authentication_failed,
      .error_message = ssx::sformat(
        "SASL authentication failed: {}", ec.message()),
    };
    return ctx.respond(sasl_authenticate_response(std::move(data)));
}

} // namespace kafka
