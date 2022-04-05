// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/sasl_handshake.h"

#include "kafka/protocol/errors.h"
#include "security/sasl_authentication.h"
#include "security/scram_authenticator.h"

static const std::vector<ss::sstring> supported_mechanisms = {
  security::scram_sha256_authenticator::name,
  security::scram_sha512_authenticator::name};

namespace kafka {

template<>
ss::future<response_ptr> sasl_handshake_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_handshake_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.debug, "Received SASL_HANDSHAKE {}", request);

    /*
     * configure sasl for the current connection context. see the sasl
     * authenticate request for the next phase of the auth process.
     */
    auto error = error_code::none;

    if (request.data.mechanism == security::scram_sha256_authenticator::name) {
        ctx.sasl().set_mechanism(
          std::make_unique<security::scram_sha256_authenticator::auth>(
            ctx.credentials()));

    } else if (
      request.data.mechanism == security::scram_sha512_authenticator::name) {
        ctx.sasl().set_mechanism(
          std::make_unique<security::scram_sha512_authenticator::auth>(
            ctx.credentials()));

    } else {
        error = error_code::unsupported_sasl_mechanism;
    }

    return ctx.respond(sasl_handshake_response(error, supported_mechanisms));
}

} // namespace kafka
