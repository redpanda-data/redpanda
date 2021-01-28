// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/sasl_handshake.h"

#include "kafka/protocol/errors.h"

namespace kafka {

template<>
ss::future<response_ptr> sasl_handshake_handler::handle(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_handshake_request request;
    request.decode(ctx.reader(), ctx.header().version);
    return ctx.respond(sasl_handshake_response(error_code::illegal_sasl_state));
}

} // namespace kafka
