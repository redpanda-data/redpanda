// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/sasl_authenticate_request.h"

#include "kafka/errors.h"

namespace kafka {

ss::future<response_ptr> sasl_authenticate_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    sasl_authenticate_request request;
    request.decode(ctx.reader(), ctx.header().version);
    return ctx.respond(sasl_authenticate_response(
      error_code::illegal_sasl_state,
      "SASL authenticate request received after successful authentication"));
}

} // namespace kafka
