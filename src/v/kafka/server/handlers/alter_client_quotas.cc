/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/alter_client_quotas.h"

#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"

namespace kafka {

namespace {

void make_error_response(
  alter_client_quotas_request& req, alter_client_quotas_response& resp) {
    for (const auto& entry [[maybe_unused]] : req.data.entries) {
        resp.data.entries.push_back(
          kafka::alter_client_quotas_response_entry_data{
            .error_code = error_code::unsupported_version,
            .error_message = "Unsupported version - not yet implemented",
          });
    }
}

} // namespace

template<>
ss::future<response_ptr> alter_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    alter_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    // TODO: implement the AlterClientQuotas API
    // ctx.quota_store().get_quota(...);
    // ctx.quota_frontend().alter_quotas(...);

    alter_client_quotas_response response;
    make_error_response(request, response);

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
