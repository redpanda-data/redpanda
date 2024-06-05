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

#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/alter_client_quotas.h"
#include "kafka/server/handlers/describe_client_quotas.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

namespace kafka {

namespace {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;
using cluster::client_quota::entity_value_diff;

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

describe_client_quotas_response_entity_data
get_entity_data(const entity_key::part& p) {
    using entity_data = describe_client_quotas_response_entity_data;
    return ss::visit(
      p.part,
      [](const entity_key::part::client_id_default_match&) -> entity_data {
          return {.entity_type = "client-id", .entity_name = std::nullopt};
      },
      [](const entity_key::part::client_id_match& m) -> entity_data {
          return {.entity_type = "client-id", .entity_name = m.value};
      },
      [](const entity_key::part::client_id_prefix_match& m) -> entity_data {
          return {.entity_type = "client-id-prefix", .entity_name = m.value};
      });
}

using entities_data
  = decltype(describe_client_quotas_response_entry_data::entity);

entities_data get_entity_data(const entity_key& k) {
    entities_data ret;
    ret.reserve(k.parts.size());
    for (const auto& p : k.parts) {
        ret.emplace_back(get_entity_data(p));
    }
    return ret;
}

using values_data
  = decltype(describe_client_quotas_response_entry_data::values);

values_data get_value_data(const entity_value& val) {
    values_data ret;

    if (val.producer_byte_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::producer_byte_rate)),
          *val.producer_byte_rate);
    }

    if (val.consumer_byte_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::consumer_byte_rate)),
          *val.consumer_byte_rate);
    }

    if (val.controller_mutation_rate) {
        ret.emplace_back(
          ss::sstring(
            to_string_view(entity_value_diff::key::controller_mutation_rate)),
          *val.controller_mutation_rate);
    }
    return ret;
}

} // namespace

template<>
ss::future<response_ptr> describe_client_quotas_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_client_quotas_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_client_quotas_response res{
      .data = {
        .error_code = kafka::error_code::none,
        .entries = decltype(res.data.entries)::value_type{}}};

    if (!ctx.authorized(
          security::acl_operation::describe_configs,
          security::default_cluster_name)) {
        res.data.error_code = error_code::cluster_authorization_failed;
        res.data.error_message = ss::sstring{
          error_code_to_str(error_code::cluster_authorization_failed)};
        return ctx.respond(std::move(res));
    }

    if (!ctx.audit()) {
        res.data.error_code = error_code::broker_not_available;
        res.data.error_message = "Broker not available - audit system failure";
        return ctx.respond(std::move(res));
    }

    auto quotas = ctx.quota_store().range(
      [](const std::pair<entity_key, entity_value>&) {
          // TODO: Matching strict && components
          return true;
      });

    res.data.entries->reserve(quotas.size());
    for (const auto& q : quotas) {
        res.data.entries->emplace_back(
          get_entity_data(q.first), get_value_data(q.second));
    }

    return ctx.respond(std::move(res));
}

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
