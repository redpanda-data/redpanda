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

#include "cluster/client_quota_frontend.h"
#include "cluster/client_quota_serde.h"
#include "cluster/client_quota_store.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_response.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/alter_client_quotas.h"
#include "kafka/server/handlers/describe_client_quotas.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>
#include <boost/outcome/success_failure.hpp>
#include <boost/range/combine.hpp>

#include <algorithm>
#include <optional>
#include <utility>
#include <variant>

namespace kafka {

namespace {

using cluster::client_quota::entity_key;
using cluster::client_quota::entity_value;
using cluster::client_quota::entity_value_diff;

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

using kerror = std::pair<kafka::error_code, ss::sstring>;

result<entity_key::part, kerror>
exact_match_key(const component_data& component) {
    return string_switch<result<entity_key::part, kerror>>(
             component.entity_type)
      .match(
        "client-id",
        entity_key::part{.part = entity_key::client_id_match{*component.match}})
      .match(
        "client-id-prefix",
        entity_key::part{
          .part = entity_key::client_id_prefix_match{*component.match}})
      .match_all(
        "user",
        "ip",
        {
          error_code::unsupported_version,
          fmt::format(
            "Entity type '{}' not yet supported", component.entity_type),
        })
      .default_match({
        error_code::unsupported_version,
        fmt::format(
          "Custom entity type '{}' not supported", component.entity_type),
      });
}

result<entity_key::part, kerror>
default_match_key(const component_data& component) {
    return string_switch<result<entity_key::part, kerror>>(
             component.entity_type)
      .match(
        "client-id",
        entity_key::part{.part = entity_key::client_id_default_match{}})
      .match(
        "client-id-prefix",
        {kafka::error_code::invalid_request,
         "Invalid quota entity type, client-id-prefix entity should not "
         "be used at the default level (use client-id default instead)."})
      .match_all(
        "user",
        "ip",
        {
          error_code::unsupported_version,
          fmt::format(
            "Entity type '{}' not yet supported", component.entity_type),
        })
      .default_match({
        error_code::unsupported_version,
        fmt::format(
          "Custom entity type '{}' not supported", component.entity_type),
      });
}

using key_part_predicate = std::function<bool(const entity_key::part&)>;

template<typename... Args>
key_part_predicate make_any_filter() {
    return [](const entity_key::part& p) {
        return (std::holds_alternative<Args>(p.part) || ...);
    };
}

result<key_part_predicate, kerror>
any_match_filter(const component_data& component) {
    return string_switch<result<key_part_predicate, kerror>>(
             component.entity_type)
      .match(
        "client-id",
        make_any_filter<
          entity_key::part::client_id_default_match,
          entity_key::part::client_id_match>())
      .match(
        "client-id-prefix",
        make_any_filter<entity_key::part::client_id_prefix_match>())
      .match_all(
        "user",
        "ip",
        {
          error_code::unsupported_version,
          fmt::format(
            "Entity type '{}' not yet supported", component.entity_type),
        })
      .default_match({
        error_code::unsupported_version,
        fmt::format(
          "Custom entity type '{}' not supported", component.entity_type),
      });
}

result<key_part_predicate, kerror>
make_filter(const component_data& component) {
    switch (component.match_type) {
    case describe_client_quotas_match_type::exact_name: {
        if (!component.match) {
            return kerror{
              error_code::invalid_request,
              "Unspecified match field for exact_name match type",
            };
        }

        auto key_or_err = exact_match_key(component);
        if (key_or_err.has_error()) {
            return std::move(key_or_err).assume_error();
        }

        return [key = std::move(key_or_err).assume_value()](
                 const entity_key::part& p) { return p == key; };
    }
    case describe_client_quotas_match_type::default_name: {
        auto key_or_err = default_match_key(component);
        if (key_or_err.has_error()) {
            return std::move(key_or_err).assume_error();
        }
        return [key = std::move(key_or_err).assume_value()](
                 const entity_key::part& p) { return p == key; };
    }
    case describe_client_quotas_match_type::any_specified_name: {
        return any_match_filter(component);
    }
    }
}

bool is_null_or_empty(const std::optional<ss::sstring>& opt_str) {
    return opt_str.value_or("") == "";
}

result<entity_key::part, kerror> make_part(const auto& entity) {
    entity_key::part part;
    if (entity.entity_type == "client-id") {
        if (is_null_or_empty(entity.entity_name)) {
            part.part.emplace<entity_key::part::client_id_default_match>();
        } else {
            part.part.emplace<entity_key::part::client_id_match>(
              entity_key::part::client_id_match{
                .value = entity.entity_name.value_or("")});
        }
    } else if (entity.entity_type == "client-id-prefix") {
        if (is_null_or_empty(entity.entity_name)) {
            return {
              kafka::error_code::invalid_request,
              "Invalid quota entity type, client-id-prefix entity should not "
              "be used at the default level (use client-id default instead)."};
        }
        part.part.emplace<entity_key::part::client_id_prefix_match>(
          entity_key::part::client_id_prefix_match{
            .value = entity.entity_name.value_or("")});
    } else if (entity.entity_type == "user" || entity.entity_type == "ip") {
        return {
          error_code::unsupported_version,
          fmt::format("Entity type '{}' not yet supported", entity.entity_type),
        };
    } else {
        return {
          kafka::error_code::invalid_request,
          fmt::format(
            "Unhandled client quota entity type: {}", entity.entity_type)};
    }
    return part;
};

using alter_entities = chunked_vector<alter_client_quotas_request_entity_data>;
result<entity_key, kerror> make_key(const alter_entities& entity) {
    // TODO: once we support compound user+client keys, we should check that
    // either there's only a single key part or the key is a user+client
    // compound key
    if (entity.size() != 1) {
        return kerror{
          error_code::invalid_request,
          "Invalid client quota entity",
        };
    }

    entity_key key;
    key.parts.reserve(entity.size());
    for (const auto& entity : entity) {
        auto part = make_part(entity);
        if (part.has_error()) {
            return std::move(part).assume_error();
        }
        key.parts.emplace(std::move(part).assume_value());
    }

    return key;
}

bool valid_key_combination(const entity_key&, entity_value_diff::key) {
    // TODO: when we add support for user/ip quotas, we should validate that
    // only valid entity + configuration combinations are configured:
    // * client/user -- produce/fetch/controller mutation/request percentage
    // * ip -- connection rate
    // For now, all combinations we can parse are valid
    return true;
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

    std::optional<key_part_predicate> client_predicate;
    // std::optional<key_part_predicate> user_predicate;
    // std::optional<key_part_predicate> ip_predicate;

    for (const auto& component : request.data.components) {
        auto filter_or_err = make_filter(component);

        if (filter_or_err.has_error()) {
            std::tie(res.data.error_code, res.data.error_message)
              = std::move(filter_or_err).assume_error();
            return ctx.respond(std::move(res));
        }

        auto& predicate = [&]() mutable -> auto& {
            return client_predicate;
            // TODO: later add support for user/ip quotas
            // if (component.entity_type == "client-id" ||
            // component.entity_type == "client-id-prefix") {
            //     return client_predicate;
            // } else if (component.entity_type == "user") {
            //     return user_predicate;
            // } else if (component.entity_type == "ip") {
            //     return ip_predicate;
            // } else {
            //     // ERROR: unknown
            // }
        }();

        if (predicate.has_value()) {
            res.data.error_code = error_code::invalid_request;
            res.data.error_message = fmt::format(
              "Duplicate filter component for entity type '{}'",
              component.entity_type);
            return ctx.respond(std::move(res));
        }

        predicate = std::move(filter_or_err).assume_value();
    }

    auto quotas = ctx.quota_store().range(
      [&client_predicate, strict = request.data.strict](
        const std::pair<entity_key, entity_value>& kv) {
          // Each predicate in the request needs to have at least one key part
          // that matches it (regardless of strict mode)
          const auto& key = kv.first;
          auto each_predicate_has_a_match = !client_predicate
                                            || absl::c_any_of(
                                              key.parts, *client_predicate);
          // && (!user_predicate || absl::c_any_of(key.parts, *user_predicate))
          // && (!ip_predicate || absl::c_any_of(key.parts, *ip_predicate));

          if (!each_predicate_has_a_match) {
              return false;
          }

          // In strict mode, also require that each key part has a matching
          // predicate
          auto reverse_predicate =
            [&client_predicate](const entity_key::part& part) {
                return client_predicate && (*client_predicate)(part);
                //  || (user_predicate && (*user_predicate)(part))
                //  || (ip_predicate && (*ip_predicate)(part));
            };
          return !strict || absl::c_all_of(key.parts, reverse_predicate);
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

    cluster::client_quota::alter_delta_cmd_data cmd;
    alter_client_quotas_response response;
    response.data.entries.reserve(request.data.entries.size());

    for (const auto& entry_req : request.data.entries) {
        auto& entry_res = response.data.entries.emplace_back();

        entry_res.entity.reserve(entry_req.entity.size());
        std::transform(
          entry_req.entity.begin(),
          entry_req.entity.end(),
          std::back_inserter(entry_res.entity),
          [](const alter_client_quotas_request_entity_data& in) {
              return alter_client_quotas_response_entity_data{
                .entity_type = in.entity_type, .entity_name = in.entity_name};
          });
    }

    if (!ctx.authorized(
          security::acl_operation::alter_configs,
          security::default_cluster_name)) {
        for (auto& entry : response.data.entries) {
            entry.error_code = error_code::cluster_authorization_failed;
            entry.error_message = ss::sstring{
              error_code_to_str(error_code::cluster_authorization_failed)};
        }
        co_return co_await ctx.respond(std::move(response));
    }

    if (!ctx.audit()) {
        for (auto& entry : response.data.entries) {
            entry.error_code = error_code::broker_not_available;
            entry.error_message = "Broker not available - audit system failure";
        }
        co_return co_await ctx.respond(std::move(response));
    }

    for (const auto& [entry, entry_res] :
         boost::combine(request.data.entries, response.data.entries)) {
        auto key_or_err = make_key(entry.entity);
        if (key_or_err.has_error()) {
            std::tie(entry_res.error_code, entry_res.error_message)
              = std::move(key_or_err).assume_error();
            continue;
        }
        entity_key key = std::move(key_or_err).assume_value();

        entity_value_diff diff;
        for (const auto& op : entry.ops) {
            auto cqt
              = cluster::client_quota::from_string_view<entity_value_diff::key>(
                op.key);
            if (!cqt || !valid_key_combination(key, *cqt)) {
                entry_res.error_code = kafka::error_code::invalid_request;
                entry_res.error_message = fmt::format(
                  "Invalid configuration key {}", op.key);
                break;
            }
            diff.entries.emplace(
              op.remove ? entity_value_diff::operation::remove
                        : entity_value_diff::operation::upsert,
              *cqt,
              op.value);
        }
        if (entry_res.error_code == error_code::none) {
            cmd.ops.push_back({.key = std::move(key), .diff = std::move(diff)});
        }
    }

    if (request.data.validate_only) {
        co_return co_await ctx.respond(std::move(response));
    }

    auto err = co_await ctx.quota_frontend().alter_quotas(
      cmd, model::timeout_clock::now() + 5s);

    if (err != cluster::errc::success) {
        // Translate error message
        auto ec = map_topic_error_code(err);
        auto em = error_code_to_str(ec);
        // Error any response that is not already errored
        for (auto& entry : response.data.entries) {
            if (entry.error_code == error_code::none) {
                entry.error_code = ec;
                entry.error_message = std::make_optional<ss::sstring>(em);
            }
        }
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
