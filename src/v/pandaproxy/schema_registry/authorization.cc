/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/schema_registry/authorization.h"

#include "container/chunked_hash_map.h"
#include "pandaproxy/api/api-doc/schema_registry.json.hh"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/context_router.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/acl.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/types.h"
#include "security/authorizer.h"
#include "security/request_auth.h"

#include <seastar/util/variant_utils.hh>

namespace pandaproxy::schema_registry::enterprise {

namespace detail {

template<typename T>
concept no_auth = std::is_same_v<T, auth::none>
                  || std::is_same_v<T, auth::deferred>;

template<typename T>
concept requires_auth = !no_auth<T>;

security::acl_principal get_principal(const server::request_t& rq) {
    return security::acl_principal{
      security::principal_type::user, rq.user.name};
}
struct auth_params {
    security::acl_principal principal;
    security::acl_host host;

    explicit auth_params(const server::request_t& rq)
      : principal{get_principal(rq)}
      , host{rq.req->get_client_address().addr()} {}
};

} // namespace detail

namespace {

auth::resource
extract_resource_from_request(const server::request_t& rq, const auth& auth) {
    return ss::visit(
      auth.get_resource(),
      [&rq](const context_subject&) -> auth::resource {
          return context_subject::from_string(
            parse::request_param<ss::sstring>(*rq.req, "subject"));
      },
      [&rq](const auth::context_prefix_subject&) -> auth::resource {
          auto ctx = normalize_context(
            parse::request_param<ss::sstring>(*rq.req, "context"));
          auto sub = parse::request_param<ss::sstring>(*rq.req, "subject");
          if (!starts_with_context(sub)) {
              sub = fmt::format(":{}:{}", ctx, sub);
          }
          return context_subject::from_string(sub);
      },
      [](const auto& res) -> auth::resource { return res; });
}

void throw_unauthorized() {
    throw ss::httpd::base_exception(
      "Forbidden (missing required ACLs)",
      ss::http::reply::status_type::forbidden);
}

template<typename... Args>
void audit_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  Args&&... args) {
    if (!rq.service().audit_mgr().enqueue_authz_audit_event(
          security::audit::event_type::schema_registry,
          audit_svc_name,
          *rq.req,
          operation_name,
          std::forward<Args>(args)...)) {
        throw ss::httpd::base_exception(
          "Failed to audit authorization request",
          ss::http::reply::status_type::service_unavailable);
    }
}

void check_authenticated(
  const server::request_t& rq,
  std::string_view operation_name,
  security::acl_operation op,
  request_auth_result& auth_result) {
    try {
        auth_result.require_authenticated();
    } catch (const ss::httpd::base_exception& e) {
        audit_authz(
          rq, operation_name, auth_result, false, op, ss::sstring{e.what()});
        throw;
    }
}

const auto subject_resource_type = ssx::sformat(
  "{}", security::resource_type::sr_subject);

const auto registry_resource_type = ssx::sformat(
  "{}", security::resource_type::sr_registry);

using audit_resources = chunked_vector<security::audit::resource_detail>;

} // namespace

void handle_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  const auth& auth,
  request_auth_result& auth_result) {
    auto params = detail::auth_params{rq};
    auto op = auth.get_op().value_or(security::acl_operation::all);

    auto resource = extract_resource_from_request(rq, auth);

    ss::visit(
      resource,
      [&](const auth::none&) { auth_result.pass(); },
      [&](const auto&) {
          check_authenticated(rq, operation_name, op, auth_result);
      });

    // Check Authorization
    auto authz_result = ss::visit(
      resource,
      [&](const detail::requires_auth auto& resource_name) {
          return rq.service().authorizor().authorized(
            resource_name,
            op,
            params.principal,
            params.host,
            security::superuser_required::no,
            auth_result.get_groups());
      },
      [&](const detail::no_auth auto&) {
          return security::auth_result::authz_disabled(
            params.principal, params.host, op, registry_resource{});
      });

    const bool is_authorized = authz_result.is_authorized();

    audit_authz(rq, operation_name, std::move(authz_result));

    if (!is_authorized) {
        throw_unauthorized();
    }
}

void handle_get_schemas_ids_id_authz(
  const server::request_t& rq,
  std::optional<request_auth_result>& auth_result,
  const chunked_vector<context_subject>& subjects) {
    const auto& operation_name
      = ss::httpd::schema_registry_json::get_schemas_ids_id.operations.nickname;
    constexpr auto op = security::acl_operation::read;
    if (!auth_result.has_value()) {
        // ACLs or authentication is disabled
        return;
    }

    check_authenticated(rq, operation_name, op, *auth_result);

    auto params = detail::auth_params{rq};

    if (subjects.empty()) {
        // If there are no subjects associated with the schema id, it does
        // not exist.
        // Throw unauthorized here to avoid leaking information about whether a
        // schema id exists or not.
        audit_authz(
          rq,
          operation_name,
          auth_result.value(),
          false,
          op,
          audit_resources{});
        throw_unauthorized();
    }

    auto authorizing_result = std::optional<security::auth_result>{};
    auto all_results = audit_resources{};
    for (const auto& ctx_sub : subjects) {
        auto res = rq.service().authorizor().authorized(
          ctx_sub,
          op,
          params.principal,
          params.host,
          security::superuser_required::no,
          auth_result.value().get_groups());

        if (res.is_authorized()) {
            authorizing_result = std::move(res);
            break;
        } else {
            all_results.emplace_back(
              ctx_sub.to_string(), subject_resource_type);
        }
    }

    if (authorizing_result.has_value()) {
        audit_authz(rq, operation_name, std::move(*authorizing_result));
    } else {
        audit_authz(
          rq,
          operation_name,
          auth_result.value(),
          false,
          op,
          std::move(all_results));
        throw_unauthorized();
    }
}

void handle_get_subjects_authz(
  const server::request_t& rq,
  std::optional<request_auth_result>& auth_result,
  chunked_vector<context_subject>& subjects) {
    const auto& operation_name
      = ss::httpd::schema_registry_json::get_subjects.operations.nickname;
    constexpr auto op = security::acl_operation::describe;

    if (!auth_result.has_value()) {
        // ACLs or authentication is disabled
        return;
    }

    check_authenticated(rq, operation_name, op, *auth_result);

    auto params = detail::auth_params{rq};

    auto passing_results = audit_resources{};
    auto failing_results = audit_resources{};

    auto new_end = std::ranges::remove_if(subjects, [&](const auto& ctx_sub) {
        auto res = rq.service().authorizor().authorized(
          ctx_sub,
          op,
          params.principal,
          params.host,
          security::superuser_required::no,
          auth_result.value().get_groups());
        if (res.is_authorized()) {
            passing_results.emplace_back(
              ctx_sub.to_string(), subject_resource_type);
            return false; // keep
        } else {
            failing_results.emplace_back(
              ctx_sub.to_string(), subject_resource_type);
            return true; // remove
        }
    });
    subjects.erase_to_end(new_end.begin());

    // This endpoint always returns a successful response.
    // Generate a successful audit event with the (possibly empty) list of
    // authorized subjects.
    // If there are any unauthorized subjects, generate failed audit event with
    // them.
    audit_authz(
      rq,
      operation_name,
      auth_result.value(),
      true,
      op,
      std::move(passing_results));

    if (!failing_results.empty()) {
        audit_authz(
          rq,
          operation_name,
          auth_result.value(),
          false,
          op,
          std::move(failing_results));
    }
}

ss::future<> handle_get_contexts_authz(
  const server::request_t& rq,
  sharded_store& store,
  std::optional<request_auth_result>& auth_result,
  chunked_vector<context>& contexts) {
    const auto& operation_name
      = ss::httpd::schema_registry_json::get_contexts.operations.nickname;
    constexpr auto op = security::acl_operation::describe;

    if (!auth_result.has_value()) {
        co_return;
    }

    check_authenticated(rq, operation_name, op, *auth_result);

    auto params = detail::auth_params{rq};

    auto has_registry_describe = rq.service().authorizor().authorized(
      registry_resource{},
      op,
      params.principal,
      params.host,
      security::superuser_required::no,
      auth_result.value().get_groups());

    auto all_subjects = co_await store.get_subjects(include_deleted::yes);

    auto passing_results = audit_resources{};
    auto failing_results = audit_resources{};

    // Pass 1: Iterate subjects once, check authorization, track accessible
    // contexts. Audit entries match the actual ACL checks performed.
    auto allowed_contexts = chunked_hash_set<context>{};
    auto non_empty_contexts = chunked_hash_set<context>{};

    for (const auto& ctx_sub : all_subjects) {
        // Track that this context has subjects
        if (!non_empty_contexts.contains(ctx_sub.ctx)) {
            non_empty_contexts.insert(ctx_sub.ctx);
        }

        // Skip if we already know this context is accessible
        if (allowed_contexts.contains(ctx_sub.ctx)) {
            continue;
        }

        auto res = rq.service().authorizor().authorized(
          ctx_sub,
          op,
          params.principal,
          params.host,
          security::superuser_required::no,
          auth_result.value().get_groups());

        if (res.is_authorized()) {
            passing_results.emplace_back(
              ctx_sub.to_string(), subject_resource_type);
            allowed_contexts.insert(ctx_sub.ctx);
        } else {
            failing_results.emplace_back(
              ctx_sub.to_string(), subject_resource_type);
        }
    }

    // Pass 2: Filter contexts list. For empty contexts, check registry access.
    auto result_contexts = chunked_vector<context>{};
    auto has_empty_contexts = false;

    for (const auto& ctx : contexts) {
        auto has_subjects = non_empty_contexts.contains(ctx);

        if (!has_subjects) {
            // Empty context: requires sr_registry describe access
            has_empty_contexts = true;
            if (has_registry_describe.is_authorized()) {
                result_contexts.push_back(ctx);
            }
        } else if (allowed_contexts.contains(ctx)) {
            result_contexts.push_back(ctx);
        }
    }

    // Audit the registry resource check once (if any empty contexts existed)
    if (has_empty_contexts) {
        if (has_registry_describe.is_authorized()) {
            passing_results.emplace_back(
              registry_resource{}(), registry_resource_type);
        } else {
            failing_results.emplace_back(
              registry_resource{}(), registry_resource_type);
        }
    }

    contexts = std::move(result_contexts);

    audit_authz(
      rq,
      operation_name,
      auth_result.value(),
      true,
      op,
      std::move(passing_results));

    if (!failing_results.empty()) {
        audit_authz(
          rq,
          operation_name,
          auth_result.value(),
          false,
          op,
          std::move(failing_results));
    }
}

void handle_config_mode_authz(
  const server::request_t& rq,
  std::string_view operation_name,
  std::optional<request_auth_result>& auth_result,
  const context_subject& ctx_sub,
  security::acl_operation op) {
    if (!auth_result.has_value()) {
        // ACLs or authentication is disabled
        return;
    }

    check_authenticated(rq, operation_name, op, *auth_result);

    auto params = detail::auth_params{rq};

    // Context-level operations require sr_registry access
    // Subject-level operations require sr_subject access
    auto authz_result = ctx_sub.is_context_only()
                          ? rq.service().authorizor().authorized(
                              registry_resource{},
                              op,
                              params.principal,
                              params.host,
                              security::superuser_required::no,
                              auth_result.value().get_groups())
                          : rq.service().authorizor().authorized(
                              ctx_sub,
                              op,
                              params.principal,
                              params.host,
                              security::superuser_required::no,
                              auth_result.value().get_groups());

    const bool is_authorized = authz_result.is_authorized();

    audit_authz(rq, operation_name, std::move(authz_result));

    if (!is_authorized) {
        throw_unauthorized();
    }
}

} // namespace pandaproxy::schema_registry::enterprise
