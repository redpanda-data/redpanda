/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#include "cluster_link/security_migrator.h"

#include "cluster_link/link.h"
#include "cluster_link/model/types.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "security/acl.h"

#include <algorithm>
#include <cstdint>

using namespace std::chrono_literals;

namespace cluster_link {

namespace {
bool has_required_permissions(
  kafka::cluster_authorized_operations permissions_to_check,
  kafka::cluster_authorized_operations required_permissions) {
    return (permissions_to_check & required_permissions)
           == required_permissions;
}

security::acl_operation model_to_security(model::acl_operation op) {
    switch (op) {
    case model::acl_operation::any:
        vassert(
          false,
          "Cannot convert acl_operation::any to security::acl_operation");
    case model::acl_operation::all:
        return security::acl_operation::all;
    case model::acl_operation::read:
        return security::acl_operation::read;
    case model::acl_operation::write:
        return security::acl_operation::write;
    case model::acl_operation::create:
        return security::acl_operation::create;
    case model::acl_operation::remove:
        return security::acl_operation::remove;
    case model::acl_operation::alter:
        return security::acl_operation::alter;
    case model::acl_operation::describe:
        return security::acl_operation::describe;
    case model::acl_operation::cluster_action:
        return security::acl_operation::cluster_action;
    case model::acl_operation::describe_configs:
        return security::acl_operation::describe_configs;
    case model::acl_operation::alter_configs:
        return security::acl_operation::alter_configs;
    case model::acl_operation::idempotent_write:
        return security::acl_operation::idempotent_write;
    }
}

security::acl_permission model_to_security(model::acl_permission_type t) {
    switch (t) {
    case model::acl_permission_type::any:
        vassert(
          false,
          "Cannot convert acl_permission_type::any to "
          "security::acl_permission");
    case model::acl_permission_type::deny:
        return security::acl_permission::deny;
    case model::acl_permission_type::allow:
        return security::acl_permission::allow;
    }
}

security::pattern_type model_to_security(model::acl_pattern p) {
    switch (p) {
    case model::acl_pattern::any:
        vassert(
          false, "Cannot convert acl_pattern::any to security::pattern_type");
    case model::acl_pattern::literal:
        return security::pattern_type::literal;
    case model::acl_pattern::prefixed:
        return security::pattern_type::prefixed;
    case model::acl_pattern::match:
        vassert(
          false,
          "No conversion from acl_pattern::match to security::pattern_type");
    }
}

security::resource_type model_to_security(model::acl_resource r) {
    switch (r) {
    case model::acl_resource::any:
        vassert(
          false, "Cannot convert acl_resource::any to security::resource_type");
    case model::acl_resource::topic:
        return security::resource_type::topic;
    case model::acl_resource::group:
        return security::resource_type::group;
    case model::acl_resource::cluster:
        return security::resource_type::cluster;
    case model::acl_resource::txn_id:
        return security::resource_type::transactional_id;
    case model::acl_resource::schema_registry_subject:
        return security::resource_type::sr_subject;
    case model::acl_resource::schema_registry_global:
        return security::resource_type::sr_registry;
    case model::acl_resource::schema_registry_any:
        vassert(
          false,
          "Cannot convert acl_resource::schema_registry_any to "
          "security::resource_type");
    }
}

bool is_registry_resource(model::acl_resource resource) {
    switch (resource) {
    case model::acl_resource::any:
    case model::acl_resource::cluster:
    case model::acl_resource::group:
    case model::acl_resource::topic:
    case model::acl_resource::txn_id:
        return false;
    case model::acl_resource::schema_registry_subject:
    case model::acl_resource::schema_registry_global:
    case model::acl_resource::schema_registry_any:
        return true;
    }
}

int8_t to_kafka_resource_type(model::acl_resource resource) {
    switch (resource) {
    case model::acl_resource::any:
    case model::acl_resource::schema_registry_any:
        return 1;
    default: {
        auto resource_type = model_to_security(resource);
        if (is_registry_resource(resource)) {
            return kafka::details::to_kafka_registry_resource_type(
              resource_type);
        } else {
            return kafka::details::to_kafka_resource_type(resource_type);
        }
    }
    }
}

int8_t to_kafka_resource_pattern_type(model::acl_pattern pattern) {
    switch (pattern) {
    case model::acl_pattern::any:
        return 1;
    case model::acl_pattern::match:
        return 2;
    default:
        return kafka::details::to_kafka_pattern_type(
          model_to_security(pattern));
    }
}

int8_t to_kafka_operation(model::acl_operation op) {
    switch (op) {
    case model::acl_operation::any:
        return 1;
    default:
        return kafka::details::to_kafka_operation(model_to_security(op));
    }
}

int8_t to_kafka_permission_type(model::acl_permission_type type) {
    switch (type) {
    case model::acl_permission_type::any:
        return 1;
    default:
        return kafka::details::to_kafka_permission(model_to_security(type));
    }
}

kafka::describe_acls_request_data
filter_to_request_data(const model::acl_filter& filter) {
    kafka::describe_acls_request_data data;
    data.describe_registry_acls = is_registry_resource(
      filter.resource_filter.resource_type);
    data.resource_type = to_kafka_resource_type(
      filter.resource_filter.resource_type);
    if (!filter.resource_filter.name.empty()) {
        data.resource_name_filter = filter.resource_filter.name;
    }
    data.resource_pattern_type = to_kafka_resource_pattern_type(
      filter.resource_filter.pattern_type);

    if (!filter.access_filter.principal.empty()) {
        data.principal_filter = filter.access_filter.principal;
    }
    if (!filter.access_filter.host.empty()) {
        data.host_filter = filter.access_filter.host;
    }
    data.operation = to_kafka_operation(filter.access_filter.operation);
    data.permission_type = to_kafka_permission_type(
      filter.access_filter.permission_type);

    return data;
}

std::vector<security::acl_binding>
to_acl_bindings(const kafka::describe_acls_resource& r) {
    if (r.name.empty()) {
        throw security::acl_conversion_error("Empty resource name");
    }

    security::resource_pattern pattern(
      r.registry_resource ? kafka::details::to_registry_resource_type(r.type)
                          : kafka::details::to_resource_type(r.type),
      r.name,
      kafka::details::to_pattern_type(r.pattern_type));

    std::vector<security::acl_entry> entries;
    entries.reserve(r.acls.size());

    std::ranges::transform(
      r.acls,
      std::back_inserter(entries),
      [](const kafka::acl_description& acl) {
          return security::acl_entry(
            security::acl_principal::from_string(acl.principal),
            kafka::details::to_acl_host(acl.host),
            kafka::details::to_acl_operation(acl.operation),
            kafka::details::to_acl_permission(acl.permission_type));
      });

    std::vector<security::acl_binding> bindings;
    bindings.reserve(entries.size());
    std::ranges::transform(
      entries,
      std::back_inserter(bindings),
      [&pattern](const security::acl_entry& entry) {
          return security::acl_binding(pattern, entry);
      });

    return bindings;
}

std::vector<security::acl_binding> to_acl_bindings(
  const chunked_vector<kafka::describe_acls_resource>& resources) {
    std::vector<security::acl_binding> bindings;
    bindings.reserve(resources.size());
    for (const auto& r : resources) {
        auto resource_bindings = to_acl_bindings(r);
        std::ranges::move(resource_bindings, std::back_inserter(bindings));
    }

    return bindings;
}
} // namespace

security_migrator::security_migrator(link* link, const model::metadata& config)
  : controller_locked_task(
      link,
      config.configuration.security_settings_sync_cfg.get_task_interval(),
      security_migrator::task_name)
  , _config(config.configuration.security_settings_sync_cfg.copy()) {}

void security_migrator::update_config(const model::metadata& config) {
    _config = config.configuration.security_settings_sync_cfg.copy();
    set_run_interval(
      config.configuration.security_settings_sync_cfg.get_task_interval());
}

model::enabled_t security_migrator::is_enabled() const {
    return _config.is_enabled;
}

ss::future<task::state_transition>
security_migrator::run_impl(ss::abort_source& as) {
    vlog(logger().trace, "Running security migrator task");
    constexpr auto acl_creation_timeout = 5s;

    if (_config.acl_filters.empty()) {
        vlog(logger().debug, "No ACL filters configured, skipping task");
        co_return state_transition{
          .desired_state = model::task_state::active,
          .reason = "No ACL filters configured, skipping task"};
    }

    auto& cluster = get_link()->get_cluster_connection();

    try {
        co_await cluster.request_metadata_update(std::nullopt);
    } catch (const std::exception& e) {
        auto msg = ssx::sformat("Failed to update metadata: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    as.check();

    if (!has_required_permissions(
          cluster.get_cluster_authorized_operations(),
          security_migrator::required_permissions)) {
        auto msg = ssx::sformat(
          "Insufficient permissions to replicate ACLs.  Requires {:08x}, has "
          "{:08x}",
          security_migrator::required_permissions,
          cluster.get_cluster_authorized_operations());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    kafka::api_version describe_acls_version;
    try {
        auto supported_api_versions = co_await cluster.supported_api_versions(
          kafka::describe_acls_api::key, as);
        if (!supported_api_versions.has_value()) {
            auto msg = ssx::sformat(
              "Failed to get supported API version for DescribeACLs");
            vlog(logger().warn, "{}", msg);
            co_return state_transition{
              .desired_state = model::task_state::link_unavailable,
              .reason = std::move(msg)};
        }

        if (
          supported_api_versions.value().min
          > kafka::describe_acls_api::max_valid) {
            auto msg = ssx::sformat(
              "Unsupported API version for DescribeACLs: {}",
              supported_api_versions.value().min);
            vlog(logger().warn, "{}", msg);
            co_return state_transition{
              .desired_state = model::task_state::link_unavailable,
              .reason = std::move(msg)};
        }

        describe_acls_version = std::min(
          supported_api_versions.value().max,
          kafka::describe_acls_api::max_valid);
        vlog(
          logger().debug,
          "Using describe_acls version: {}",
          describe_acls_version);
    } catch (const ss::abort_requested_exception&) {
        // Rethrow abort requested to allow caller to handle it
        throw;
    } catch (const std::exception& e) {
        auto msg = ssx::sformat(
          "Failed to get supported API version for DescribeACLs: {}", e.what());
        vlog(logger().warn, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }

    as.check();
    auto acls_f = co_await ss::coroutine::as_future(
      fetch_acls(describe_acls_version));

    if (acls_f.failed()) {
        auto ex = acls_f.get_exception();
        auto level = ssx::is_shutdown_exception(ex) ? ss::log_level::trace
                                                    : ss::log_level::warn;
        auto msg = ssx::sformat("Failed to fetch ACLs: {}", ex);
        vlogl(logger(), level, "{}", msg);
        co_return state_transition{
          .desired_state = model::task_state::link_unavailable,
          .reason = std::move(msg)};
    }
    auto acls = std::move(acls_f).get();
    vlog(logger().trace, "Fetched ACLs: {}", acls);

    if (acls.empty()) {
        vlog(logger().trace, "No ACLS fetched, nothing to migrate");
        co_return state_transition{
          .desired_state = model::task_state::active,
          .reason = "Security migrator task run successfully"};
    }

    std::vector<security::acl_binding> bindings;
    try {
        bindings = to_acl_bindings(acls);
    } catch (const std::exception& e) {
        vlog(logger().warn, "Error transforming received ACLs: {}", e.what());
        co_return state_transition{
          .desired_state = model::task_state::faulted,
          .reason = ssx::sformat(
            "Error transforming received ACLs: {}", e.what())};
    }
    vlog(logger().trace, "bindings fetched from source cluster: {}", bindings);

    as.check();

    auto res = co_await get_link()->get_security_service().create_acls(
      std::move(bindings), acl_creation_timeout);

    std::ranges::for_each(res, [this](cluster::errc ec) {
        if (ec != cluster::errc::success) {
            vlog(logger().warn, "Failure during ACL creation: {}", ec);
        }
    });

    vlog(logger().trace, "Security migrator task completed");
    co_return state_transition{
      .desired_state = model::task_state::active,
      .reason = "Security migrator task completed"};
}

ss::future<chunked_vector<kafka::describe_acls_resource>>
security_migrator::fetch_acls(kafka::api_version describe_acls_version) {
    auto& cluster = get_link()->get_cluster_connection();
    chunked_vector<kafka::describe_acls_request> requests;
    requests.reserve(_config.acl_filters.size());

    std::ranges::transform(
      _config.acl_filters,
      std::back_inserter(requests),
      [](const auto& filter) {
          return kafka::describe_acls_request{
            .data = filter_to_request_data(filter)};
      });

    const auto describe_acls_request_limit = 5;

    chunked_vector<kafka::describe_acls_resource> acls;

    std::exception_ptr ex = nullptr;

    co_await ss::max_concurrent_for_each(
      requests,
      describe_acls_request_limit,
      [this, &cluster, describe_acls_version, &acls, &ex](
        kafka::describe_acls_request& req) {
          vlog(logger().trace, "Requesting ACLs: {}", req);
          return cluster.dispatch_to_any(std::move(req), describe_acls_version)
            .then([this, &acls](kafka::describe_acls_response response) {
                vlog(logger().trace, "ACL response: {}", response);
                if (response.data.error_code != kafka::error_code::none) {
                    vlog(
                      logger().warn,
                      "Failed to fetch ACLs: {}{}",
                      response.data.error_code,
                      response.data.error_message.has_value()
                        ? " (" + response.data.error_message.value() + ")"
                        : "");
                }
                acls.reserve(acls.size() + response.data.resources.size());
                std::ranges::move(
                  response.data.resources, std::back_inserter(acls));
            })
            .handle_exception([this, &ex](const std::exception_ptr& e) {
                vlog(logger().warn, "Failed to fetch ACLs: {}", e);
                if (!ex) {
                    ex = e;
                }
            });
      });

    if (ex) {
        std::rethrow_exception(ex);
    }

    co_return acls;
}

std::string_view security_migrator_factory::created_task_name() const noexcept {
    return security_migrator::task_name;
}

std::unique_ptr<task> security_migrator_factory::create_task(link* link) {
    return std::make_unique<security_migrator>(link, *(link->get_config()));
}

} // namespace cluster_link
