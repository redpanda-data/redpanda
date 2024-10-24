/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/schemas/utils.h"

#include "kafka/protocol/schemata/add_offsets_to_txn_request.h"
#include "kafka/protocol/schemata/add_partitions_to_txn_request.h"
#include "kafka/protocol/schemata/alter_client_quotas_request.h"
#include "kafka/protocol/schemata/alter_configs_request.h"
#include "kafka/protocol/schemata/alter_partition_reassignments_request.h"
#include "kafka/protocol/schemata/create_acls_request.h"
#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/delete_acls_request.h"
#include "kafka/protocol/schemata/delete_groups_request.h"
#include "kafka/protocol/schemata/delete_records_request.h"
#include "kafka/protocol/schemata/delete_topics_request.h"
#include "kafka/protocol/schemata/describe_acls_request.h"
#include "kafka/protocol/schemata/describe_client_quotas_request.h"
#include "kafka/protocol/schemata/describe_configs_request.h"
#include "kafka/protocol/schemata/describe_groups_request.h"
#include "kafka/protocol/schemata/describe_log_dirs_request.h"
#include "kafka/protocol/schemata/describe_producers_request.h"
#include "kafka/protocol/schemata/describe_transactions_request.h"
#include "kafka/protocol/schemata/end_txn_request.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/find_coordinator_request.h"
#include "kafka/protocol/schemata/heartbeat_request.h"
#include "kafka/protocol/schemata/incremental_alter_configs_request.h"
#include "kafka/protocol/schemata/init_producer_id_request.h"
#include "kafka/protocol/schemata/join_group_request.h"
#include "kafka/protocol/schemata/leave_group_request.h"
#include "kafka/protocol/schemata/list_groups_request.h"
#include "kafka/protocol/schemata/list_offset_request.h"
#include "kafka/protocol/schemata/list_partition_reassignments_request.h"
#include "kafka/protocol/schemata/list_transactions_request.h"
#include "kafka/protocol/schemata/metadata_request.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/protocol/schemata/offset_delete_request.h"
#include "kafka/protocol/schemata/offset_fetch_request.h"
#include "kafka/protocol/schemata/offset_for_leader_epoch_request.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/schemata/sync_group_request.h"
#include "kafka/protocol/schemata/txn_offset_commit_request.h"
#include "kafka/protocol/types.h"
#include "model/metadata.h"
#include "security/acl.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"
#include "security/audit/types.h"
#include "security/authorizer.h"
#include "security/gssapi_authenticator.h"
#include "security/mtls.h"
#include "security/oidc_authenticator.h"
#include "security/request_auth.h"
#include "security/scram_authenticator.h"
#include "utils/unresolved_address.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>

#include <boost/algorithm/string/predicate.hpp>

namespace security::audit {

namespace {

uniform_resource_locator
uri_from_ss_http_request(const ss::http::request& req) {
    return {
      .hostname = req.get_header("Host"),
      .path = req._url,
      .port = port_t{req.get_server_address().port()},
      .scheme = req.get_protocol_name(),
      .url_string = req.get_url()};
}

http_request from_ss_http_request(const ss::http::request& req) {
    using ss_http_headers_t = decltype(req._headers);
    using ss_http_headers_value_t = ss_http_headers_t::value_type;
    const auto get_headers = [](const ss_http_headers_t& headers) {
        const auto sanitize_header =
          [](const ss_http_headers_value_t& kv) -> ss::sstring {
            constexpr auto sensitive_headers = std::to_array<std::string_view>(
              {"Authorization", "Cookie"});

            if (
              std::find_if(
                sensitive_headers.begin(),
                sensitive_headers.end(),
                [&kv](std::string_view s) {
                    return boost::iequals(s, kv.first);
                })
              != sensitive_headers.end()) {
                return "******";
            } else {
                return kv.second;
            }
        };
        std::vector<http_header> audit_headers;
        std::transform(
          headers.begin(),
          headers.end(),
          std::back_inserter(audit_headers),
          [sanitize_header = std::move(sanitize_header)](const auto& kv) {
              return http_header{
                .name = kv.first, .value = sanitize_header(kv)};
          });
        return audit_headers;
    };
    return http_request{
      .http_headers = get_headers(req._headers),
      .http_method = req._method,
      .url = uri_from_ss_http_request(req),
      .user_agent = req.get_header("User-Agent"),
      .version = req._version};
}

network_endpoint from_ss_endpoint(
  const ss::socket_address& sa,
  std::optional<std::string_view> svc_name = std::nullopt) {
    return network_endpoint{
      .addr = net::unresolved_address(
        fmt::format("{}", sa.addr()), sa.port(), sa.addr().in_family()),
      .svc_name = ss::sstring{svc_name.value_or("")}};
}

authorization_metadata auth_result_to_authorization_metadata(
  const security::auth_result& auth_result) {
    struct authorization_metadata metadata;

    metadata.acl_authorization.permission_type = auth_result.authorized
                                                   ? "AUTHORIZED"
                                                   : "DENIED";

    if (auth_result.acl.has_value()) {
        auto& acl = auth_result.acl.value();
        metadata.acl_authorization.principal = fmt::format(
          "{}", acl.get().principal());
        metadata.acl_authorization.host = fmt::format("{}", acl.get().host());
        metadata.acl_authorization.op = fmt::format(
          "{}", acl.get().operation());
        metadata.acl_authorization.permission_type = fmt::format(
          "{}", acl.get().permission());
    }

    if (auth_result.resource_pattern) {
        auto& resource_pattern = auth_result.resource_pattern.value();
        metadata.resource.name = resource_pattern.get().name();
        metadata.resource.pattern = fmt::format(
          "{}", resource_pattern.get().pattern());
        metadata.resource.type = fmt::format(
          "{}", resource_pattern.get().resource());
    }

    return metadata;
}

user user_from_request_auth_result(const request_auth_result& r) {
    auto& username = r.get_username();

    return {
      .name = username.empty() ? "{{anonymous}}" : username,
      .type_id = r.is_authenticated()
                   ? (r.is_superuser() ? user::type::admin : user::type::user)
                   : user::type::unknown,
    };
}

actor actor_from_request_auth_result(
  const request_auth_result& r,
  bool authorized,
  const ss::sstring& svc_name,
  const std::optional<std::string_view>& reason) {
    auto u = user_from_request_auth_result(r);
    std::vector<authorization_result> auths{
      {.decision = authorized ? "authorized" : "denied",
       .policy = policy{
         .desc = ss::sstring{reason.value_or(
           r.is_auth_required() ? "" : "Auth Disabled")},
         .name = svc_name}}};

    return {.authorizations = std::move(auths), .user = std::move(u)};
}

actor actor_from_user_string(
  const ss::sstring& user, const ss::sstring& svc_name) {
    struct user u = {
      .name = user.empty() ? "{{anonymous}}" : user,
      .type_id = user.empty() ? user::type::unknown : user::type::user,
    };
    std::vector<authorization_result> auths{
      {.decision = "authorized",
       .policy = policy{
         .desc = user.empty() ? "Auth Disabled" : "", .name = svc_name}}};

    return {.authorizations = std::move(auths), .user = std::move(u)};
}

authentication::auth_protocol_variant
mechanism_string_to_auth_protocol(std::string_view mech) {
    if (mech.empty()) {
        return authentication::auth_protocol_id::unknown;
    }
    return string_switch<authentication::auth_protocol_variant>(mech)
      .match(
        security::scram_sha256_authenticator::name,
        security::scram_sha256_authenticator::name)
      .match(
        security::scram_sha512_authenticator::name,
        security::scram_sha512_authenticator::name)
      .match(
        security::gssapi_authenticator::name,
        authentication::auth_protocol_id::kerberos)
      .match(
        security::oidc::sasl_authenticator::name,
        authentication::auth_protocol_id::oauth_2_0)
      .default_match(ss::sstring{mech});
}

} // namespace

/// TODO: Via ACLs metadata return correct response
api_activity_unmapped unmapped_data() { return api_activity_unmapped{}; }

api_activity_unmapped unmapped_data(const security::auth_result& auth_result) {
    return api_activity_unmapped{
      .authorization_metadata = auth_result_to_authorization_metadata(
        auth_result)};
}

actor result_to_actor(const security::auth_result& result) {
    user user{
      .name = result.principal.name(),
      .type_id = result.is_superuser ? user::type::admin : user::type::user,
      .groups = result.role ? std::vector<group>{group{
                                .type = group::type_id::role,
                                .name = result.role.value()}}
                            : std::vector<group>{}};

    policy policy;
    policy.name = "aclAuthorization";

    if (result.authorization_disabled) {
        policy.desc = "authorization disabled";
    } else if (result.is_superuser) {
        policy.desc = "superuser";
    } else if (result.empty_matches) {
        policy.desc = "no matches";
    } else if (result.acl.has_value() || result.resource_pattern.has_value()) {
        ss::sstring desc;
        if (result.acl.has_value()) {
            desc += fmt::format("acl: {}", result.acl.value());
        }
        if (result.resource_pattern.has_value()) {
            if (!desc.empty()) {
                desc += ", ";
            }
            desc += fmt::format(
              "resource: {}", result.resource_pattern.value());
        }

        policy.desc = std::move(desc);
    }

    std::vector<authorization_result> auths{
      {.decision = result.authorized ? "authorized" : "denied",
       .policy = std::move(policy)}};

    return {.authorizations = std::move(auths), .user = std::move(user)};
}

api_activity::activity_id http_method_to_activity_id(std::string_view method) {
    return string_switch<api_activity::activity_id>(method)
      .match_all("POST", "post", api_activity::activity_id::create)
      .match_all("GET", "get", api_activity::activity_id::read)
      .match_all("PUT", "put", api_activity::activity_id::update)
      .match_all("DELETE", "delete", api_activity::activity_id::delete_id)
      .default_match(api_activity::activity_id::unknown);
}

std::ostream& operator<<(std::ostream& os, audit_resource_type type) {
    switch (type) {
    case audit_resource_type::topic:
        return os << "topic";
    case audit_resource_type::group:
        return os << "group";
    case audit_resource_type::cluster:
        return os << "cluster";
    case audit_resource_type::transactional_id:
        return os << "transactional_id";
    case audit_resource_type::acl_binding:
        return os << "acl_binding";
    case audit_resource_type::acl_binding_filter:
        return os << "acl_binding_filter";
    }
}

std::ostream& operator<<(std::ostream& os, const category_uid& uid) {
    switch (uid) {
    case category_uid::system_activity:
        return os << "system_activity";
    case category_uid::findings:
        return os << "findings";
    case category_uid::iam:
        return os << "iam";
    case category_uid::network_activity:
        return os << "network_activity";
    case category_uid::discovery:
        return os << "discovery";
    case category_uid::application_activity:
        return os << "application_activity";
    }
}

std::ostream& operator<<(std::ostream& os, const class_uid& uid) {
    switch (uid) {
    case class_uid::file_system_activity:
        return os << "file_system_activity";
    case class_uid::kernel_extension_activity:
        return os << "kernel_extension_activity";
    case class_uid::kernel_activity:
        return os << "kernel_activity";
    case class_uid::memory_activity:
        return os << "memory_activity";
    case class_uid::module_activity:
        return os << "module_activity";
    case class_uid::scheduled_job_activity:
        return os << "scheduled_job_activity";
    case class_uid::process_activity:
        return os << "process_activity";
    case class_uid::security_finding:
        return os << "security_finding";
    case class_uid::account_change:
        return os << "account_change";
    case class_uid::authentication:
        return os << "authentication";
    case class_uid::authorize_session:
        return os << "authorize_session";
    case class_uid::entity_management:
        return os << "entity_management";
    case class_uid::user_access_management:
        return os << "user_access_management";
    case class_uid::group_management:
        return os << "group_management";
    case class_uid::network_activity:
        return os << "network_activity";
    case class_uid::http_activity:
        return os << "http_activity";
    case class_uid::dns_activity:
        return os << "dns_activity";
    case class_uid::dhcp_activity:
        return os << "dhcp_activity";
    case class_uid::rdp_activity:
        return os << "rdp_activity";
    case class_uid::smb_activity:
        return os << "smb_activity";
    case class_uid::ssh_activity:
        return os << "ssh_activity";
    case class_uid::ftp_activity:
        return os << "ftp_activity";
    case class_uid::email_activity:
        return os << "email_activity";
    case class_uid::network_file_activity:
        return os << "network_file_activity";
    case class_uid::email_file_activity:
        return os << "email_file_activity";
    case class_uid::email_url_activity:
        return os << "email_url_activity";
    case class_uid::device_inventory_info:
        return os << "device_inventory_info";
    case class_uid::device_config_state:
        return os << "device_config_state";
    case class_uid::web_resource_activity:
        return os << "web_resource_activity";
    case class_uid::application_lifecycle:
        return os << "application_lifecycle";
    case class_uid::api_activity:
        return os << "api_activity";
    case class_uid::web_resource_access_activity:
        return os << "web_resource_access_activity";
    }
}

std::ostream& operator<<(std::ostream& os, const ocsf_base_impl& impl) {
    return os << "{category: " << impl.get_category_uid()
              << ", class: " << impl.get_class_uid()
              << ", type_uid: " << impl.get_type_uid()()
              << ", detail: " << impl.api_info() << "}";
}

event_type kafka_api_to_event_type(kafka::api_key key) {
    switch (key) {
    case kafka::alter_configs_api::key:
        return event_type::management;
    case kafka::alter_partition_reassignments_api::key:
        return event_type::management;
    case kafka::create_acls_api::key:
        return event_type::management;
    case kafka::create_partitions_api::key:
        return event_type::management;
    case kafka::create_topics_api::key:
        return event_type::management;
    case kafka::delete_acls_api::key:
        return event_type::management;
    case kafka::delete_groups_api::key:
        return event_type::management;
    case kafka::delete_records_api::key:
        return event_type::management;
    case kafka::delete_topics_api::key:
        return event_type::management;
    case kafka::incremental_alter_configs_api::key:
        return event_type::management;
    case kafka::offset_delete_api::key:
        return event_type::management;
    case kafka::alter_client_quotas_api::key:
        return event_type::management;
    case kafka::add_partitions_to_txn_api::key:
        return event_type::produce;
    case kafka::end_txn_api::key:
        return event_type::produce;
    case kafka::init_producer_id_api::key:
        return event_type::produce;
    case kafka::produce_api::key:
        return event_type::produce;
    case kafka::add_offsets_to_txn_api::key:
        return event_type::consume;
    case kafka::fetch_api::key:
        return event_type::consume;
    case kafka::join_group_api::key:
        return event_type::consume;
    case kafka::leave_group_api::key:
        return event_type::consume;
    case kafka::list_offsets_api::key:
        return event_type::consume;
    case kafka::offset_commit_api::key:
        return event_type::consume;
    case kafka::offset_fetch_api::key:
        return event_type::consume;
    case kafka::sync_group_api::key:
        return event_type::consume;
    case kafka::txn_offset_commit_api::key:
        return event_type::consume;
    case kafka::describe_acls_api::key:
        return event_type::describe;
    case kafka::describe_configs_api::key:
        return event_type::describe;
    case kafka::describe_groups_api::key:
        return event_type::describe;
    case kafka::describe_log_dirs_api::key:
        return event_type::describe;
    case kafka::find_coordinator_api::key:
        return event_type::describe;
    case kafka::list_groups_api::key:
        return event_type::describe;
    case kafka::list_partition_reassignments_api::key:
        return event_type::describe;
    case kafka::metadata_api::key:
        return event_type::describe;
    case kafka::offset_for_leader_epoch_api::key:
        return event_type::describe;
    case kafka::describe_producers_api::key:
        return event_type::describe;
    case kafka::describe_transactions_api::key:
        return event_type::describe;
    case kafka::list_transactions_api::key:
        return event_type::describe;
    case kafka::describe_client_quotas_api::key:
        return event_type::describe;
    case kafka::heartbeat_api::key:
        return event_type::heartbeat;
    }

    // this method should only be used by handlers or the audit system while an
    // event is being handled meaning if we have reached this spot, there is a
    // bug
    vassert(false, "Unhandled Kafka API in kafka_api_to_event_type: {}", key);
}

api_activity api_activity::construct(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  const ss::sstring& svc_name,
  bool authorized,
  const std::optional<std::string_view>& reason) {
    auto act = actor_from_request_auth_result(
      auth_result, authorized, svc_name, reason);
    return {
      http_method_to_activity_id(req._method),
      std::move(act),
      api{.operation = req._method, .service = {.name = svc_name}},
      from_ss_endpoint(req.get_server_address(), svc_name),
      from_ss_http_request(req),
      {},
      severity_id::informational,
      from_ss_endpoint(req.get_client_address()),
      authorized ? api_activity::status_id::success
                 : api_activity::status_id::failure,
      create_timestamp_t(),
      unmapped_data()};
}

api_activity api_activity::construct(
  ss::httpd::const_req req,
  const ss::sstring& user,
  const ss::sstring& svc_name) {
    auto act = actor_from_user_string(user, svc_name);
    return {
      http_method_to_activity_id(req._method),
      std::move(act),
      api{.operation = req._method, .service = {.name = svc_name}},
      from_ss_endpoint(req.get_server_address(), svc_name),
      from_ss_http_request(req),
      {},
      severity_id::informational,
      from_ss_endpoint(req.get_client_address()),
      api_activity::status_id::success,
      create_timestamp_t(),
      unmapped_data()};
}

application_lifecycle application_lifecycle::construct(
  application_lifecycle::activity_id activity_id,
  std::optional<ss::sstring> feature_name) {
    auto product = redpanda_product();
    if (feature_name) {
        product.feature = feature{.name = std::move(*feature_name)};
    }
    product.uid = ss::to_sstring(
      config::node().node_id().value_or(model::node_id{0}));

    return {
      activity_id,
      std::move(product),
      severity_id::informational,
      create_timestamp_t()};
}

authentication authentication::construct(authentication_event_options options) {
    auto result = options.error_reason.has_value()
                    ? authentication::status_id::failure
                    : authentication::status_id::success;
    return {
      authentication::activity_id::logon,
      mechanism_string_to_auth_protocol(options.auth_protocol),
      network_endpoint{
        .addr = std::move(options.server_addr),
        .svc_name = ss::sstring{options.svc_name.value_or("")},
      },
      options.is_cleartext,
      authentication::used_mfa::no,
      network_endpoint{
        .addr = std::move(options.client_addr),
        .name = ss::sstring{options.client_id.value_or("")}},
      service{.name = ss::sstring{options.svc_name.value_or("")}},
      severity_id::informational,
      result,
      std::move(options.error_reason),
      create_timestamp_t(),
      std::move(options.user)};
}

} // namespace security::audit
