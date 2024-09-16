/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "hashing/combine.h"
#include "security/audit/schemas/hashing_utils.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"
#include "security/audit/schemas/utils.h"
#include "security/authorizer.h"
#include "security/request_auth.h"

#include <seastar/http/handlers.hh>

namespace security::audit {
// Describe generate CRUD API activities
// https://schema.ocsf.io/1.0.0/classes/api_activity?extensions=
class api_activity final : public ocsf_base_event<api_activity> {
public:
    enum class activity_id : uint8_t {
        unknown = 0,
        create = 1,
        read = 2,
        update = 3,
        delete_id = 4,
        other = 99
    };

    enum class status_id : uint8_t {
        unknown = 0,
        success = 1,
        failure = 2,
        other = 99
    };

    api_activity(
      activity_id activity_id,
      actor actor,
      api api,
      network_endpoint dst_endpoint,
      std::optional<http_request> http_request,
      std::vector<resource_detail> resources,
      severity_id severity_id,
      network_endpoint src_endpoint,
      status_id status_id,
      timestamp_t time,
      api_activity_unmapped unmapped)
      : ocsf_base_event(
          category_uid::application_activity,
          class_uid::api_activity,
          ocsf_redpanda_metadata_cloud_profile(),
          severity_id,
          time,
          activity_id)
      , _activity_id(activity_id)
      , _actor(std::move(actor))
      , _api(std::move(api))
      , _cloud(cloud{.provider = ""})
      , _dst_endpoint(std::move(dst_endpoint))
      , _http_request(std::move(http_request))
      , _resources(std::move(resources))
      , _src_endpoint(std::move(src_endpoint))
      , _status_id(status_id)
      , _unmapped(std::move(unmapped)) {}

    template<typename T>
    static size_t hash(
      std::string_view operation_name,
      const security::auth_result& auth_result,
      const ss::socket_address& local_address,
      std::string_view service_name,
      ss::net::inet_address client_addr,
      uint16_t client_port,
      std::optional<std::string_view> client_id,
      const std::vector<T>& additional_resources) {
        size_t h = api_activity_event_base_hash(
          operation_name,
          auth_result,
          local_address,
          service_name,
          client_addr);

        hash::combine(h, client_port, client_id);

        for (const auto& v : additional_resources) {
            hash::combine(
              h, get_resource_name(v), get_audit_resource_type<T>());
            if constexpr (
              std::is_same_v<T, security::acl_binding>
              || std::is_same_v<T, security::acl_binding_filter>) {
                hash::combine(h, v);
            }
        }
        return h;
    }

    static size_t hash(
      ss::httpd::const_req req,
      const request_auth_result& auth_result,
      const ss::sstring& svc_name,
      bool authorized,
      const std::optional<std::string_view>& reason);

    static size_t hash(
      ss::httpd::const_req req,
      const ss::sstring& user,
      const ss::sstring& svc_name);

    template<typename T>
    static api_activity construct(
      std::string_view operation_name,
      const security::auth_result& auth_result,
      const ss::socket_address& local_address,
      std::string_view service_name,
      ss::net::inet_address client_addr,
      uint16_t client_port,
      std::optional<std::string_view> client_id,
      std::vector<T> additional_resources) {
        auto new_ars = create_resource_details(std::move(additional_resources));
        auto crud = op_to_crud(auth_result.operation);
        auto actor = result_to_actor(auth_result);
        new_ars.emplace_back(resource_detail{
          .name = auth_result.resource_name,
          .type = fmt::format("{}", auth_result.resource_type)});

        return {
          crud,
          std::move(actor),
          api{
            .operation = ss::sstring{operation_name},
            .service = {.name = ss::sstring{service_name}}},
          network_endpoint{
            .addr = net::unresolved_address(
              fmt::format("{}", local_address.addr()), local_address.port()),
            .svc_name = ss::sstring{service_name},
          },
          std::nullopt,
          std::move(new_ars),
          severity_id::informational,
          network_endpoint{
            .addr = net::unresolved_address(
              fmt::format("{}", client_addr), client_port),
            .name = ss::sstring{client_id.value_or("")}},
          auth_result.authorized ? api_activity::status_id::success
                                 : api_activity::status_id::failure,
          create_timestamp_t(),
          unmapped_data(auth_result)};
    }

    static api_activity construct(
      ss::httpd::const_req req,
      const request_auth_result& auth_result,
      const ss::sstring& svc_name,
      bool authorized,
      const std::optional<std::string_view>& reason);

    static api_activity construct(
      ss::httpd::const_req req,
      const ss::sstring& user,
      const ss::sstring& svc_name);

    static constexpr api_activity::activity_id
    op_to_crud(security::acl_operation op) {
        switch (op) {
        case acl_operation::read:
            return api_activity::activity_id::read;
        case acl_operation::write:
            return api_activity::activity_id::update;
        case acl_operation::create:
            return api_activity::activity_id::create;
        case acl_operation::remove:
            return api_activity::activity_id::delete_id;
        case acl_operation::alter:
            return api_activity::activity_id::update;
        case acl_operation::describe:
            return api_activity::activity_id::update;
        case acl_operation::cluster_action:
            return api_activity::activity_id::update;
        case acl_operation::describe_configs:
            return api_activity::activity_id::read;
        case acl_operation::alter_configs:
            return api_activity::activity_id::update;
        case acl_operation::idempotent_write:
            return api_activity::activity_id::update;
        case acl_operation::all:
            // The `acl_operation` passed to this function is based off of
            // the ACL check performed by the Kafka handlers.  None of the
            // handlers should be providing `all` to an ACL check.
            vassert(false, "Cannot convert an ALL acl operation to a CRUD");
        }
    }

    auto equality_fields() const {
        return std::tie(
          _activity_id,
          _actor,
          _api,
          _dst_endpoint.addr.host(),
          _http_request,
          _resources,
          _src_endpoint.addr.host(),
          _status_id,
          _unmapped);
    }

private:
    activity_id _activity_id;
    actor _actor;
    api _api;
    cloud _cloud;
    network_endpoint _dst_endpoint;
    std::optional<http_request> _http_request;
    std::vector<resource_detail> _resources;
    network_endpoint _src_endpoint;
    status_id _status_id;
    api_activity_unmapped _unmapped;

    ss::sstring api_info() const final {
        if (_http_request) {
            return _http_request->url.path;
        }
        return _api.operation;
    }

    friend inline void rjson_serialize(
      ::json::Writer<::json::StringBuffer>& w, const api_activity& a) {
        w.StartObject();
        a.rjson_serialize(w);
        w.Key("activity_id");
        ::json::rjson_serialize(w, a._activity_id);
        w.Key("actor");
        ::json::rjson_serialize(w, a._actor);
        w.Key("api");
        ::json::rjson_serialize(w, a._api);
        w.Key("cloud");
        ::json::rjson_serialize(w, a._cloud);
        w.Key("dst_endpoint");
        ::json::rjson_serialize(w, a._dst_endpoint);
        if (a._http_request) {
            w.Key("http_request");
            ::json::rjson_serialize(w, a._http_request);
        }
        if (!a._resources.empty()) {
            w.Key("resources");
            ::json::rjson_serialize(w, a._resources);
        }

        w.Key("src_endpoint");
        ::json::rjson_serialize(w, a._src_endpoint);
        w.Key("status_id");
        ::json::rjson_serialize(w, a._status_id);
        w.Key("unmapped");
        ::json::rjson_serialize(w, a._unmapped);
        w.EndObject();
    }
};

// Reports the installation, removal, start, or stop of an application or
// service
// https://schema.ocsf.io/1.0.0/classes/application_lifecycle?extensions=
class application_lifecycle final
  : public ocsf_base_event<application_lifecycle> {
public:
    enum class activity_id : uint8_t {
        unknown = 0,
        install = 1,
        remove = 2,
        start = 3,
        stop = 4,
        other = 99
    };

    application_lifecycle(
      activity_id activity_id,
      product app,
      severity_id severity_id,
      timestamp_t time)
      : ocsf_base_event(
          category_uid::application_activity,
          class_uid::application_lifecycle,
          severity_id,
          time,
          activity_id)
      , _activity_id(activity_id)
      , _app(std::move(app)) {}

    static size_t hash(
      activity_id activity_id,
      const std::optional<ss::sstring>& feature_name = std::nullopt);

    static application_lifecycle construct(
      activity_id activity_id,
      std::optional<ss::sstring> feature_name = std::nullopt);

    auto equality_fields() const { return std::tie(_activity_id, _app); }

private:
    activity_id _activity_id;
    product _app;

    friend inline void rjson_serialize(
      ::json::Writer<::json::StringBuffer>& w, const application_lifecycle& a) {
        w.StartObject();
        a.rjson_serialize(w);

        w.Key("activity_id");
        ::json::rjson_serialize(w, a._activity_id);
        w.Key("app");
        ::json::rjson_serialize(w, a._app);

        w.EndObject();
    }
};

api_activity::activity_id http_method_to_activity_id(std::string_view method);

} // namespace security::audit
