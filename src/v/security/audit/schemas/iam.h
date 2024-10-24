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

#include "json/json.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"
#include "security/request_auth.h"

#include <seastar/http/handlers.hh>

namespace security::audit {

struct authentication_event_options;

// Event that reports authentication session activities
// https://schema.ocsf.io/1.0.0/classes/authentication?extensions=
class authentication final : public ocsf_base_event<authentication> {
public:
    using used_cleartext = ss::bool_class<struct used_cleartext_tag>;
    using used_mfa = ss::bool_class<struct used_mfa_tag>;

    enum class activity_id : uint8_t {
        unknown = 0,
        logon = 1,
        logoff = 2,
        authentication_ticket = 3,
        service_ticket = 4,
        other = 99
    };

    enum class auth_protocol_id : uint8_t {
        unknown = 0,
        ntlm = 1,
        kerberos = 2,
        digest = 3,
        openid = 4,
        saml = 5,
        oauth_2_0 = 6,
        pap = 7,
        chap = 8,
        eap = 9,
        radius = 10,
        other = 99

    };
    enum class status_id : uint8_t {
        unknown = 0,
        success = 1,
        failure = 2,
        other = 99,
    };

    using auth_protocol_variant = std::variant<auth_protocol_id, ss::sstring>;

    authentication(
      activity_id activity_id,
      auth_protocol_variant auth_protocol,
      network_endpoint dst_endpoint,
      used_cleartext is_cleartext,
      used_mfa mfa,
      network_endpoint src_endpoint,
      service service,
      severity_id severity_id,
      status_id status_id,
      std::optional<ss::sstring> status_detail,
      timestamp_t time,
      user user)
      : ocsf_base_event(
          category_uid::iam,
          class_uid::authentication,
          severity_id,
          time,
          activity_id)
      , _activity_id(activity_id)
      , _dst_endpoint(std::move(dst_endpoint))
      , _is_cleartext(is_cleartext)
      , _mfa(mfa)
      , _service(std::move(service))
      , _src_endpoint(std::move(src_endpoint))
      , _status_id(status_id)
      , _status_detail(std::move(status_detail))
      , _user(std::move(user)) {
        ss::visit(
          auth_protocol,
          [this](enum auth_protocol_id auth_protocol_id) {
              this->_auth_protocol = "";
              this->_auth_protocol_id = auth_protocol_id;
          },
          [this](const ss::sstring& auth_protocol) {
              this->_auth_protocol = auth_protocol;
              this->_auth_protocol_id = auth_protocol_id::other;
          });
    }

    static size_t hash(const authentication_event_options&);

    static authentication construct(authentication_event_options);

    auto equality_fields() const {
        return std::tie(
          _activity_id,
          _auth_protocol,
          _auth_protocol_id,
          _dst_endpoint.addr.host(),
          _is_cleartext,
          _mfa,
          _service,
          _src_endpoint.addr.host(),
          _status_id,
          _status_detail,
          _user);
    }

private:
    activity_id _activity_id;
    ss::sstring _auth_protocol;
    auth_protocol_id _auth_protocol_id;
    network_endpoint _dst_endpoint;
    used_cleartext _is_cleartext;
    used_mfa _mfa;
    service _service;
    network_endpoint _src_endpoint;
    status_id _status_id;
    std::optional<ss::sstring> _status_detail;
    user _user;

    ss::sstring api_info() const final { return _user.name; }

    friend inline void rjson_serialize(
      ::json::Writer<::json::StringBuffer>& w, const authentication& a) {
        w.StartObject();
        a.rjson_serialize(w);
        w.Key("activity_id");
        ::json::rjson_serialize(w, a._activity_id);
        if (!a._auth_protocol.empty()) {
            w.Key("auth_protocol");
            ::json::rjson_serialize(w, a._auth_protocol);
        }
        w.Key("auth_protocol_id");
        ::json::rjson_serialize(w, a._auth_protocol_id);
        w.Key("dst_endpoint");
        ::json::rjson_serialize(w, a._dst_endpoint);
        w.Key("is_cleartext");
        ::json::rjson_serialize(w, bool(a._is_cleartext));
        w.Key("is_mfa");
        ::json::rjson_serialize(w, bool(a._mfa));
        w.Key("service");
        ::json::rjson_serialize(w, a._service);
        w.Key("src_endpoint");
        ::json::rjson_serialize(w, a._src_endpoint);
        w.Key("status_id");
        ::json::rjson_serialize(w, a._status_id);
        if (a._status_detail.has_value()) {
            w.Key("status_detail");
            ::json::rjson_serialize(w, a._status_detail.value());
        }
        w.Key("user");
        ::json::rjson_serialize(w, a._user);
        w.EndObject();
    }
};

struct authentication_event_options {
    std::string_view auth_protocol;

    net::unresolved_address server_addr;
    std::optional<std::string_view> svc_name;
    net::unresolved_address client_addr;
    std::optional<std::string_view> client_id;
    authentication::used_cleartext is_cleartext;
    security::audit::user user;
    std::optional<ss::sstring> error_reason;
};

} // namespace security::audit
