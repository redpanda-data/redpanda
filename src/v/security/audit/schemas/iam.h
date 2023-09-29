/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/json.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"

namespace security::audit {
class authentication final : public ocsf_base_event {
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

    authentication(
      activity_id activity_id,
      std::variant<auth_protocol_id, ss::sstring> auth_protocol,
      network_endpoint dst_endpoint,
      used_cleartext is_cleartext,
      used_mfa mfa,
      network_endpoint src_endpoint,
      severity_id severity_id,
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
      , _src_endpoint(std::move(src_endpoint))
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

    auto equality_fields() const {
        return std::tie(
          _activity_id,
          _auth_protocol,
          _auth_protocol_id,
          _dst_endpoint.addr.host(),
          _is_cleartext,
          _mfa,
          _src_endpoint.addr.host(),
          _user);
    }

private:
    activity_id _activity_id;
    ss::sstring _auth_protocol;
    auth_protocol_id _auth_protocol_id;
    network_endpoint _dst_endpoint;
    used_cleartext _is_cleartext;
    used_mfa _mfa;
    network_endpoint _src_endpoint;
    user _user;

    size_t hash() const final { return std::hash<authentication>()(*this); }

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
        w.Key("mfa");
        ::json::rjson_serialize(w, bool(a._mfa));
        w.Key("src_endpoint");
        ::json::rjson_serialize(w, a._src_endpoint);
        w.Key("user");
        ::json::rjson_serialize(w, a._user);
        w.EndObject();
    }
};
} // namespace security::audit
