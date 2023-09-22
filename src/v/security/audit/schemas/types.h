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
#include "json/stringbuffer.h"
#include "seastarx.h"
#include "utils/named_type.h"
#include "version.h"

#include <seastar/core/sstring.hh>

#include <string_view>

namespace security::audit {
static constexpr std::string_view ocsf_api_version = "1.0.0";
static constexpr std::string_view vendor_name = "Redpanda Data, Inc.";

using port_t = named_type<uint16_t, struct port_t_type>;
// OCSF defines timestamp as a signed long (64-bit) value that holds
// milliseconds since Unix epoch
using timestamp_t = named_type<int64_t, struct timestamp_t_type>;
// OCSF defines type as a signed integer
using type_uid = named_type<int32_t, struct type_uid_type>;

template<typename T>
concept has_equality_fields = requires(T t) { t.equality_fields(); };

enum class category_uid : uint8_t {
    system_activity = 1,
    findings = 2,
    iam = 3,
    network_activity = 4,
    discovery = 5,
    application_activity = 6
};

enum class class_uid : uint16_t {
    file_system_activity = 1001,
    kernel_extension_activity = 1002,
    kernel_activity = 1003,
    memory_activity = 1004,
    module_activity = 1005,
    scheduled_job_activity = 1006,
    process_activity = 1007,
    security_finding = 2001,
    account_change = 3001,
    authentication = 3002,
    authorize_session = 3003,
    entity_management = 3004,
    user_access_management = 3005,
    group_management = 3006,
    network_activity = 4001,
    http_activity = 4002,
    dns_activity = 4003,
    dhcp_activity = 4004,
    rdp_activity = 4005,
    smb_activity = 4006,
    ssh_activity = 4007,
    ftp_activity = 4008,
    email_activity = 4009,
    network_file_activity = 4010,
    email_file_activity = 4011,
    email_url_activity = 4012,
    device_inventory_info = 5001,
    device_config_state = 5002,
    web_resource_activity = 6001,
    application_lifecycle = 6002,
    api_activity = 6003,
    web_resource_access_activity = 6004
};

enum class severity_id : uint8_t {
    unknown = 0,
    informational = 1,
    low = 2,
    medium = 3,
    high = 4,
    critical = 5,
    fatal = 6,
    other = 99,
};

struct api {
    ss::sstring operation;

    auto equality_fields() const { return std::tie(operation); }
};

struct product {
    ss::sstring name;
    ss::sstring vendor_name;
    ss::sstring version;

    auto equality_fields() const {
        return std::tie(name, vendor_name, version);
    }
};

static inline const product& redpanda_product() {
    static const product redpanda_product_struct{
      .name = "Redpanda",
      .vendor_name = ss::sstring{vendor_name},
      .version = ss::sstring{redpanda_git_version()}};
    return redpanda_product_struct;
}

struct metadata {
    product product;
    ss::sstring version;

    auto equality_fields() const { return std::tie(product, version); }
};

static inline const metadata& ocsf_redpanda_metadata() {
    static const metadata ocsf_metadata{
      .product = redpanda_product(), .version = ss::sstring{ocsf_api_version}};

    return ocsf_metadata;
}

struct network_endpoint {
    std::vector<ss::sstring> intermediate_ips;
    net::unresolved_address addr;
    ss::sstring name;
    ss::sstring svc_name;
    ss::sstring uid;
};

struct policy {
    ss::sstring desc;
    ss::sstring name;

    auto equality_fields() const { return std::tie(desc, name); }
};

struct authorization_result {
    ss::sstring decision;
    std::optional<policy> policy;

    auto equality_fields() const { return std::tie(decision, policy); }
};

struct user {
    enum class type : int {
        unknown = 0,
        user = 1,
        admin = 2,
        system = 3,
        other = 99
    };

    ss::sstring credential_uid;
    ss::sstring domain;
    ss::sstring name;
    type type_id;

    auto equality_fields() const {
        return std::tie(credential_uid, domain, name, type_id);
    }
};

struct actor {
    std::vector<authorization_result> authorizations;
    user user;

    auto equality_fields() const { return std::tie(authorizations, user); }
};

struct resource_detail {
    ss::sstring name;
    ss::sstring type;

    auto equality_fields() const { return std::tie(name, type); }
};

struct authorization_metadata {
    struct {
        ss::sstring host;
        ss::sstring op;
        ss::sstring permission_type;
        ss::sstring principal;
    } acl_authorization;

    struct {
        ss::sstring name;
        ss::sstring pattern;
        ss::sstring type;
    } resource;

    auto equality_fields() const {
        return std::tie(
          acl_authorization.host,
          acl_authorization.op,
          acl_authorization.permission_type,
          acl_authorization.principal,
          resource.name,
          resource.pattern,
          resource.type);
    }
};

struct api_activity_unmapped {
    ss::shard_id shard_id;
    std::optional<authorization_metadata> authorization_metadata;

    auto equality_fields() const {
        return std::tie(shard_id, authorization_metadata);
    }
};

struct http_header {
    ss::sstring name;
    ss::sstring value;

    auto equality_fields() const { return std::tie(name, value); }
};

struct uniform_resource_locator {
    ss::sstring hostname;
    ss::sstring path;
    port_t port;
    ss::sstring scheme;
    ss::sstring url_string;

    auto equality_fields() const {
        return std::tie(hostname, path, port, scheme, url_string);
    }
};

struct http_request {
    std::vector<http_header> http_headers;
    ss::sstring http_method;
    uniform_resource_locator url;
    ss::sstring user_agent;
    ss::sstring version;

    auto equality_fields() const {
        return std::tie(http_headers, http_method, url, user_agent, version);
    }
};
} // namespace security::audit

namespace json {
namespace sa = security::audit;

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::api& api) {
    w.StartObject();
    w.Key("operation");
    rjson_serialize(w, api.operation);
    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::product& p) {
    w.StartObject();
    w.Key("name");
    rjson_serialize(w, p.name);
    w.Key("vendor_name");
    rjson_serialize(w, p.vendor_name);
    w.Key("version");
    rjson_serialize(w, p.version);
    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::metadata& m) {
    w.StartObject();
    w.Key("product");
    rjson_serialize(w, m.product);
    w.Key("version");
    rjson_serialize(w, m.version);
    w.EndObject();
}

inline void
rjson_serialize(Writer<StringBuffer>& w, const sa::network_endpoint& n) {
    w.StartObject();
    if (!n.intermediate_ips.empty()) {
        w.Key("intermediate_ips");
        rjson_serialize(w, n.intermediate_ips);
    }
    w.Key("ip");
    rjson_serialize(w, n.addr.host());
    if (!n.name.empty()) {
        w.Key("name");
        rjson_serialize(w, n.name);
    }
    w.Key("port");
    rjson_serialize(w, n.addr.port());
    if (!n.svc_name.empty()) {
        w.Key("svc_name");
        rjson_serialize(w, n.svc_name);
    }
    if (!n.uid.empty()) {
        w.Key("uid");
        rjson_serialize(w, n.uid);
    }
    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::policy& p) {
    w.StartObject();
    w.Key("desc");
    rjson_serialize(w, p.desc);
    w.Key("name");
    rjson_serialize(w, p.name);
    w.EndObject();
}

inline void rjson_serialize(
  Writer<StringBuffer>& w, const sa::authorization_result& authz) {
    w.StartObject();
    w.Key("decision");
    rjson_serialize(w, authz.decision);
    if (authz.policy) {
        w.Key("policy");
        rjson_serialize(w, authz.policy);
    }

    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::user& user) {
    w.StartObject();
    if (!user.credential_uid.empty()) {
        w.Key("credential_uid");
        rjson_serialize(w, user.credential_uid);
    }
    if (!user.domain.empty()) {
        w.Key("domain");
        rjson_serialize(w, user.domain);
    }
    w.Key("name");
    rjson_serialize(w, user.name);
    w.Key("type_id");
    rjson_serialize(w, user.type_id);
    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::actor& actor) {
    w.StartObject();
    w.Key("authorizations");
    rjson_serialize(w, actor.authorizations);
    w.Key("user");
    rjson_serialize(w, actor.user);
    w.EndObject();
}

inline void
rjson_serialize(Writer<StringBuffer>& w, const sa::resource_detail& resource) {
    w.StartObject();
    w.Key("name");
    rjson_serialize(w, resource.name);
    w.Key("type");
    rjson_serialize(w, resource.type);
    w.EndObject();
}

inline void
rjson_serialize(Writer<StringBuffer>& w, const sa::authorization_metadata& m) {
    w.StartObject();
    w.Key("acl_authorization");
    w.StartObject();
    w.Key("host");
    ::json::rjson_serialize(w, m.acl_authorization.host);
    w.Key("op");
    ::json::rjson_serialize(w, m.acl_authorization.op);
    w.Key("permission_type");
    ::json::rjson_serialize(w, m.acl_authorization.permission_type);
    w.Key("principal");
    ::json::rjson_serialize(w, m.acl_authorization.principal);
    w.EndObject();
    w.Key("resource");
    w.StartObject();
    w.Key("name");
    ::json::rjson_serialize(w, m.resource.name);
    w.Key("pattern");
    ::json::rjson_serialize(w, m.resource.pattern);
    w.Key("type");
    ::json::rjson_serialize(w, m.resource.type);
    w.EndObject();
    w.EndObject();
}

inline void
rjson_serialize(Writer<StringBuffer>& w, const sa::api_activity_unmapped& u) {
    w.StartObject();
    w.Key("shard_id");
    ::json::rjson_serialize(w, u.shard_id);
    if (u.authorization_metadata) {
        w.Key("authorization_metadata");
        ::json::rjson_serialize(w, u.authorization_metadata.value());
    }
    w.EndObject();
}

inline void rjson_serialize(Writer<StringBuffer>& w, const sa::http_header& h) {
    w.StartObject();
    w.Key("name");
    json::rjson_serialize(w, h.name);
    w.Key("value");
    json::rjson_serialize(w, h.value);
    w.EndObject();
}

inline void rjson_serialize(
  Writer<StringBuffer>& w, const sa::uniform_resource_locator& url) {
    w.StartObject();
    w.Key("hostname");
    rjson_serialize(w, url.hostname);
    w.Key("path");
    rjson_serialize(w, url.path);
    w.Key("port");
    rjson_serialize(w, url.port);
    w.Key("scheme");
    rjson_serialize(w, url.scheme);
    w.Key("url_string");
    rjson_serialize(w, url.url_string);
    w.EndObject();
}

inline void
rjson_serialize(Writer<StringBuffer>& w, const sa::http_request& r) {
    w.StartObject();
    w.Key("http_headers");
    rjson_serialize(w, r.http_headers);
    w.Key("http_method");
    rjson_serialize(w, r.http_method);
    w.Key("url");
    rjson_serialize(w, r.url);
    w.Key("user_agent");
    rjson_serialize(w, r.user_agent);
    w.Key("version");
    rjson_serialize(w, r.version);
    w.EndObject();
}
} // namespace json

namespace std {

namespace sa = security::audit;

template<typename T>
struct hash<std::vector<T>> {
    size_t operator()(const std::vector<T>& v) const {
        size_t h = 0;
        for (const auto& val : v) {
            boost::hash_combine(h, std::hash<T>()(val));
        }

        return h;
    }
};

template<sa::has_equality_fields T>
struct hash<T> {
    size_t operator()(const T& val) const {
        size_t h = 0;
        std::apply(
          [&h](const auto&... arg) {
              (boost::hash_combine(
                 h, std::hash<std::remove_cvref_t<decltype(arg)>>()(arg)),
               ...);
          },
          val.equality_fields());
        return h;
    }
};
} // namespace std
