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
#include "absl/container/flat_hash_set.h"
#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "config/broker_authn_endpoint.h"
#include "config/sasl_mechanisms.h"
#include "features/enterprise_feature_messages.h"
#include "json/document.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "kafka/client/config_utils.h"
#include "kafka/client/configuration.h"
#include "kafka/server/server.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/schema_registry/api.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "redpanda/admin/api-doc/security.json.hh"
#include "redpanda/admin/server.h"
#include "security/credential_store.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_service.h"
#include "security/request_auth.h"
#include "security/role_store.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"
#include "security/types.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/http/url.hh>
#include <seastar/json/json_elements.hh>

#include <algorithm>
#include <optional>
#include <sstream>

namespace seastar::httpd::security_json {
struct interfaces_report : public json::json_base {
    json::json_list<kafka_interface_security_report> kafka;
    json::json_element<rpc_interface_security_report> rpc;
    json::json_list<admin_interface_security_report> admin;
    json::json_list<schema_registry_interface_security_report> schema_registry;
    json::json_element<client_security_report> schema_registry_client;
    json::json_list<pandaproxy_interface_security_report> pandaproxy;
    json::json_element<client_security_report> audit_log_client;

    void register_params() {
        add(&kafka, "kafka");
        add(&rpc, "rpc");
        add(&admin, "admin");
        add(&schema_registry, "schema_registry");
        add(&schema_registry_client, "schema_registry_client");
        add(&pandaproxy, "pandaproxy");
        add(&audit_log_client, "audit_log_client");
    }

    interfaces_report() { register_params(); }

    interfaces_report(const interfaces_report& e) {
        register_params();
        kafka = e.kafka;
        rpc = e.rpc;
        admin = e.admin;
        schema_registry = e.schema_registry;
        schema_registry_client = e.schema_registry_client;
        pandaproxy = e.pandaproxy;
        audit_log_client = e.audit_log_client;
    }
    template<class T>
    interfaces_report& operator=(const T& e) {
        kafka = e.kafka;
        rpc = e.rpc;
        admin = e.admin;
        schema_registry = e.schema_registry;
        schema_registry_client = e.schema_registry_client;
        pandaproxy = e.pandaproxy;
        audit_log_client = e.audit_log_client;
        return *this;
    }
    interfaces_report& operator=(const interfaces_report& e) {
        kafka = e.kafka;
        rpc = e.rpc;
        admin = e.admin;
        schema_registry = e.schema_registry;
        schema_registry_client = e.schema_registry_client;
        pandaproxy = e.pandaproxy;
        audit_log_client = e.audit_log_client;
        return *this;
    }
    template<class T>
    interfaces_report& update(T& e) {
        e.kafka = kafka;
        e.rpc = rpc;
        e.admin = admin;
        e.schema_registry = schema_registry;
        e.schema_registry_client = schema_registry_client;
        e.pandaproxy = pandaproxy;
        e.audit_log_client = audit_log_client;
        return *this;
    }
};
struct security_report : public json::json_base {
    json::json_element<interfaces_report> interfaces;
    json::json_list<security_report_alert> alerts;

    void register_params() {
        add(&interfaces, "interfaces");
        add(&alerts, "alerts");
    }
    security_report() { register_params(); }
    security_report(const security_report& e) {
        register_params();
        interfaces = e.interfaces;
        alerts = e.alerts;
    }
    template<class T>
    security_report& operator=(const T& e) {
        interfaces = e.interfaces;
        alerts = e.alerts;
        return *this;
    }
    security_report& operator=(const security_report& e) {
        interfaces = e.interfaces;
        alerts = e.alerts;
        return *this;
    }
    template<class T>
    security_report& update(T& e) {
        e.interfaces = interfaces;
        e.alerts = alerts;
        return *this;
    }
};
} // namespace seastar::httpd::security_json

namespace {

// TODO: factor out generic serialization from seastar http exceptions
security::scram_credential parse_scram_credential(const json::Document& doc) {
    if (!doc.IsObject()) {
        throw ss::httpd::bad_request_exception(fmt::format("Not an object"));
    }

    if (!doc.HasMember("algorithm") || !doc["algorithm"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String algo missing"));
    }
    const auto algorithm = std::string_view(
      doc["algorithm"].GetString(), doc["algorithm"].GetStringLength());
    validate_no_control(
      algorithm, admin_server::string_conversion_exception{"algorithm"});

    if (!doc.HasMember("password") || !doc["password"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String password is missing"));
    }
    const auto password = doc["password"].GetString();
    validate_no_control(
      password, admin_server::string_conversion_exception{"password"});

    const ss::sstring ss_password{password};

    if (crypto::is_scram_password_too_short(ss_password)) {
        throw ss::httpd::bad_request_exception(
          fmt::format(
            "Password length less than {} characters",
            crypto::hmac_key_fips_min_bytes));
    }

    security::scram_credential credential;

    if (algorithm == security::scram_sha256_authenticator::name) {
        credential = security::scram_sha256::make_credentials(
          ss_password, security::scram_sha256::min_iterations);

    } else if (algorithm == security::scram_sha512_authenticator::name) {
        credential = security::scram_sha512::make_credentials(
          ss_password, security::scram_sha512::min_iterations);

    } else {
        throw ss::httpd::bad_request_exception(
          fmt::format("Unknown scram algorithm: {}", algorithm));
    }

    return credential;
}

bool match_scram_credential(
  const json::Document& doc, const security::scram_credential& creds) {
    // Document is pre-validated via earlier parse_scram_credential call
    const auto password = ss::sstring(doc["password"].GetString());
    const auto algorithm = std::string_view(
      doc["algorithm"].GetString(), doc["algorithm"].GetStringLength());
    validate_no_control(
      algorithm, admin_server::string_conversion_exception{"algorithm"});

    if (algorithm == security::scram_sha256_authenticator::name) {
        return security::scram_sha256::validate_password(
          password, creds.stored_key(), creds.salt(), creds.iterations());
    } else if (algorithm == security::scram_sha512_authenticator::name) {
        return security::scram_sha512::validate_password(
          password, creds.stored_key(), creds.salt(), creds.iterations());
    } else {
        throw ss::httpd::bad_request_exception(
          fmt::format("Unknown scram algorithm: {}", algorithm));
    }
}

bool is_no_op_user_write(
  security::credential_store& store,
  security::credential_user username,
  security::scram_credential credential) {
    auto user_opt = store.get<security::scram_credential>(username);
    if (user_opt.has_value()) {
        return user_opt.value() == credential;
    } else {
        return false;
    }
}

enum class role_errc {
    malformed_def = 40001,
    invalid_name = 40002,
    unrecognized_field = 40003,
    member_list_conflict = 40004,
    role_not_found = 40401,
    role_already_exists = 40901,
    role_name_conflict = 40902,
};

// NOTE(oren): bogus -Wunneeded-internal-declaration here from clang-tidy (?)
std::ostream& operator<<(std::ostream& os, role_errc code) {
    switch (code) {
    case role_errc::malformed_def:
        return os << "Malformed request";
    case role_errc::invalid_name:
        return os << "Invalid role name";
    case role_errc::unrecognized_field:
        return os << "Unrecognized field";
    case role_errc::member_list_conflict:
        return os << "Conflict between 'add' and 'remove' lists";
    case role_errc::role_not_found:
        return os << "Role not found";
    case role_errc::role_already_exists:
        return os << "Role already exists";
    case role_errc::role_name_conflict:
        return os << "Role name conflict";
    }
    __builtin_unreachable();
}

ss::http::reply::status_type role_errc_to_status(role_errc c) {
    return ss::http::reply::status_type{static_cast<int>(c) / 100};
}

ss::httpd::security_json::role_member
role_member_to_json(const security::role_member& m) {
    ss::httpd::security_json::role_member j_member;
    j_member.name = ss::sstring{m.name()};
    j_member.principal_type = static_cast<
      ss::httpd::security_json::role_member::role_member_principal_type>(
      m.type());
    return j_member;
}

} // namespace

namespace json {
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const ss::httpd::security_json::rbac_error_body& v) {
    w.StartObject();
    w.Key("message");
    w.String(v.message());
    w.Key("code");
    w.Uint(v.code());
    w.EndObject();
}
} // namespace json

namespace {
std::string
role_errc_to_json(role_errc e, std::optional<std::string_view> msg) {
    ss::httpd::security_json::rbac_error_body body;
    body.code = static_cast<int>(e);
    if (msg.has_value()) {
        body.message = fmt::format("{}: {}", e, msg.value());
    } else {
        body.message = fmt::format("{}", e);
    }

    json::StringBuffer sb;
    json::Writer<json::StringBuffer> writer(sb);
    using ::json::rjson_serialize;
    rjson_serialize(writer, body);
    return {sb.GetString(), sb.GetSize()};
}

void throw_role_exception(
  role_errc ec, std::optional<std::string_view> msg = std::nullopt) {
    throw ss::httpd::base_exception(
      role_errc_to_json(ec, msg), role_errc_to_status(ec));
}

void throw_on_role_command_err(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        switch (cluster::errc(ec.value())) {
        case cluster::errc::role_does_not_exist:
            throw_role_exception(role_errc::role_not_found);
            break;
        case cluster::errc::role_exists:
            throw_role_exception(role_errc::role_already_exists);
            break;
        default:
            break;
        }
    }
}

absl::flat_hash_set<security::role_member>
parse_json_members_list(const json::Document& doc, std::string_view key) {
    bool has_key = doc.HasMember(key.data());

    if (!has_key) {
        return {};
    } else if (!doc[key.data()].IsArray()) {
        throw_role_exception(
          role_errc::malformed_def, fmt::format("Array '{}' missing.", key));
    }

    std::vector<security::role_member> result;
    const auto& mem_arr = doc[key.data()].GetArray();
    result.reserve(mem_arr.Size());
    std::ranges::transform(
      mem_arr,
      std::back_inserter(result),
      [](const auto& p) -> security::role_member {
          if (!p.IsObject()) {
              throw_role_exception(
                role_errc::malformed_def,
                fmt::format("Role member is not a JSON object"));
          }
          if (!p.HasMember("name") || !p["name"].IsString()) {
              throw_role_exception(
                role_errc::malformed_def,
                fmt::format("String 'name' missing from role_member"));
          }
          if (
            !p.HasMember("principal_type") || !p["principal_type"].IsString()) {
              throw_role_exception(
                role_errc::malformed_def,
                fmt::format(
                  "String 'principal_type' missing from role_member"));
          }

          ss::sstring p_type{p["principal_type"].GetString()};
          ss::sstring name{p["name"].GetString()};
          if (p_type != "User") {
              throw_role_exception(
                role_errc::malformed_def,
                fmt::format(
                  "Role membership reserved for user principals, got {{{}:{}}}",
                  p_type,
                  name));
          }
          return {security::role_member_type::user, p["name"].GetString()};
      });
    return {result.begin(), result.end()};
}

security::role_name parse_role_name(const ss::http::request& req) {
    ss::sstring role_v = req.get_path_param("role");
    if (role_v == "") {
        vlog(adminlog.debug, "Invalid parameter 'role' got {}", role_v);
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'role' got {{{}}}", req.get_path_param("role"))};
    }
    return security::role_name(role_v);
}

security::role_name parse_role_definition(const json::Document& doc) {
    if (!doc.IsObject()) {
        vlog(adminlog.debug, "Request body is not a JSON object");
        throw_role_exception(
          role_errc::malformed_def, "Request body is not a JSON object");
    }

    if (!doc.HasMember("role") || !doc["role"].IsString()) {
        vlog(adminlog.debug, "String 'role' missing from request body");
        throw_role_exception(
          role_errc::malformed_def, "Missing string field 'role'");
    }

    auto role_name = security::role_name{doc["role"].GetString()};
    validate_no_control(
      role_name(), admin_server::string_conversion_exception{"role"});

    if (!security::validate_scram_username(role_name())) {
        throw_role_exception(role_errc::invalid_name);
    }

    return role_name;
}

template<class T>
requires std::is_base_of_v<ss::json::jsonable, std::remove_cvref_t<T>>
inline std::unique_ptr<ss::http::reply> make_json_response(
  std::unique_ptr<ss::http::reply> rep,
  ss::http::reply::status_type status,
  T&& body) {
    rep->set_status(status);
    rep->write_body(
      "json", ss::json::formatter::to_json(std::forward<T>(body)));
    return rep;
}

bool parse_bool_nocase(ss::sstring param) {
    bool result = false;
    std::ranges::transform(param, param.begin(), ::tolower);
    std::istringstream(param) >> std::boolalpha >> result;
    return result;
}

} // namespace

void admin_server::register_security_routes() {
    register_route<superuser>(
      ss::httpd::security_json::create_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return create_user_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::delete_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return delete_user_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::update_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return update_user_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::security_json::oidc_whoami,
      [this](std::unique_ptr<ss::http::request> req) {
          return oidc_whoami_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::oidc_keys_cache_invalidate,
      [this](std::unique_ptr<ss::http::request> req) {
          return oidc_keys_cache_invalidate_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::oidc_revoke,
      [this](std::unique_ptr<ss::http::request> req) {
          return oidc_revoke_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::list_users,
      [this](std::unique_ptr<ss::http::request> req) {
          bool include_ephemeral = req->get_query_param("include_ephemeral")
                                   == "true";

          auto pred = [include_ephemeral](const auto& c) {
              return include_ephemeral
                     || security::credential_store::is_not_ephemeral(c);
          };
          auto creds = _controller->get_credential_store().local().range(pred);

          std::vector<ss::sstring> users{};
          users.reserve(std::distance(creds.begin(), creds.end()));
          for (const auto& [user, type] : creds) {
              users.push_back(user());
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(users));
      });

    // RBAC stubs

    register_route<user, true>(
      ss::httpd::security_json::list_user_roles,
      [this](
        std::unique_ptr<ss::http::request> req,
        const request_auth_result& auth_result)
        -> ss::future<ss::json::json_return_type> {
          return list_user_roles_handler(std::move(req), auth_result);
      });

    register_route<superuser>(
      ss::httpd::security_json::list_roles,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return list_roles_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::create_role,
      request_handler_fn{[this](auto req, auto reply) {
          check_license(features::enterprise_error_message::acl_with_rbac());
          return create_role_handler(std::move(req), std::move(reply));
      }});

    register_route<superuser>(
      ss::httpd::security_json::get_role,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return get_role_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::delete_role,
      request_handler_fn{[this](auto req, auto reply) {
          return delete_role_handler(std::move(req), std::move(reply));
      }});

    register_route<superuser>(
      ss::httpd::security_json::list_role_members,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto role_name = parse_role_name(*req);
          auto role = _controller->get_role_store().local().get(role_name);
          if (!role.has_value()) {
              vlog(adminlog.debug, "Role '{}' does not exist", role_name);
              throw_role_exception(role_errc::role_not_found);
          }
          ss::httpd::security_json::role_members_list j_res;
          for (const auto& mem : role.value().members()) {
              j_res.members.push(role_member_to_json(mem));
          }

          return ssx::now(ss::json::json_return_type(j_res));
      });

    register_route<superuser>(
      ss::httpd::security_json::update_role_members,
      [this]([[maybe_unused]] std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          check_license(features::enterprise_error_message::acl_with_rbac());
          return update_role_members_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::get_security_report,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return get_security_report(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::create_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto doc = co_await parse_json_body(req.get());

    auto credential = parse_scram_credential(doc);

    if (!doc.HasMember("username") || !doc["username"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String username missing"));
    }

    auto username = security::credential_user(doc["username"].GetString());
    validate_no_control(username(), string_conversion_exception{"username"});

    if (!security::validate_scram_username(username())) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Invalid SCRAM username {{{}}}", username()));
    }

    if (
      is_no_op_user_write(
        _controller->get_credential_store().local(), username, credential)) {
        vlog(
          adminlog.debug,
          "User {} already exists with matching credential",
          username);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().create_user(
        username, credential, model::timeout_clock::now() + 5s);
    vlog(
      adminlog.debug, "Creating user '{}' {}:{}", username, err, err.message());

    if (err == cluster::errc::user_exists) {
        // Idempotency: if user is same as one that already exists,
        // suppress the user_exists error and return success.
        const auto& credentials_store
          = _controller->get_credential_store().local();
        std::optional<security::scram_credential> creds
          = credentials_store.get<security::scram_credential>(username);
        if (creds.has_value() && match_scram_credential(doc, creds.value())) {
            co_return ss::json::json_return_type(ss::json::json_void());
        }
    }

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::delete_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring user_v = req->get_path_param("user");
    if (user_v == "") {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'user' got {{{}}}", req->get_path_param("user"))};
    }
    auto user = security::credential_user(user_v);

    if (!_controller->get_credential_store().local().contains(user)) {
        vlog(adminlog.debug, "User '{}' already gone during deletion", user);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().delete_user(
        user, model::timeout_clock::now() + 5s);
    vlog(adminlog.debug, "Deleting user '{}' {}:{}", user, err, err.message());
    if (err == cluster::errc::user_does_not_exist) {
        // Idempotency: removing a non-existent user is successful.
        co_return ss::json::json_return_type(ss::json::json_void());
    }
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::update_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring user_v = req->get_path_param("user");
    if (user_v == "") {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'user' got {{{}}}", req->get_path_param("user"))};
    }
    auto user = security::credential_user(user_v);

    auto doc = co_await parse_json_body(req.get());

    auto credential = parse_scram_credential(doc);

    if (
      is_no_op_user_write(
        _controller->get_credential_store().local(), user, credential)) {
        vlog(
          adminlog.debug,
          "User {} already exists with matching credential",
          user);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().update_user(
        user, credential, model::timeout_clock::now() + 5s);
    vlog(adminlog.debug, "Updating user {}:{}", err, err.message());
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::oidc_whoami_handler(std::unique_ptr<ss::http::request> req) {
    auto auth_hdr = req->get_header("authorization");
    if (!auth_hdr.starts_with(authz_bearer_prefix)) {
        throw ss::httpd::base_exception{
          "Invalid Authorization header",
          ss::http::reply::status_type::unauthorized};
    }

    security::oidc::authenticator auth{_controller->get_oidc_service().local()};
    auto res = auth.authenticate(auth_hdr.substr(authz_bearer_prefix.length()));

    if (res.has_error()) {
        throw ss::httpd::base_exception{
          "Invalid Authorization header",
          ss::http::reply::status_type::unauthorized};
    }

    ss::httpd::security_json::oidc_whoami_response j_res{};
    j_res.id = res.assume_value().principal.name();
    j_res.expire = res.assume_value().expiry.time_since_epoch() / 1s;

    co_return ss::json::json_return_type(j_res);
}

ss::future<ss::json::json_return_type>
admin_server::oidc_keys_cache_invalidate_handler(
  std::unique_ptr<ss::http::request>) {
    auto f = co_await ss::coroutine::as_future(
      _controller->get_oidc_service().invoke_on_all(
        [](auto& s) { return s.refresh_keys(); }));
    if (f.failed()) {
        ss::httpd::security_json::oidc_keys_cache_invalidate_error_response res;
        res.error_message = ssx::sformat("", f.get_exception());
        co_return ss::json::json_return_type(res);
    }
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::oidc_revoke_handler(std::unique_ptr<ss::http::request>) {
    auto f = co_await ss::coroutine::as_future(
      _controller->get_oidc_service().invoke_on_all(
        [](auto& s) { return s.refresh_keys(); }));
    if (f.failed()) {
        ss::httpd::security_json::oidc_keys_cache_invalidate_error_response res;
        res.error_message = ssx::sformat("", f.get_exception());
        co_return ss::json::json_return_type(res);
    }
    co_await _kafka_server.invoke_on_all([](kafka::server& ks) {
        return ks.revoke_credentials(security::oidc::sasl_authenticator::name);
    });
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type> admin_server::list_user_roles_handler(
  std::unique_ptr<ss::http::request> req,
  const request_auth_result& auth_result) {
    ss::sstring filter = req->get_query_param("filter");

    security::role_member member{
      security::role_member_type::user, auth_result.get_username()};

    auto rng = _controller->get_role_store().local().range(
      [&filter, &member](const auto& e) {
          return security::role_store::has_member(e, member)
                 && security::role_store::name_prefix_filter(e, filter);
      });

    ss::httpd::security_json::roles_list body;
    std::for_each(rng.begin(), rng.end(), [&body](const auto& rn) {
        ss::httpd::security_json::role_description j_desc;
        j_desc.name = ss::sstring{rn()};
        body.roles.push(j_desc);
    });
    co_return ss::json::json_return_type(body);
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::create_role_handler(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    auto doc = co_await parse_json_body(req.get());
    auto role_name = parse_role_definition(doc);

    ss::httpd::security_json::role_definition j_res;
    j_res.role = role_name();

    security::role role{};
    auto err
      = co_await _controller->get_security_frontend().local().create_role(
        role_name, role, model::timeout_clock::now() + 5s);

    if (err == cluster::errc::role_exists) {
        // Idempotency: if the empty role already exists,
        // suppress the role_exists error and return success.
        if (_controller->get_role_store().local().get(role_name) == role) {
            co_return make_json_response(
              std::move(rep), ss::http::reply::status_type::created, j_res);
        } else {
            throw_role_exception(role_errc::role_already_exists);
        }
    }
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return make_json_response(
      std::move(rep), ss::http::reply::status_type::created, j_res);
}

ss::future<ss::json::json_return_type>
admin_server::update_role_members_handler(
  std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring role_v = req->get_path_param("role");
    if (role_v == "") {
        vlog(
          adminlog.debug,
          "Invalid parameter 'role' got {{{}}}",
          req->get_path_param("role"));
        throw_role_exception(role_errc::invalid_name);
    }

    bool create_if_not_found = parse_bool_nocase(
      req->get_query_param("create"));

    auto doc = co_await parse_json_body(req.get());
    if (!doc.IsObject()) {
        vlog(adminlog.debug, "Request body is not a JSON object");
        throw_role_exception(
          role_errc::malformed_def, "Request body is not a JSON object");
    }

    auto role_name = security::role_name(std::move(role_v));
    auto add = parse_json_members_list(doc, "add");
    auto remove = parse_json_members_list(doc, "remove");
    if (std::ranges::any_of(remove, [&add](auto m) {
            return add.contains(m);
        })) {
        throw_role_exception(role_errc::member_list_conflict);
    }

    auto curr_members = _controller->get_role_store()
                          .local()
                          .get(role_name)
                          .value_or(security::role{})
                          .members();

    // Members diff accounting for the response body
    absl::erase_if(
      add, [&curr_members](const auto& m) { return curr_members.contains(m); });

    absl::erase_if(remove, [&curr_members](const auto& m) {
        return !curr_members.contains(m);
    });

    ss::httpd::security_json::role_member_update_response j_res;
    j_res.role = role_name();
    j_res.created = false;

    std::ranges::for_each(add, [&curr_members, &j_res](const auto& a) {
        curr_members.insert(a);
        j_res.added.push(role_member_to_json(a));
    });

    std::ranges::for_each(remove, [&curr_members, &j_res](const auto& r) {
        curr_members.erase(r);
        j_res.removed.push(role_member_to_json(r));
    });

    auto err
      = co_await _controller->get_security_frontend().local().update_role(
        role_name,
        security::role{curr_members},
        model::timeout_clock::now() + 5s);
    if (err == cluster::errc::role_does_not_exist && create_if_not_found) {
        j_res.created = true;
        err = co_await _controller->get_security_frontend().local().create_role(
          role_name,
          security::role{std::move(curr_members)},
          model::timeout_clock::now() + 5s);
    }

    throw_on_role_command_err(err);

    vlog(
      adminlog.debug,
      "{} role '{}' {}:{}",
      j_res.created() ? "Creating" : "Updating",
      role_name(),
      err,
      err.message());

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(j_res);
}

ss::future<ss::json::json_return_type>
admin_server::list_roles_handler(std::unique_ptr<ss::http::request> req) {
    auto filter = req->get_query_param("filter");
    auto user = req->get_query_param("principal");
    auto principal_type = req->get_query_param("principal_type");

    if (!principal_type.empty() && principal_type != "User") {
        throw_role_exception(
          role_errc::malformed_def,
          fmt::format(
            "Role membership reserved for user principals, got {{{}}}",
            principal_type));
    }

    auto pred = [&filter, &user](const auto& role_entry) {
        auto name_prefix_matches = filter.empty()
                                   || security::role_store::name_prefix_filter(
                                     role_entry, filter);
        auto role_has_user = user.empty()
                             || security::role_store::has_member(
                               role_entry,
                               security::role_member{
                                 security::role_member_type::user, user});
        return name_prefix_matches && role_has_user;
    };

    auto roles = _controller->get_role_store().local().range(pred);

    ss::httpd::security_json::roles_list j_res{};
    for (const auto& role_name : roles) {
        ss::httpd::security_json::role_description j_desc;
        j_desc.name = ss::sstring{role_name};
        j_res.roles.push(j_desc);
    }

    return ss::make_ready_future<ss::json::json_return_type>(j_res);
}

ss::future<ss::json::json_return_type>
admin_server::get_role_handler(std::unique_ptr<ss::http::request> req) {
    auto role_name = parse_role_name(*req);
    auto role = _controller->get_role_store().local().get(role_name);
    if (!role.has_value()) {
        vlog(adminlog.debug, "Role '{}' does not exist", role_name);
        throw_role_exception(role_errc::role_not_found);
    }

    ss::httpd::security_json::role j_res;
    j_res.name = role_name();
    for (const auto& member : role.value().members()) {
        j_res.members.push(role_member_to_json(member));
    }
    co_return ss::json::json_return_type(j_res);
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::delete_role_handler(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto role_name = parse_role_name(*req);
    bool delete_acls = parse_bool_nocase(req->get_query_param("delete_acls"));

    auto err
      = co_await _controller->get_security_frontend().local().delete_role(
        role_name, model::timeout_clock::now() + 5s);
    if (err == cluster::errc::role_does_not_exist) {
        // Idempotency: removing a non-existent user is successful.
        vlog(
          adminlog.debug, "Role '{}' already gone during deletion", role_name);
        co_return make_json_response(
          std::move(rep),
          ss::http::reply::status_type::no_content,
          ss::json::json_void{});
    }

    co_await throw_on_error(*req, err, model::controller_ntp);

    if (delete_acls) {
        security::acl_binding_filter role_binding_filter{
          security::resource_pattern_filter::any(),
          security::acl_entry_filter{
            security::role::to_principal(role_name()),
            std::nullopt,
            std::nullopt,
            std::nullopt}};

        auto results
          = co_await _controller->get_security_frontend().local().delete_acls(
            {std::move(role_binding_filter)}, 5s);

        size_t n_deleted = 0;
        size_t n_failed = 0;
        for (const auto& r : results) {
            if (r.error == cluster::errc::success) {
                n_deleted += 1;
            } else {
                n_failed += 1;
                auto ec = make_error_code(r.error);
                vlog(
                  adminlog.warn,
                  "Error while deleting ACLs for {} - {}:{}",
                  role_name,
                  ec,
                  ec.message());
            }
        }

        vlog(
          adminlog.debug,
          "Deleted {} ACL bindings for role {} ({} failed)",
          n_deleted,
          role_name,
          n_failed);
    }

    co_return make_json_response(
      std::move(rep),
      ss::http::reply::status_type::no_content,
      seastar::json::json_void{});
}

namespace {

using kafka_authn_method
  = ss::httpd::security_json::kafka_interface_security_report::
    kafka_interface_security_report_authentication_method;

using affected_interface = ss::httpd::security_json::security_report_alert::
  security_report_alert_affected_interface;

using alert_issue = ss::httpd::security_json::security_report_alert::
  security_report_alert_issue;

using pp_authn_method
  = ss::httpd::security_json::pandaproxy_interface_security_report::
    pandaproxy_interface_security_report_configured_authentication_method;

using client_authn_method = ss::httpd::security_json::client_security_report::
  client_security_report_configured_authentication_method;

kafka_authn_method to_report_type(config::broker_authn_method m) {
    switch (m) {
    case config::broker_authn_method::none:
        return kafka_authn_method::None;
    case config::broker_authn_method::sasl:
        return kafka_authn_method::SASL;
    case config::broker_authn_method::mtls_identity:
        return kafka_authn_method::mTLS;
    }

    __builtin_unreachable();
}

ss::sstring get_listener_name(ss::sstring name) {
    static const ss::sstring unnamed_interface = "{{unnamed}}";
    if (name.empty()) {
        return unnamed_interface;
    }
    return name;
}

ss::httpd::security_json::security_report_alert make_interface_alert(
  const affected_interface interface_type,
  const alert_issue issue,
  std::optional<ss::sstring> listener_name = std::nullopt) {
    listener_name = std::move(listener_name).transform(get_listener_name);

    ss::httpd::security_json::security_report_alert alert;
    alert.affected_interface = interface_type;
    if (listener_name.has_value()) {
        alert.listener_name = listener_name.value();
    }
    alert.issue = issue;

    const auto make_description = [&alert,
                                   &listener_name](std::string_view msg) {
        return ssx::sformat(
          "{} interface{}{}. This is insecure and not recommended.",
          alert.affected_interface().to_json(),
          listener_name.has_value()
            ? ss::format(" \"{}\"", listener_name.value())
            : "",
          msg);
    };

    switch (issue) {
    case alert_issue::NO_TLS:
        alert.description = make_description(" is not using TLS");
        break;
    case alert_issue::NO_AUTHN:
        alert.description = make_description(" is not using authentication");
        break;
    case alert_issue::NO_AUTHZ:
        alert.description = make_description(" is not using authorization");
        break;
    case alert_issue::SASL_PLAIN:
        alert.description = make_description(" is using SASL/PLAIN");
        break;
    case alert_issue::PP_CONFIGURED_CLIENT:
        alert.description = make_description(
          ", authorization is not enabled and the pandaproxy client has scram "
          "credentials");
        break;
    default:
        vassert(
          false, "make_interface_alert should not be called on this value");
    }

    return alert;
}

// Empty lists are normally ignored by ss::json. By setting the _set variable
// they will be serialized even if empty.
template<typename T>
void force_list(seastar::json::json_list<T>& jlist) {
    jlist._set = true;
}

template<typename Report, typename Endpoint>
void set_report_advertised(
  Report& report,
  const ss::sstring& name,
  const std::vector<Endpoint>& advertised_api) {
    auto it = std::ranges::find(advertised_api, name, &Endpoint::name);
    if (it != advertised_api.end()) {
        report.advertised_host = it->address.host();
        report.advertised_port = it->address.port();
    } else {
        report.advertised_host = report.host;
        report.advertised_port = report.port;
    }
}

template<typename Report, typename TlsConfig>
void set_report_tls(
  Report& report,
  const ss::sstring& name,
  const std::vector<TlsConfig>& tls_interfaces) {
    auto it = std::ranges::find(tls_interfaces, name, &TlsConfig::name);
    if (it != tls_interfaces.end()) {
        report.tls_enabled = it->config.is_enabled();
        report.mutual_tls_enabled = it->config.get_require_client_auth();
    } else {
        report.tls_enabled = false;
        report.mutual_tls_enabled = false;
    }
}

template<typename Report>
void set_report_http_authentication(
  Report& report, const bool& is_authn_enabled) {
    force_list(report.authentication_methods);

    if (!is_authn_enabled) {
        return;
    }

    for (auto& meth : config::shard_local_cfg().http_authentication()) {
        report.authentication_methods.push(meth);
    }
}

std::vector<ss::httpd::security_json::kafka_interface_security_report>
generate_kafka_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts) {
    std::vector<ss::httpd::security_json::kafka_interface_security_report>
      reports;
    const auto& kafka_interfaces = config::node().kafka_api();

    reports.reserve(kafka_interfaces.size());

    for (const auto& kface : kafka_interfaces) {
        ss::httpd::security_json::kafka_interface_security_report report;
        report.name = kface.name;
        report.host = kface.address.host();
        report.port = kface.address.port();

        set_report_advertised(
          report, kface.name, config::node().advertised_kafka_api_property()());

        set_report_tls(report, kface.name, config::node().kafka_api_tls());
        if (!report.tls_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::kafka, alert_issue::NO_TLS, kface.name));
        }

        auto authn_method = config::get_authn_method(kface.name);
        report.authentication_method = to_report_type(authn_method);
        if (report.authentication_method().v == kafka_authn_method::None) {
            alerts.push_back(make_interface_alert(
              affected_interface::kafka, alert_issue::NO_AUTHN, kface.name));
        }

        if (authn_method == config::broker_authn_method::sasl) {
            std::vector<ss::sstring> sasl_mechs = config::get_sasl_mechanisms(
              kface.name);

            if (std::ranges::contains(sasl_mechs, "PLAIN")) {
                alerts.push_back(make_interface_alert(
                  affected_interface::kafka,
                  alert_issue::SASL_PLAIN,
                  kface.name));
            }

            report.supported_sasl_mechanisms = sasl_mechs;
        }

        report.authorization_enabled = config::kafka_authz_enabled();
        if (!report.authorization_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::kafka, alert_issue::NO_AUTHZ, kface.name));
        }

        reports.emplace_back(std::move(report));
    }

    return reports;
}

ss::httpd::security_json::rpc_interface_security_report
generate_rpc_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts) {
    ss::httpd::security_json::rpc_interface_security_report report;

    const auto& rpc_server = config::node().rpc_server();
    const auto& rpc_advertised = config::node().advertised_rpc_api();

    report.host = rpc_server.host();
    report.port = rpc_server.port();
    report.advertised_host = rpc_advertised.host();
    report.advertised_port = rpc_advertised.port();

    report.tls_enabled = config::node().rpc_server_tls().is_enabled();
    report.mutual_tls_enabled
      = config::node().rpc_server_tls().get_require_client_auth();

    if (!report.tls_enabled()) {
        alerts.push_back(
          make_interface_alert(affected_interface::rpc, alert_issue::NO_TLS));
    }

    return report;
}

std::vector<ss::httpd::security_json::admin_interface_security_report>
generate_admin_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts) {
    std::vector<ss::httpd::security_json::admin_interface_security_report>
      reports;
    const auto& admin_interfaces = config::node().admin();

    reports.reserve(admin_interfaces.size());

    for (const auto& iface : admin_interfaces) {
        ss::httpd::security_json::admin_interface_security_report report;
        report.name = iface.name;
        report.host = iface.address.host();
        report.port = iface.address.port();

        set_report_tls(report, iface.name, config::node().admin_api_tls());
        if (!report.tls_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::admin, alert_issue::NO_TLS, iface.name));
        }

        const auto require_auth
          = config::shard_local_cfg().admin_api_require_auth();

        report.authorization_enabled = require_auth;
        if (!report.authorization_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::admin, alert_issue::NO_AUTHZ, iface.name));
        }

        set_report_http_authentication(report, require_auth);
        if (report.authentication_methods._elements.empty()) {
            alerts.push_back(make_interface_alert(
              affected_interface::admin, alert_issue::NO_AUTHN, iface.name));
        }

        reports.emplace_back(std::move(report));
    }

    return reports;
}

std::vector<ss::httpd::security_json::pandaproxy_interface_security_report>
generate_pandaproxy_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts,
  const pandaproxy::rest::configuration& config,
  const kafka::client::configuration& client_config) {
    std::vector<ss::httpd::security_json::pandaproxy_interface_security_report>
      reports;
    const auto& pp_interfaces = config.pandaproxy_api();

    reports.reserve(pp_interfaces.size());

    for (const auto& iface : pp_interfaces) {
        ss::httpd::security_json::pandaproxy_interface_security_report report;
        report.name = iface.name;
        report.host = iface.address.host();
        report.port = iface.address.port();

        set_report_advertised(
          report, iface.name, config.advertised_pandaproxy_api());

        set_report_tls(report, iface.name, config.pandaproxy_api_tls());
        if (!report.tls_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::pandaproxy, alert_issue::NO_TLS, iface.name));
        }

        const auto authn = iface.authn_method.value_or(
          config::rest_authn_method::none);
        // If rest_authn_method is not none, then the actual authentication
        // method is being read from http_authentication cluster config
        const bool is_authn_enabled = authn
                                      == config::rest_authn_method::http_basic;

        const auto get_pp_auth_method = [is_authn_enabled, &client_config]() {
            if (is_authn_enabled) {
                return pp_authn_method::SCRAM_Proxied;
            }

            const auto kclient_configured = is_scram_configured(client_config);
            if (kclient_configured) {
                return pp_authn_method::SCRAM_Configured;
            }

            return pp_authn_method::None;
        };
        const auto config_authn_method = get_pp_auth_method();
        report.configured_authentication_method = config_authn_method;

        // For pp, authn kinda implies authz as well.
        report.authorization_enabled = is_authn_enabled;
        if (!report.authorization_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::pandaproxy,
              alert_issue::NO_AUTHZ,
              iface.name));
        }

        if (config_authn_method == pp_authn_method::SCRAM_Configured) {
            alerts.push_back(make_interface_alert(
              affected_interface::pandaproxy,
              alert_issue::PP_CONFIGURED_CLIENT,
              iface.name));
        }

        set_report_http_authentication(report, is_authn_enabled);
        if (report.authentication_methods._elements.empty()) {
            alerts.push_back(make_interface_alert(
              affected_interface::pandaproxy,
              alert_issue::NO_AUTHN,
              iface.name));
        }

        reports.emplace_back(std::move(report));
    }

    return reports;
}

std::vector<ss::httpd::security_json::schema_registry_interface_security_report>
generate_schema_registry_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts,
  const pandaproxy::schema_registry::configuration& config) {
    std::vector<
      ss::httpd::security_json::schema_registry_interface_security_report>
      reports;
    const auto& sr_interfaces = config.schema_registry_api();

    reports.reserve(sr_interfaces.size());

    for (const auto& iface : sr_interfaces) {
        ss::httpd::security_json::schema_registry_interface_security_report
          report;
        report.name = iface.name;
        report.host = iface.address.host();
        report.port = iface.address.port();

        set_report_tls(report, iface.name, config.schema_registry_api_tls());
        if (!report.tls_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::schema_registry,
              alert_issue::NO_TLS,
              iface.name));
        }

        const auto authn = iface.authn_method.value_or(
          config::rest_authn_method::none);
        // If rest_authn_method is not none, then the actual authentication
        // method is being red from http_authentication cluster config
        const bool is_authn_enabled = authn
                                      == config::rest_authn_method::http_basic;

        report.authorization_enabled
          = config::shard_local_cfg().schema_registry_enable_authorization()
            && is_authn_enabled;
        if (!report.authorization_enabled()) {
            alerts.push_back(make_interface_alert(
              affected_interface::schema_registry,
              alert_issue::NO_AUTHZ,
              iface.name));
        }

        set_report_http_authentication(report, is_authn_enabled);
        if (report.authentication_methods._elements.empty()) {
            alerts.push_back(make_interface_alert(
              affected_interface::schema_registry,
              alert_issue::NO_AUTHN,
              iface.name));
        }

        reports.emplace_back(std::move(report));
    }

    return reports;
}

using ephemeral_credentials = ss::bool_class<struct ephemeral_credentials_tag>;

client_authn_method get_kclient_auth(
  const kafka::client::configuration& config, ephemeral_credentials ephemeral) {
    if (ephemeral) {
        return client_authn_method::SCRAM_Ephemeral;
    }
    if (is_scram_configured(config)) {
        return client_authn_method::SCRAM_Configured;
    }
    return client_authn_method::None;
}

ss::httpd::security_json::client_security_report
generate_kafka_client_interface_report(
  std::vector<ss::httpd::security_json::security_report_alert>& alerts,
  const affected_interface interface,
  const kafka::client::configuration& config,
  ephemeral_credentials ephemeral = ephemeral_credentials::no) {
    ss::httpd::security_json::client_security_report report;

    const ss::sstring name = config.client_identifier().value_or("");
    report.kafka_listener_name = name;

    const auto& brokers = config.brokers();

    std::vector<ss::httpd::security_json::host_port> json_brokers;
    json_brokers.reserve(brokers.size());
    for (const auto& broker : brokers) {
        ss::httpd::security_json::host_port hp;
        hp.host = broker.host();
        hp.port = broker.port();
        json_brokers.push_back(std::move(hp));
    }
    report.brokers = json_brokers;

    const auto& broker_tls = config.broker_tls();
    report.tls_enabled = broker_tls.is_enabled();
    report.mutual_tls_enabled = broker_tls.get_require_client_auth();

    if (!report.tls_enabled()) {
        alerts.push_back(
          make_interface_alert(interface, alert_issue::NO_TLS, name));
    }

    report.configured_authentication_method = get_kclient_auth(
      config, ephemeral);

    if (
      report.configured_authentication_method().v
      == ss::httpd::security_json::client_security_report::
        client_security_report_configured_authentication_method::None) {
        alerts.push_back(
          make_interface_alert(interface, alert_issue::NO_AUTHN, name));
    }

    return report;
}

} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_security_report(std::unique_ptr<ss::http::request>) {
    ss::httpd::security_json::security_report report;
    ss::httpd::security_json::interfaces_report interfaces_report;
    std::vector<ss::httpd::security_json::security_report_alert> alerts;

    interfaces_report.kafka = generate_kafka_interface_report(alerts);
    interfaces_report.rpc = generate_rpc_interface_report(alerts);
    interfaces_report.admin = generate_admin_interface_report(alerts);
    if (_http_proxy) {
        interfaces_report.pandaproxy = generate_pandaproxy_interface_report(
          alerts, _http_proxy->get_config(), _http_proxy->get_client_config());
    }
    if (_schema_registry) {
        interfaces_report.schema_registry
          = generate_schema_registry_interface_report(
            alerts, _schema_registry->get_config());
        interfaces_report
          .schema_registry_client = generate_kafka_client_interface_report(
          alerts,
          affected_interface::schema_registry_client,
          _schema_registry->get_client_config(),
          ephemeral_credentials{_schema_registry->has_ephemeral_credentials()});
    }
    if (
      config::shard_local_cfg().audit_enabled()
      && !config::shard_local_cfg().audit_use_rpc()) {
        interfaces_report.audit_log_client
          = generate_kafka_client_interface_report(
            alerts,
            affected_interface::audit_log_client,
            _audit_mgr.local().get_client_config(),
            ephemeral_credentials::yes);
    }
    report.interfaces = interfaces_report;

    const auto min_secure_tls = config::tls_version::v1_2;
    if (config::shard_local_cfg().tls_min_version < min_secure_tls) {
        ss::httpd::security_json::security_report_alert alert;
        alert.issue = alert_issue::INSECURE_MIN_TLS_VERSION;
        alert.description = ssx::sformat(
          "TLS minimum version is set to {} which is less than {}. This is "
          "insecure and not recommended.",
          config::shard_local_cfg().tls_min_version,
          min_secure_tls);
        alerts.push_back(std::move(alert));
    }

    if (config::shard_local_cfg().tls_enable_renegotiation()) {
        ss::httpd::security_json::security_report_alert alert;
        alert.issue = alert_issue::TLS_RENEGOTIATION;
        alert.description = ssx::sformat(
          "TLS renegotiation is enabled. This is insecure and not "
          "recommended.");
        alerts.push_back(std::move(alert));
    }

    report.alerts = alerts;

    return ss::make_ready_future<ss::json::json_return_type>(std::move(report));
}
