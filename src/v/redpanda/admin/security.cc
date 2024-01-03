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
#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "kafka/server/server.h"
#include "redpanda/admin/api-doc/security.json.hh"
#include "redpanda/admin/server.h"
#include "security/credential_store.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_service.h"
#include "security/role_store.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/url.hh>

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
      algorithm, admin_server::string_conversion_exception{algorithm});

    if (!doc.HasMember("password") || !doc["password"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String password smissing"));
    }
    const auto password = doc["password"].GetString();
    validate_no_control(
      password, admin_server::string_conversion_exception{"PASSWORD"});

    security::scram_credential credential;

    if (algorithm == security::scram_sha256_authenticator::name) {
        credential = security::scram_sha256::make_credentials(
          password, security::scram_sha256::min_iterations);

    } else if (algorithm == security::scram_sha512_authenticator::name) {
        credential = security::scram_sha512::make_credentials(
          password, security::scram_sha512::min_iterations);

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
      algorithm, admin_server::string_conversion_exception{algorithm});

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

absl::flat_hash_set<security::role_member>
parse_members_list(const json::Document& doc, const char* key) {
    if (!doc.IsObject()) {
        throw ss::httpd::bad_request_exception(fmt::format("Not an object"));
    }
    bool has_key = doc.HasMember(key);

    if (!has_key) {
        return {};
    } else if (!doc[key].IsArray()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Array '{}' missing.", key));
    }

    std::vector<security::role_member> result;
    result.reserve(doc[key].GetArray().Size());
    // TODO(oren): could be a std::transform
    for (auto& p : doc[key].GetArray()) {
        if (!p.IsObject()) {
            throw ss::httpd::bad_request_exception(
              fmt::format("Expected array[role_member]"));
        }
        if (!p.HasMember("name") || !p["name"].IsString()) {
            throw ss::httpd::bad_request_exception(
              fmt::format("String 'name' missing from role_member"));
        }
        if (!p.HasMember("principal_type") || !p["principal_type"].IsString()) {
            throw ss::httpd::bad_request_exception(
              fmt::format("String 'principal_type' missing from role_member"));
        }

        ss::sstring p_type{p["principal_type"].GetString()};
        ss::sstring name{p["name"].GetString()};
        if (p_type != "User") {
            throw ss::httpd::bad_request_exception(fmt::format(
              "Role membership reserved for user principals, got {{{}:{}}}",
              p_type,
              name));
        }
        result.emplace_back(
          security::role_member_type::user, p["name"].GetString());
    }
    return {result.begin(), result.end()};
}

template<class Store, class KeyT, class ValT>
requires requires(Store s, KeyT key, ValT val) {
    { s.template get<ValT>(key) } -> std::convertible_to<std::optional<ValT>>;
    { val == val } -> std::convertible_to<bool>;
}
bool is_no_op_write(Store& store, KeyT key, ValT val) {
    auto val_opt = store.template get<ValT>(key);
    if (val_opt.has_value()) {
        return val_opt.value() == val;
    } else {
        return false;
    }
}

bool is_no_op_user_write(
  security::credential_store& store,
  security::credential_user username,
  security::scram_credential credential) {
    return is_no_op_write(store, std::move(username), std::move(credential));
    // auto user_opt = store.get<security::scram_credential>(username);
    // if (user_opt.has_value()) {
    //     return user_opt.value() == credential;
    // } else {
    //     return false;
    // }
}

bool is_no_op_role_write(
  security::role_store& store,
  security::role_name role_name,
  security::role role) {
    return is_no_op_write(store, std::move(role_name), std::move(role));
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

          auto pred = [include_ephemeral](auto const& c) {
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

    // TODO(oren): see whoami?
    register_route<user>(
      ss::httpd::security_json::list_user_roles,
      []([[maybe_unused]] std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          throw ss::httpd::base_exception{
            "Not Implemented", ss::http::reply::status_type::not_implemented};
      });

    register_route<superuser>(
      ss::httpd::security_json::list_roles,
      [this](std::unique_ptr<ss::http::request> req) {
          auto filter = req->get_query_param("filter");
          auto user = req->get_query_param("user");

          auto pred = [&filter, &user](const auto& role_entry) {
              return security::role_store::name_prefix_filter(
                       role_entry, filter)
                     && security::role_store::has_member(
                       role_entry,
                       security::role_member{
                         security::role_member_type::user, user});
          };

          // TODO(oren): this might all be better or more easily  accomplished
          // with std::ranges or at least constructing the json response in the
          // monadic/pipeline style or whatever
          auto roles = _controller->get_role_store().local().range(pred);

          ss::httpd::security_json::roles_list j_res{};
          for (const auto& [name, _] : roles) {
              ss::httpd::security_json::role_description j_desc;
              j_desc.name = name();
              j_res.roles.push(j_desc);
          }

          return ss::make_ready_future<ss::json::json_return_type>(j_res);
      });

    register_route<superuser>(
      ss::httpd::security_json::create_role,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return create_role_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::get_role,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          ss::sstring role_v;
          if (!ss::http::internal::url_decode(req->param["role"], role_v)) {
              throw ss::httpd::bad_param_exception{fmt::format(
                "Invalid parameter 'role' got {{{}}}", req->param["role"])};
          }
          auto role_name = security::role_name{role_v};

          auto role = _controller->get_role_store()
                        .local()
                        .template get<security::role>(role_name);

          if (!role.has_value()) {
              vlog(adminlog.debug, "Role '{}' does not exist", role_name);
              throw ss::httpd::not_found_exception{
                fmt::format("Role '{}' not found", role_name)};
          }
          ss::httpd::security_json::role j_res;
          j_res.name = role_name();
          for (const auto& mem : role.value().members()) {
              ss::httpd::security_json::role_member j_member;
              j_member.name = mem.name();
              // TODO(oren): discern from mem.type(), with a operator<<
              j_member.principal_type = "User";
              j_res.members.push(j_member);
          }
          co_return ss::json::json_return_type(j_res);
      });

    register_route<superuser>(
      ss::httpd::security_json::update_role,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return update_role_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::delete_role,
      [this]([[maybe_unused]] std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return delete_role_handler(std::move(req));
      });

    // TODO(oren): this is nearly line-for-line identical to get_role
    // can/should collapse into a helper function
    register_route<superuser>(
      ss::httpd::security_json::list_role_members,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          ss::sstring role_v;
          if (!ss::http::internal::url_decode(req->param["role"], role_v)) {
              throw ss::httpd::bad_param_exception{fmt::format(
                "Invalid parameter 'role' got {{{}}}", req->param["role"])};
          }
          auto role_name = security::role_name{role_v};

          auto role = _controller->get_role_store()
                        .local()
                        .template get<security::role>(role_name);

          if (!role.has_value()) {
              vlog(adminlog.debug, "Role '{}' does not exist", role_name);
              throw ss::httpd::not_found_exception{
                fmt::format("Role '{}' not found", role_name)};
          }
          ss::httpd::security_json::role_members_list j_res;
          for (const auto& mem : role.value().members()) {
              ss::httpd::security_json::role_member j_member;
              j_member.name = mem.name();
              j_member.principal_type = "User";
              j_res.members.push(j_member);
          }
          co_return ss::json::json_return_type(j_res);
      });

    register_route<superuser>(
      ss::httpd::security_json::update_role_members,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return update_role_members(std::move(req));
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
    validate_no_control(username(), string_conversion_exception{username()});

    if (!security::validate_scram_username(username())) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Invalid SCRAM username {{{}}}", username()));
    }

    if (is_no_op_user_write(
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

    ss::sstring user_v;
    if (!ss::http::internal::url_decode(req->param["user"], user_v)) {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'user' got {{{}}}", req->param["user"])};
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

    ss::sstring user_v;
    if (!ss::http::internal::url_decode(req->param["user"], user_v)) {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'user' got {{{}}}", req->param["user"])};
    }
    auto user = security::credential_user(user_v);

    auto doc = co_await parse_json_body(req.get());

    auto credential = parse_scram_credential(doc);

    if (is_no_op_user_write(
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
  std::unique_ptr<ss::http::request> req) {
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
admin_server::oidc_revoke_handler(std::unique_ptr<ss::http::request> req) {
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

ss::future<ss::json::json_return_type>
admin_server::create_role_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto doc = co_await parse_json_body(req.get());

    if (!doc.HasMember("role") || !doc["role"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String role missing"));
    }

    auto role_name = security::role_name{doc["role"].GetString()};
    validate_no_control(role_name(), string_conversion_exception{role_name()});

    // TODO(oren): separate validation for role names
    if (!security::validate_scram_username(role_name())) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Invalid role name {{{}}}", role_name()));
    }

    ss::httpd::security_json::role_definition j_res;
    j_res.role = role_name();

    if (is_no_op_role_write(
          _controller->get_role_store().local(), role_name, security::role{})) {
        vlog(adminlog.debug, "Empty role {} already exists", role_name);
        co_return ss::json::json_return_type(j_res);
    }

    auto err
      = co_await _controller->get_security_frontend().local().create_role(
        role_name, security::role{}, model::timeout_clock::now() + 5s);

    if (err == cluster::errc::role_exists) {
        auto role = _controller->get_role_store().local().get<security::role>(
          role_name);
        if (role.has_value() && role.value().members().empty()) {
            co_return ss::json::json_return_type(j_res);
        }
    }

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(j_res);
}

ss::future<ss::json::json_return_type>
admin_server::update_role_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring role_v;
    if (!ss::http::internal::url_decode(req->param["role"], role_v)) {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'role' got {{{}}}", req->param["role"])};
    }
    auto role_name = security::role_name(role_v);

    auto doc = co_await parse_json_body(req.get());

    // TODO(oren): error codes? how to apply?
    if (!doc.HasMember("role") || !doc["role"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String role missing"));
    }

    // TODO(oren): reject improperly formed configs

    auto new_role_name = security::role_name{doc["role"].GetString()};
    validate_no_control(
      new_role_name(), string_conversion_exception{new_role_name()});

    auto role
      = _controller->get_role_store().local().template get<security::role>(
        role_name);

    ss::httpd::security_json::role_definition j_res;
    j_res.role = new_role_name();

    if (
      role.has_value()
      && is_no_op_role_write(
        _controller->get_role_store().local(), new_role_name, role.value())) {
        vlog(adminlog.debug, "Identical role {} already exists", new_role_name);
        // TODO(oren): delete old role
        co_return ss::json::json_return_type(j_res);
    }

    auto err
      = co_await _controller->get_security_frontend().local().rename_role(
        role_name, new_role_name, model::timeout_clock::now() + 5s);

    vlog(adminlog.debug, "Updating role {}:{}", err, err.message());
    // TODO(oren): check errors (I think we're covered)
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(j_res);
}

ss::future<ss::json::json_return_type>
admin_server::delete_role_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring role_v;
    if (!ss::http::internal::url_decode(req->param["role"], role_v)) {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'role' got {{{}}}", req->param["role"])};
    }
    auto role_name = security::role_name(role_v);

    if (!_controller->get_role_store().local().contains(role_name)) {
        vlog(
          adminlog.debug, "Role '{}' already gone during deletion", role_name);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().delete_role(
        role_name, model::timeout_clock::now() + 5s);
    vlog(
      adminlog.debug,
      "Deleting user '{}' {}:{}",
      role_name,
      err,
      err.message());
    if (err == cluster::errc::role_does_not_exist) {
        // TODO(oren): deviating somewhat from API spec. please update spec
        // Idempotency: removing a non-existent user is successful.
        co_return ss::json::json_return_type(ss::json::json_void{});
    }
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type> admin_server::update_role_members(
  [[maybe_unused]] std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    ss::sstring role_v;
    if (!ss::http::internal::url_decode(req->param["role"], role_v)) {
        throw ss::httpd::bad_param_exception{fmt::format(
          "Invalid parameter 'role' got {{{}}}", req->param["role"])};
    }
    auto role_name = security::role_name(role_v);

    auto role = _controller->get_role_store().local().get<security::role>(
      role_name);

    // TODO(oren): we can create the role here. query param
    if (!role.has_value()) {
        vlog(adminlog.debug, "Role '{}' does not exist", role_name);
        throw ss::httpd::not_found_exception{
          fmt::format("Role '{}' not found", role_name)};
    }

    auto doc = co_await parse_json_body(req.get());

    auto add = parse_members_list(doc, "add");
    auto remove = parse_members_list(doc, "remove");

    // TODO(oren): check for conflict between add and remove because we don't
    // guarantee an ordering of operations
    // for now, we'll remove first, so the overall effect is positive

    // make a copy of the members
    auto members{role.value().members()};

    absl::erase_if(
      members, [&remove](const auto& m) { return remove.contains(m); });
    std::move(add.begin(), add.end(), std::inserter(members, members.end()));

    security::role new_role{std::move(members)};

    ss::httpd::security_json::role_membership_update_response j_res;
    j_res.role = role_name();
    j_res.created = false;

    if (is_no_op_role_write(
          _controller->get_role_store().local(), role_name, new_role)) {
        vlog(
          adminlog.debug,
          "Role '{}' already reflects these membership changes",
          role_name);
        co_return ss::json::json_return_type(j_res);
    }

    auto err
      = co_await _controller->get_security_frontend().local().update_role(
        role_name, new_role, model::timeout_clock::now() + 5s);

    vlog(adminlog.debug, "Updating role {}:{}", err, err.message());
    co_await throw_on_error(*req, err, model::controller_ntp);

    for (const auto& m : new_role.members()) {
        if (!role.value().members().contains(m)) {
            ss::httpd::security_json::role_member j_member;
            j_member.name = m.name();
            // TODO(oren): discern type from the role member item itself
            j_member.principal_type = "User";
            j_res.added.push(j_member);
        }
    }

    for (const auto& m : role.value().members()) {
        if (!new_role.members().contains(m)) {
            ss::httpd::security_json::role_member j_member;
            j_member.name = m.name();
            // TODO(oren): discern type from role member itself
            j_member.principal_type = "User";
            j_res.removed.push(j_member);
        }
    }

    co_return ss::json::json_return_type(j_res);
}
