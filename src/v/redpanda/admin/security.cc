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
#include "redpanda/admin/util.h"
#include "security/credential_store.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_service.h"
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
    if (!admin::path_decode(req->param["user"], user_v)) {
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
    if (!admin::path_decode(req->param["user"], user_v)) {
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
