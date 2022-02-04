#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "redpanda/admin/api-doc/security.json.h"
#include "redpanda/admin/server.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "vlog.h"

namespace admin {

namespace {
// TODO: factor out generic serialization from seastar http exceptions
security::scram_credential
parse_scram_credential(const rapidjson::Document& doc) {
    if (!doc.IsObject()) {
        throw ss::httpd::bad_request_exception(fmt::format("Not an object"));
    }

    if (!doc.HasMember("algorithm") || !doc["algorithm"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String algo missing"));
    }
    const auto algorithm = std::string_view(
      doc["algorithm"].GetString(), doc["algorithm"].GetStringLength());

    if (!doc.HasMember("password") || !doc["password"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String password smissing"));
    }
    const auto password = doc["password"].GetString();

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
} // namespace

void admin_server::register_security_routes() {
    ss::httpd::security_json::create_user.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto doc = parse_json_body(*req);

          auto credential = parse_scram_credential(doc);

          if (!doc.HasMember("username") || !doc["username"].IsString()) {
              throw ss::httpd::bad_request_exception(
                fmt::format("String username missing"));
          }

          auto username = security::credential_user(
            doc["username"].GetString());

          auto err
            = co_await _controller->get_security_frontend().local().create_user(
              username, credential, model::timeout_clock::now() + 5s);
          vlog(logger.debug, "Creating user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::delete_user.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto user = security::credential_user(req->param["user"]);

          auto err
            = co_await _controller->get_security_frontend().local().delete_user(
              user, model::timeout_clock::now() + 5s);
          vlog(logger.debug, "Deleting user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::update_user.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto user = security::credential_user(req->param["user"]);

          auto doc = parse_json_body(*req);

          auto credential = parse_scram_credential(doc);

          auto err
            = co_await _controller->get_security_frontend().local().update_user(
              user, credential, model::timeout_clock::now() + 5s);
          vlog(logger.debug, "Updating user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::list_users.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          std::vector<ss::sstring> users;
          for (const auto& [user, _] :
               _controller->get_credential_store().local()) {
              users.push_back(user());
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(users));
      });
}

} // namespace admin
