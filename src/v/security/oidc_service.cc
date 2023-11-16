/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/oidc_service.h"

#include "http/client.h"
#include "net/tls_certificate_probe.h"
#include "security/exceptions.h"
#include "security/jwt.h"
#include "security/logger.h"
#include "security/oidc_principal_mapping.h"
#include "security/oidc_url_parser.h"
#include "ssx/future-util.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include <absl/algorithm/container.h>
#include <boost/outcome/success_failure.hpp>

namespace security::oidc {

namespace {

template<typename... Args>
[[nodiscard]] auto return_exception(
  std::error_code ec,
  fmt::format_string<Args...> fmt,
  Args&&... args) noexcept {
    return ss::coroutine::return_exception(
      exception(ec, fmt::format(fmt, std::forward<Args>(args)...)));
}

using seastar::operator co_await;

} // namespace

struct service::impl {
    ss::shard_id update_shard_id{0};
    impl(
      config::binding<std::vector<ss::sstring>> sasl_mechanisms,
      config::binding<std::vector<ss::sstring>> http_authentication,
      config::binding<ss::sstring> discovery_url,
      config::binding<ss::sstring> token_audience,
      config::binding<std::chrono::seconds> clock_skew_tolerance,
      config::binding<ss::sstring> mapping)
      : _verifier{}
      , _sasl_mechanisms{std::move(sasl_mechanisms)}
      , _http_authentication{std::move(http_authentication)}
      , _discovery_url{std::move(discovery_url)}
      , _token_audience{std::move(token_audience)}
      , _clock_skew_tolerance{std::move(clock_skew_tolerance)}
      , _mapping{std::move(mapping)}
      , _rule{} {
        _sasl_mechanisms.watch([this]() {
            ssx::spawn_with_gate(_gate, [this] { return update(); });
        });
        _http_authentication.watch([this]() {
            ssx::spawn_with_gate(_gate, [this] { return update(); });
        });
        _discovery_url.watch([this]() {
            ssx::spawn_with_gate(_gate, [this] { return update(); });
        });
        _mapping.watch([this]() { update_rule(); });
        update_rule();
    }

    ss::future<> start() {
        auto holder = _gate.hold();
        co_await update();
    }
    ss::future<> stop() { return _gate.close(); }

    result<void> update_url(
      std::optional<parsed_url>& existing, std::string_view update_sv) {
        auto url_res = security::oidc::parse_url(update_sv);
        if (url_res.has_error()) {
            return errc::metadata_invalid;
        }
        existing = std::move(url_res).assume_value();
        return outcome::success();
    }

    ss::future<> update() {
        auto enabled = absl::c_any_of(
                         _sasl_mechanisms(),
                         [](auto const& m) { return m == "OAUTHBEARER"; })
                       || absl::c_any_of(
                         _http_authentication(),
                         [](auto const& m) { return m == "OIDC"; });
        if (!enabled) {
            co_return;
        }

        constexpr auto log_exception = [](std::string_view msg) {
            return [msg](std::exception_ptr e) {
                vlog(seclog.error, "Error updating {}: {}", msg, e);
            };
        };

        co_await update_metadata().handle_exception(log_exception("metadata"));
        co_await update_jwks().handle_exception(log_exception("jwks"));
    }

    ss::future<> update_metadata() {
        auto url_res = update_url(_parsed_discovery_url, _discovery_url());
        if (url_res.has_error()) {
            co_await return_exception(
              url_res.assume_error(),
              "Failed to parse discovery_uri: {}",
              _discovery_url());
        }

        auto response_body = co_await ss::coroutine::as_future(
          make_request(*_parsed_discovery_url));
        if (response_body.failed()) {
            co_await return_exception(
              errc::metadata_invalid,
              "Failed to retrieve metadata: {}, error: {}",
              _discovery_url(),
              response_body.get_exception());
        }
        auto metadata = oidc::metadata::make(response_body.get());
        if (metadata.has_error()) {
            co_await return_exception(
              metadata.assume_error(),
              "invalid response from discovery_url: {}, err: {}",
              _discovery_url(),
              metadata.assume_error().message());
        }

        url_res = update_url(
          _parsed_jwks_url, metadata.assume_value().jwks_uri());
        if (url_res.has_error()) {
            co_await return_exception(
              url_res.assume_error(),
              "Failed to parse jwkk_uri: {}",
              metadata.assume_value().jwks_uri());
        }

        _issuer.emplace(metadata.assume_value().issuer());
    }

    ss::future<> update_jwks() {
        if (!_parsed_jwks_url.has_value()) {
            co_await return_exception(
              errc::metadata_invalid, "jwks_uri is not set");
        }

        auto response_body = co_await ss::coroutine::as_future(
          make_request(*_parsed_jwks_url));
        if (response_body.failed()) {
            co_await return_exception(
              errc::metadata_invalid,
              "Failed to retrieve jwks: {}, error: {}",
              *_parsed_jwks_url,
              response_body.get_exception());
        }

        auto jwks = oidc::jwks::make(response_body.get());
        if (jwks.has_error()) {
            co_await return_exception(
              jwks.assume_error(),
              "Invalid response from jwks_uri: {}",
              *_parsed_jwks_url);
        }

        auto res = _verifier.update_keys(std::move(jwks).assume_value());
        if (res.has_error()) {
            co_await return_exception(
              res.assume_error(), "Error updating keys");
        }
    }

    void update_rule() {
        if (auto r = parse_principal_mapping_rule(_mapping()); r.has_error()) {
            vlog(seclog.error, "Rule failed to parse: {}", _mapping());
        } else {
            _rule = std::move(r).assume_value();
        }
    }

    ss::future<ss::sstring> make_request(parsed_url url) {
        auto is_https = url.scheme == "https";
        std::optional<ss::sstring> tls_host;
        if (is_https) {
            tls_host.emplace(url.host);
            if (!_creds) {
                ss::tls::credentials_builder builder;
                builder.set_client_auth(ss::tls::client_auth::NONE);
                co_await builder.set_system_trust();
                _creds = co_await net::build_reloadable_credentials_with_probe<
                  ss::tls::certificate_credentials>(
                  std::move(builder), "oidc_provider", "httpclient");
                _creds->set_dn_verification_callback(
                  [](
                    ss::tls::session_type type,
                    ss::sstring subject,
                    ss::sstring issuer) {
                      vlog(
                        seclog.trace,
                        "type: ?, subject: {}, issuer: {}",
                        (uint8_t)type,
                        subject,
                        issuer);
                  });
            }
        }
        http::client client{net::base_transport::configuration{
          .server_addr = {url.host, url.port},
          .credentials = is_https ? _creds : nullptr,
          .tls_sni_hostname = tls_host}};

        http::client::request_header req_hdr;
        req_hdr.method(boost::beast::http::verb::get);
        req_hdr.target({url.target.data(), url.target.length()});
        req_hdr.insert(
          boost::beast::http::field::host,
          {url.host.data(), url.host.length()});
        req_hdr.insert(boost::beast::http::field::accept, "*/*");

        co_return co_await http::with_client(
          std::move(client),
          [req_hdr{std::move(req_hdr)}](http::client& client) mutable {
              return client.request(std::move(req_hdr))
                .then([](auto res) -> ss::future<ss::sstring> {
                    ss::sstring response_body;
                    while (!res->is_done()) {
                        iobuf buf = co_await res->recv_some();
                        for (auto& fragm : buf) {
                            response_body.append(fragm.get(), fragm.size());
                        }
                    }
                    co_return response_body;
                });
          });
    }

    ss::gate _gate;
    verifier _verifier;
    config::binding<std::vector<ss::sstring>> _sasl_mechanisms;
    config::binding<std::vector<ss::sstring>> _http_authentication;
    config::binding<ss::sstring> _discovery_url;
    config::binding<ss::sstring> _token_audience;
    config::binding<std::chrono::seconds> _clock_skew_tolerance;
    config::binding<ss::sstring> _mapping;
    principal_mapping_rule _rule;
    std::optional<parsed_url> _parsed_discovery_url;
    std::optional<parsed_url> _parsed_jwks_url;
    std::optional<ss::sstring> _issuer;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
};

service::service(
  config::binding<std::vector<ss::sstring>> sasl_mechanisms,
  config::binding<std::vector<ss::sstring>> http_authentication,
  config::binding<ss::sstring> discovery_url,
  config::binding<ss::sstring> token_audience,
  config::binding<std::chrono::seconds> clock_skew_tolerance,
  config::binding<ss::sstring> mapping)
  : _impl{std::make_unique<impl>(
    std::move(sasl_mechanisms),
    std::move(http_authentication),
    std::move(discovery_url),
    std::move(token_audience),
    std::move(clock_skew_tolerance),
    std::move(mapping))} {}

service::~service() noexcept = default;

ss::future<> service::start() { return _impl->start(); }
ss::future<> service::stop() { return _impl->stop(); }

std::string_view service::audience() const { return _impl->_token_audience(); }

result<std::string_view> service::issuer() const {
    if (_impl->_issuer.has_value()) {
        return _impl->_issuer.value();
    }
    return errc::metadata_invalid;
}
std::chrono::seconds service::clock_skew_tolerance() const {
    return _impl->_clock_skew_tolerance();
}

verifier const& service::get_verifier() const { return _impl->_verifier; }

principal_mapping_rule const& service::get_principal_mapping_rule() const {
    return _impl->_rule;
}

} // namespace security::oidc
