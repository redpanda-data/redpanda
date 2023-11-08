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
#include "security/jwt.h"
#include "security/logger.h"
#include "security/oidc_principal_mapping.h"
#include "security/oidc_url_parser.h"
#include "ssx/future-util.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/algorithm/container.h>

namespace security::oidc {

namespace {
ss::future<ss::sstring> make_request(std::string_view url_view) {
    auto url_res = security::oidc::parse_url(url_view);
    if (url_res.has_error()) {
        throw std::runtime_error("Invalid url");
    }
    auto url = std::move(url_res).assume_value();
    auto is_https = url.scheme == "https";
    std::optional<ss::sstring> tls_host;
    ss::shared_ptr<ss::tls::certificate_credentials> creds;
    if (is_https) {
        tls_host.emplace(url.host);
        ss::tls::credentials_builder builder;
        builder.set_client_auth(ss::tls::client_auth::NONE);
        co_await builder.set_system_trust();
        creds = builder.build_certificate_credentials();
        creds->set_dn_verification_callback([](
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
    http::client client{net::base_transport::configuration{
      .server_addr = {url.host, url.port},
      .credentials = creds,
      .tls_sni_hostname = tls_host}};

    http::client::request_header req_hdr;
    req_hdr.method(boost::beast::http::verb::get);
    req_hdr.target({url.target.data(), url.target.length()});
    req_hdr.insert(
      boost::beast::http::field::host, {url.host.data(), url.host.length()});
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

        namespace coro = ss::coroutine;

        if (auto f = co_await coro::as_future(update_metadata()); f.failed()) {
            vlog(
              seclog.error,
              "Failed to retrieve metadata: {}, error: {}",
              _discovery_url(),
              f.get_exception());
            co_return;
        }

        if (!_issuer.has_value()) {
            vlog(seclog.error, "Failed to retrieve issuer from metadata");
            co_return;
        }
        if (!_jwks_url.has_value()) {
            vlog(seclog.error, "Failed to retrieve jwks_url from metadata");
            co_return;
        }

        if (auto f = co_await coro::as_future(update_jwks()); f.failed()) {
            vlog(
              seclog.error,
              "Failed to retrieve jwks: {}, error: {}",
              _jwks_url.value(),
              f.get_exception());
            co_return;
        }

        if (!_jwks) {
            vlog(seclog.error, "Error updating keys: Keys not found");
            co_return;
        }

        auto res = _verifier.update_keys(*_jwks);
        if (res.has_error()) {
            vlog(
              seclog.error,
              "Error updating keys: {}",
              res.assume_error().message());
            co_return;
        }
    }

    ss::future<> update_metadata() {
        ss::sstring response_body = co_await make_request(_discovery_url());
        auto metadata = oidc::metadata::make(response_body);
        if (metadata.has_error()) {
            vlog(
              seclog.warn,
              "invalid response from discovery_url: {}, err: {}",
              _discovery_url(),
              metadata.assume_error().message());
            co_return;
        }

        _issuer.emplace(metadata.assume_value().issuer());
        _jwks_url.emplace(metadata.assume_value().jwks_uri());
    }

    ss::future<> update_jwks() {
        if (!_jwks_url.has_value()) {
            vlog(seclog.warn, "jwks_uri is not set");
            co_return;
        }

        auto response_body = co_await make_request(*_jwks_url);
        auto jwks = oidc::jwks::make(response_body);
        if (jwks.has_error()) {
            vlog(
              seclog.warn,
              "invalid response from jwks_uri: {}, errc: {}",
              *_jwks_url,
              jwks.assume_error().message());
            co_return;
        }

        _jwks.emplace(std::move(jwks).assume_value());
    }

    void update_rule() {
        if (auto r = parse_principal_mapping_rule(_mapping()); r.has_error()) {
            vlog(seclog.error, "Rule failed to parse: {}", _mapping());
        } else {
            _rule = std::move(r).assume_value();
        }
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
    std::optional<ss::sstring> _issuer;
    std::optional<ss::sstring> _jwks_url;
    std::optional<oidc::jwks> _jwks;
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
