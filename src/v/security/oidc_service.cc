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

#include "config/configuration.h"
#include "config/tls_config.h"
#include "http/client.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "net/tls_certificate_probe.h"
#include "security/exceptions.h"
#include "security/jwt.h"
#include "security/logger.h"
#include "security/oidc_principal_mapping.h"
#include "security/oidc_url_parser.h"
#include "ssx/future-util.h"
#include "utils/log_hist.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <boost/outcome/success_failure.hpp>

#include <chrono>

using namespace std::chrono_literals;

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

class probe {
public:
    using hist_t = log_hist_public;

    class auto_measure {
    public:
        explicit auto_measure(probe* p)
          : _p{p}
          , _m{p->_request_latency} {}

    public:
        void success() noexcept {
            if (std::exchange(_p, nullptr) != nullptr) {
                auto report_and_destroy = std::move(_m);
            }
        }
        void failed() noexcept {
            if (auto p = std::exchange(_p, nullptr); p != nullptr) {
                ++p->_request_errors;
                _m.cancel();
            }
        }

    private:
        probe* _p;
        hist_t::measurement _m;
    };

    void setup_metrics(std::string_view domain, std::string_view mechanism) {
        namespace sm = ss::metrics;

        auto domain_l = metrics::make_namespaced_label("domain");
        auto mechanism_l = metrics::make_namespaced_label("mechanism");
        const std::vector<sm::label_instance> labels = {
          domain_l(domain), mechanism_l(mechanism)};

        _metrics.clear();
        _metrics.add_group(
          prometheus_sanitize::metrics_name("security_idp"),
          {sm::make_histogram(
             "latency_seconds",
             [this] { return _request_latency.public_histogram_logform(); },
             sm::description("Latency of requests to the Identity Provider"),
             labels),
           sm::make_counter(
             "errors_total",
             [this] { return _request_errors; },
             sm::description("Number of requests that returned with an error."),
             labels)});
    }
    void
    setup_public_metrics(std::string_view domain, std::string_view mechanism) {
        namespace sm = ss::metrics;

        auto domain_l = metrics::make_namespaced_label("domain");
        auto mechanism_l = metrics::make_namespaced_label("mechanism");
        const std::vector<sm::label_instance> labels = {
          domain_l(domain), mechanism_l(mechanism)};

        _public_metrics.clear();
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("security_idp"),
          {sm::make_histogram(
             "latency_seconds",
             [this] { return _request_latency.public_histogram_logform(); },
             sm::description("Latency of requests to the Identity Provider"),
             labels),
           sm::make_counter(
             "errors_total",
             [this] { return _request_errors; },
             sm::description("Number of requests that returned with an error."),
             labels)});
    }

    auto_measure measure_request() { return auto_measure(this); }

private:
    hist_t _request_latency;
    int64_t _request_errors{};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace

struct service::impl {
    ss::shard_id update_shard_id{0};
    impl(
      config::binding<std::vector<ss::sstring>> sasl_mechanisms,
      config::binding<std::vector<ss::sstring>> http_authentication,
      config::binding<ss::sstring> discovery_url,
      config::binding<ss::sstring> token_audience,
      config::binding<std::chrono::seconds> clock_skew_tolerance,
      config::binding<ss::sstring> mapping,
      config::binding<std::chrono::seconds> jwks_refresh_interval)
      : _verifier{}
      , _sasl_mechanisms{std::move(sasl_mechanisms)}
      , _http_authentication{std::move(http_authentication)}
      , _discovery_url{std::move(discovery_url)}
      , _token_audience{std::move(token_audience)}
      , _clock_skew_tolerance{std::move(clock_skew_tolerance)}
      , _mapping{std::move(mapping)}
      , _rule{}
      , _jwks_refresh_interval{std::move(jwks_refresh_interval)}
      , _jwks_refresh{[this]() {
          ssx::spawn_with_gate(_gate, [this] { return update(); });
      }} {
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
        _jwks_refresh_interval.watch([this]() {
            if (_gate.is_closed()) {
                return;
            }
            auto now = ss::lowres_clock::now();
            if (now + _jwks_refresh_interval() < _jwks_refresh.get_timeout()) {
                _jwks_refresh.rearm(now + _jwks_refresh_interval());
            }
        });
    }

    ss::future<> start() {
        auto holder = _gate.hold();
        co_await update();
    }
    ss::future<> stop() {
        _jwks_refresh.cancel();
        return _gate.close();
    }

    probe& get_probe_for(const parsed_url& url) {
        auto& p = _probes[url.host];
        if (!p) {
            p = std::make_unique<probe>();
        }
        return *p;
    }

    result<void> update_url(
      std::optional<parsed_url>& existing, std::string_view update_sv) {
        auto url_res = security::oidc::parse_url(update_sv);
        if (url_res.has_error()) {
            return errc::metadata_invalid;
        }
        auto update = std::move(url_res).assume_value();

        auto& p = get_probe_for(update);
        if (!existing.has_value() || existing->host != update.host) {
            p.setup_metrics(update.host, "oidc");
            p.setup_public_metrics(update.host, "oidc");
        }
        existing = std::move(update);
        return outcome::success();
    }

    ss::future<> update() {
        auto enabled = absl::c_any_of(
                         _sasl_mechanisms(),
                         [](const auto& m) { return m == "OAUTHBEARER"; })
                       || absl::c_any_of(
                         _http_authentication(),
                         [](const auto& m) { return m == "OIDC"; });
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

        auto measure = get_probe_for(*_parsed_discovery_url).measure_request();
        auto response_body = co_await ss::coroutine::as_future(
          make_request(*_parsed_discovery_url));
        if (response_body.failed()) {
            measure.failed();
            co_await return_exception(
              errc::metadata_invalid,
              "Failed to retrieve metadata: {}, error: {}",
              _discovery_url(),
              response_body.get_exception());
        }
        auto metadata = oidc::metadata::make(response_body.get());
        if (metadata.has_error()) {
            measure.failed();
            co_await return_exception(
              metadata.assume_error(),
              "invalid response from discovery_url: {}, err: {}",
              _discovery_url(),
              metadata.assume_error().message());
        }

        url_res = update_url(
          _parsed_jwks_url, metadata.assume_value().jwks_uri());
        if (url_res.has_error()) {
            measure.failed();
            co_await return_exception(
              url_res.assume_error(),
              "Failed to parse jwkk_uri: {}",
              metadata.assume_value().jwks_uri());
        }
        measure.success();

        _issuer.emplace(metadata.assume_value().issuer());
    }

    ss::future<> update_jwks() {
        constexpr auto default_retry = 5s;

        std::chrono::seconds arm_duration = default_retry;
        auto arm_timer = ss::defer([this, &arm_duration]() {
            _jwks_refresh.cancel();
            _jwks_refresh.arm(arm_duration);
        });

        if (!_parsed_jwks_url.has_value()) {
            co_await return_exception(
              errc::metadata_invalid, "jwks_uri is not set");
        }

        auto measure = get_probe_for(*_parsed_jwks_url).measure_request();
        auto response_body = co_await ss::coroutine::as_future(
          make_request(*_parsed_jwks_url));
        if (response_body.failed()) {
            measure.failed();
            co_await return_exception(
              errc::metadata_invalid,
              "Failed to retrieve jwks: {}, error: {}",
              *_parsed_jwks_url,
              response_body.get_exception());
        }

        auto jwks = oidc::jwks::make(response_body.get());
        if (jwks.has_error()) {
            measure.failed();
            co_await return_exception(
              jwks.assume_error(),
              "Invalid response from jwks_uri: {}",
              *_parsed_jwks_url);
        }
        measure.success();

        auto res = _verifier.update_keys(std::move(jwks).assume_value());
        if (res.has_error()) {
            co_await return_exception(
              res.assume_error(), "Error updating keys");
        }

        arm_duration = _jwks_refresh_interval();
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
                builder.set_minimum_tls_version(config::from_config(
                  config::shard_local_cfg().tls_min_version()));
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
    config::binding<std::chrono::seconds> _jwks_refresh_interval;
    std::optional<parsed_url> _parsed_discovery_url;
    std::optional<parsed_url> _parsed_jwks_url;
    std::optional<ss::sstring> _issuer;
    ss::timer<ss::lowres_clock> _jwks_refresh;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
    absl::flat_hash_map<ss::sstring, std::unique_ptr<probe>> _probes;
};

service::service(
  config::binding<std::vector<ss::sstring>> sasl_mechanisms,
  config::binding<std::vector<ss::sstring>> http_authentication,
  config::binding<ss::sstring> discovery_url,
  config::binding<ss::sstring> token_audience,
  config::binding<std::chrono::seconds> clock_skew_tolerance,
  config::binding<ss::sstring> mapping,
  config::binding<std::chrono::seconds> keys_refresh_interval)
  : _impl{std::make_unique<impl>(
      std::move(sasl_mechanisms),
      std::move(http_authentication),
      std::move(discovery_url),
      std::move(token_audience),
      std::move(clock_skew_tolerance),
      std::move(mapping),
      std::move(keys_refresh_interval))} {}

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

const verifier& service::get_verifier() const { return _impl->_verifier; }

const principal_mapping_rule& service::get_principal_mapping_rule() const {
    return _impl->_rule;
}

ss::future<> service::refresh_keys() { return _impl->update_jwks(); }

} // namespace security::oidc
