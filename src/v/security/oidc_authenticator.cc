/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/oidc_authenticator.h"

#include "base/vlog.h"
#include "config/property.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/jwt.h"
#include "security/logger.h"
#include "security/oidc_principal_mapping_applicator.h"
#include "security/oidc_service.h"

#include <seastar/core/lowres_clock.hh>

#include <boost/outcome/success_failure.hpp>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <memory>
#include <string_view>

namespace security::oidc {

result<authentication_data> authenticate(
  const jwt& jwt,
  const principal_mapping_rule& mapping,
  std::string_view issuer,
  std::string_view audience,
  std::chrono::seconds clock_skew_tolerance,
  ss::lowres_system_clock::time_point now) {
    if (jwt.iss() != issuer) {
        return errc::jwt_invalid_iss;
    }

    if (!jwt.has_aud(audience)) {
        return errc::jwt_invalid_aud;
    }

    using clock = ss::lowres_system_clock;
    auto skew = clock_skew_tolerance;

    auto exp = jwt.exp<clock>().value_or(now);
    if ((exp + skew) < now) {
        return errc::jwt_invalid_exp;
    }

    if ((jwt.iat<clock>().value_or(now) - skew) > now) {
        return errc::jwt_invalid_iat;
    }

    if ((jwt.nbf<clock>().value_or(now) - skew) > now) {
        return errc::jwt_invalid_nbf;
    }

    auto principal = principal_mapping_rule_apply(mapping, jwt);
    if (principal.has_error()) {
        return principal.assume_error();
    }

    return authentication_data{
      std::move(principal).assume_value(),
      ss::sstring{jwt.sub().value_or("")},
      exp};
}

result<authentication_data> authenticate(
  const jws& jws,
  const verifier& verifier,
  const principal_mapping_rule& mapping,
  std::string_view issuer,
  std::string_view audience,
  std::chrono::seconds clock_skew_tolerance,
  ss::lowres_system_clock::time_point now) {
    auto jwt_res = verifier.verify(jws);
    if (jwt_res.has_error()) {
        vlog(seclog.warn, "{}", jwt_res.assume_error());
        return jwt_res.assume_error();
    }

    auto jwt = std::move(jwt_res).assume_value();
    auto a_res = authenticate(
      jwt, mapping, issuer, audience, clock_skew_tolerance, now);
    if (a_res.has_error()) {
        vlog(
          seclog.warn,
          "JWT Validation failed with err: {}, JWT: {}",
          a_res.assume_error().message(),
          jwt);
        return a_res.assume_error();
    }
    return std::move(a_res).assume_value();
}

class authenticator::impl {
public:
    explicit impl(oidc::service& service)
      : _service{service} {};

    result<authentication_data>
    authenticate(std::string_view bearer_token) const {
        auto jws = oidc::jws::make(ss::sstring{bearer_token});
        if (jws.has_error()) {
            vlog(
              seclog.warn, "Invalid token: {}", jws.assume_error().message());
            return jws.assume_error();
        }

        auto issuer = _service.issuer();
        if (issuer.has_error()) {
            vlog(
              seclog.warn,
              "Identity Provider has no issuer: {}",
              issuer.assume_error().message());
            return issuer.assume_error();
        }

        return oidc::authenticate(
          jws.assume_value(),
          _service.get_verifier(),
          _service.get_principal_mapping_rule(),
          issuer.assume_value(),
          _service.audience(),
          _service.clock_skew_tolerance(),
          ss::lowres_system_clock::now());
    }

private:
    service& _service;
};

authenticator::authenticator(service& service)
  : _impl{std::make_unique<impl>(service)} {}

authenticator::~authenticator() = default;

result<authentication_data>
authenticator::authenticate(std::string_view bearer_token) {
    return _impl->authenticate(bearer_token);
}

std::ostream& operator<<(std::ostream& os, const sasl_authenticator::state s) {
    using state = sasl_authenticator::state;
    switch (s) {
    case state::init:
        return os << "init";
    case state::complete:
        return os << "complete";
    case state::failed:
        return os << "failed";
    }
}

sasl_authenticator::sasl_authenticator(service& service)
  : _authenticator{service}
  , _audit_user() {}

sasl_authenticator::~sasl_authenticator() = default;

ss::future<result<bytes>> sasl_authenticator::authenticate(bytes auth_bytes) {
    if (_state != state::init) {
        vlog(seclog.warn, "invalid oidc state: {}", _state);
        co_return security::errc::invalid_oidc_state;
    }

    auto auth_str = std::string_view(
      reinterpret_cast<char*>(auth_bytes.data()), auth_bytes.size());

    constexpr std::string_view sasl_header{"n,,\1auth=Bearer "};
    if (!auth_str.starts_with(sasl_header)) {
        vlog(seclog.warn, "invalid sasl_header");
        co_return security::errc::invalid_credentials;
    }
    auth_str = auth_str.substr(sasl_header.length());
    if (!auth_str.ends_with("\1\1")) {
        vlog(seclog.warn, "invalid sasl_header");
        co_return security::errc::invalid_credentials;
    }
    auth_str = auth_str.substr(0, auth_str.length() - 2);

    auto auth_res = _authenticator.authenticate(auth_str);
    if (auth_res.has_error()) {
        vlog(seclog.warn, "{}", auth_res.error());
        co_return security::errc::invalid_credentials;
    }

    _auth_data = auth_res.assume_value();
    _audit_user.type_id = audit::user::type::user;
    _audit_user.name = _auth_data.principal.name();
    _audit_user.uid = _auth_data.sub;
    _state = state::complete;

    co_return bytes{};
}

} // namespace security::oidc
