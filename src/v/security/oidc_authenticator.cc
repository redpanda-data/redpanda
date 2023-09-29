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

#include "config/property.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/jwt.h"
#include "security/logger.h"
#include "security/oidc_service.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>

#include <boost/outcome/success_failure.hpp>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <memory>
#include <string_view>

namespace security::oidc {

result<acl_principal> authenticate(
  jwt const& jwt,
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

    if ((jwt.exp<clock>().value_or(now) + skew) < now) {
        return errc::jwt_invalid_exp;
    }

    if ((jwt.iat<clock>().value_or(now) - skew) > now) {
        return errc::jwt_invalid_iat;
    }

    if ((jwt.nbf<clock>().value_or(now) - skew) > now) {
        return errc::jwt_invalid_nbf;
    }

    auto sub = jwt.sub().value_or("");
    if (sub.empty()) {
        return errc::jwt_invalid_sub;
    }

    return {principal_type::user, ss::sstring(sub)};
}

result<acl_principal> authenticate(
  jws const& jws,
  verifier const& verifier,
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
    auto a_res = authenticate(jwt, issuer, audience, clock_skew_tolerance, now);
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

    result<acl_principal> authenticate(std::string_view bearer_token) const {
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

result<acl_principal>
authenticator::authenticate(std::string_view bearer_token) {
    return _impl->authenticate(bearer_token);
}

} // namespace security::oidc
