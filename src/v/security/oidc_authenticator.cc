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

#include "bytes/bytes.h"
#include "config/property.h"
#include "json/document.h"
#include "security/acl.h"
#include "security/errc.h"
#include "security/logger.h"
#include "vlog.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/outcome/success_failure.hpp>
#include <cryptopp/base64.h>
#include <cryptopp/integer.h>
#include <cryptopp/rsa.h>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <string_view>

namespace security {

std::ostream& operator<<(std::ostream& os, oidc_authenticator::state const s) {
    using state = oidc_authenticator::state;
    switch (s) {
    case state::init:
        return os << "init";
    case state::complete:
        return os << "complete";
    case state::failed:
        return os << "failed";
    }
}

class oidc_authenticator::impl {
public:
    impl() = default;

    ss::future<result<void>> authenticate(bytes auth_bytes);
    const security::acl_principal& principal() const { return _principal; }

private:
    ss::future<result<void>> extract_principal(bytes auth_bytes);
    security::acl_principal _principal;
};

oidc_authenticator::oidc_authenticator()
  : _impl{std::make_unique<impl>()} {}

oidc_authenticator::~oidc_authenticator() = default;

ss::future<result<bytes>> oidc_authenticator::authenticate(bytes auth_bytes) {
    if (!_impl) {
        vlog(
          seclog.error,
          "authenticate received after handshake complete {} bytes {}",
          _state,
          auth_bytes.size());
        co_return errc::invalid_oidc_state;
    }

    auto res = co_await _impl->authenticate(std::move(auth_bytes));
    if (res.has_error()) {
        _state = state::failed;
        co_return res.assume_error();
    }
    _principal = _impl->principal();
    _state = state::complete;
    co_return bytes{};
}

ss::future<result<void>> oidc_authenticator::impl::authenticate(bytes b) {
    vlog(seclog.trace, "oidc authenticate received {} bytes", b.size());
    return extract_principal(std::move(b));
}

ss::future<result<void>>
oidc_authenticator::impl::extract_principal(bytes auth_bytes) {
    auto auth_str = std::string_view(
      reinterpret_cast<char*>(auth_bytes.data()), auth_bytes.size());

    constexpr std::string_view sasl_header{"n,,\1auth=Bearer "};
    if (!auth_str.starts_with(sasl_header)) {
        vlog(seclog.debug, "invalid sasl_header");
        co_return errc::invalid_credentials;
    }
    auth_str = auth_str.substr(sasl_header.length());
    if (!auth_str.ends_with("\1\1")) {
        vlog(seclog.debug, "invalid sasl_header");
        co_return errc::invalid_credentials;
    }
    auth_str = auth_str.substr(0, auth_str.length() - 2);
    vlog(seclog.trace, "auth_bytes: {}", auth_str);

    std::vector<std::string_view> jose_enc;
    jose_enc.reserve(3);
    boost::algorithm::split(
      jose_enc, auth_str, [](char c) { return c == '.'; });
    vlog(seclog.trace, "jose_b64: {}", jose_enc);

    if (jose_enc.size() != 3) {
        vlog(seclog.debug, "invalid secured JWT");
        co_return errc::invalid_credentials;
    }

    auto base64_url_decode = [](std::string_view sv) {
        CryptoPP::Base64URLDecoder decoder;

        decoder.Put((CryptoPP::byte*)sv.data(), sv.size());
        decoder.MessageEnd();
        std::string decoded;
        CryptoPP::word64 size = decoder.MaxRetrievable();
        if (size && size <= SIZE_MAX) {
            decoded.resize(size);
            decoder.Get((CryptoPP::byte*)decoded.data(), decoded.size());
        }
        return decoded;
    };

    auto jose_payload_str = base64_url_decode(jose_enc[1]);

    const auto get_member = [](auto const& doc, std::string_view name) {
        std::string_view val;
        if (auto it = doc.FindMember(name.data()); it == doc.MemberEnd()) {
            return val;
        } else if (!it->value.IsString()) {
            return val;
        } else {
            val = {it->value.GetString(), it->value.GetStringLength()};
        }
        return val;
    };

    json::Document jose_payload;
    if (jose_payload.Parse(jose_payload_str).HasParseError()) {
        vlog(seclog.debug, "invalid jwt payload");
        co_return errc::invalid_credentials;
    }

    auto sub = get_member(jose_payload, "sub");
    if (sub.empty()) {
        vlog(seclog.debug, "empty sub");
        co_return errc::invalid_credentials;
    }

    _principal = acl_principal{principal_type::user, ss::sstring(sub)};
    co_return outcome::success();
}

} // namespace security
