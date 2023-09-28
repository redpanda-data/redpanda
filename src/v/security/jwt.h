/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "json/document.h"
#include "oncore.h"
#include "outcome.h"
#include "utils/utf8.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>
#include <cryptopp/base64.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/integer.h>
#include <cryptopp/osrng.h>
#include <cryptopp/rsa.h>

#include <optional>
#include <string_view>
#include <system_error>

namespace security::oidc {

enum errc {
    success = 0,
    metadata_invalid,
    jwks_invalid,
    jwk_invalid,
    jws_invalid,
    jwt_invalid,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "security::oidc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::metadata_invalid:
            return "Invalid metadata";
        case errc::jwks_invalid:
            return "Invalid jwks";
        case errc::jwk_invalid:
            return "Invalid jwk";
        case errc::jws_invalid:
            return "Invalid jws";
        case errc::jwt_invalid:
            return "Invalid jwt";
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}

inline std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}

} // namespace security::oidc

namespace std {
template<>
struct is_error_code_enum<security::oidc::errc> : true_type {};

} // namespace std

namespace security::oidc {

namespace detail {

template<typename T>
struct is_string_viewable : std::false_type {};
template<typename CharT, typename Size, Size max_size, bool NulTerminate>
struct is_string_viewable<
  ss::basic_sstring<CharT, Size, max_size, NulTerminate>> : std::true_type {};
template<typename CharT>
struct is_string_viewable<std::basic_string_view<CharT>> : std::true_type {};
template<typename T>
concept string_viewable = detail::is_string_viewable<T>::value;

using cryptopp_bytes = ss::basic_sstring<CryptoPP::byte, uint32_t, 31, false>;
using cryptopp_byte_view = std::basic_string_view<cryptopp_bytes::value_type>;

template<typename CharT = std::string_view::value_type>
std::basic_string_view<CharT> as_string_view(json::Value const& v) {
    expression_in_debug_mode(
      vassert(
        v.IsString(), "only Strings can be converted to std::string_view"););
    return {reinterpret_cast<CharT const*>(v.GetString()), v.GetStringLength()};
}

template<typename CharT = std::string_view::value_type>
std::optional<std::basic_string_view<CharT>>
string_view(json::Value const& doc, std::string_view field) {
    auto it = doc.FindMember(field.data());
    if (it == doc.MemberEnd() || !it->value.IsString()) {
        return std::nullopt;
    }
    return as_string_view<CharT>(it->value);
}

template<typename Clock>
std::optional<typename Clock::time_point>
time_point(json::Value const& doc, std::string_view field) {
    auto it = doc.FindMember(field.data());
    if (it == doc.MemberEnd() || !it->value.IsInt64()) {
        return std::nullopt;
    }
    return
      typename Clock::time_point(std::chrono::seconds(it->value.GetInt64()));
}

template<string_viewable StringT = cryptopp_bytes>
auto base64_url_decode(cryptopp_byte_view sv) {
    CryptoPP::Base64URLDecoder decoder;

    decoder.Put(sv.data(), sv.size());
    decoder.MessageEnd();

    StringT decoded;
    if (auto size = decoder.MaxRetrievable(); size != 0) {
        decoded.resize(size);
        decoder.Get(
          reinterpret_cast<CryptoPP::byte*>(decoded.data()), decoded.size());
    }
    return decoded;
};

inline std::optional<CryptoPP::Integer>
as_cryptopp_integer(json::Value const& v, std::string_view field) {
    CryptoPP::Integer integer;
    auto b64 = string_view<CryptoPP::byte>(v, field);
    if (!b64.has_value()) {
        return std::nullopt;
    }
    auto chars = base64_url_decode(b64.value());
    return CryptoPP::Integer{chars.data(), chars.size()};
}

} // namespace detail

// Authorization Server Metadata as defined by
// https://www.rfc-editor.org/rfc/rfc8414.html
class metadata {
public:
    static result<metadata> make(std::string_view sv) {
        json::Document doc;
        doc.Parse(sv.data(), sv.length());
        return make(std::move(doc));
    }
    static result<metadata> make(json::Document doc) {
        if (doc.HasParseError() || !doc.IsObject()) {
            return errc::metadata_invalid;
        }
        for (auto const& field : {"issuer", "jwks_uri"}) {
            auto f = detail::string_view(doc, field);
            if (!f || f->empty()) {
                return errc::metadata_invalid;
            }
        }
        return metadata(std::move(doc));
    }

    auto issuer() const {
        return detail::string_view(_metadata, "issuer").value();
    }
    auto jwks_uri() const {
        return detail::string_view(_metadata, "jwks_uri").value();
    }

private:
    explicit metadata(json::Document metadata)
      : _metadata{std::move(metadata)} {}
    json::Document _metadata;
};

// A JSON Web Key Set as defined in
// https://www.rfc-editor.org/rfc/rfc7517.html#section-5
class jwks {
public:
    static result<jwks> make(std::string_view sv) {
        json::Document doc;
        doc.Parse(sv.data(), sv.length());
        return make(std::move(doc));
    }

    static result<jwks> make(json::Document doc) {
        if (doc.HasParseError() || !doc.IsObject()) {
            return errc::jwks_invalid;
        }
        auto keys = doc.FindMember("keys");
        if (keys == doc.MemberEnd() || !keys->value.IsArray()) {
            return errc::jwks_invalid;
        }

        return jwks(std::move(doc));
    }

    auto keys() const { return _impl.FindMember("keys")->value.GetArray(); }

private:
    explicit jwks(json::Document doc)
      : _impl{std::move(doc)} {}

    json::Document _impl;
};

// A JSON Web Signature as defined by
// https://www.rfc-editor.org/rfc/rfc7515.html
//
// This is the encoded form; operations are intentially limited until it has
// been validated and a JWT extracted.
class jws {
public:
    static result<jws> make(ss::sstring encoded) {
        // Perform a quick check that this could be a valid JWS.
        // I.e., it's not an unsecured JWT (2 dots, empty signature) or a JWE (4
        // dots)
        if (encoded.ends_with('.') || absl::c_count(encoded, '.') != 2) {
            return errc::jws_invalid;
        }
        return jws{std::move(encoded)};
    }

private:
    explicit jws(ss::sstring encoded)
      : _encoded{std::move(encoded)} {}

    ss::sstring _encoded;
};

// A JSON Web Token as defined by
// https://www.rfc-editor.org/rfc/rfc7519
class jwt {
public:
    static result<jwt> make(json::Document header, json::Document payload) {
        if (header.HasParseError() || !header.IsObject()) {
            return errc::jwt_invalid;
        }

        if (payload.HasParseError() || !payload.IsObject()) {
            return errc::jwt_invalid;
        }

        if (detail::string_view(header, "typ") != "JWT") {
            return errc::jwt_invalid;
        }

        for (auto const& field : {"alg", "kid"}) {
            auto f = detail::string_view(header, field);
            if (!f || f->empty()) {
                return jwt_invalid;
            }
        }

        return jwt(std::move(header), std::move(payload));
    }

    // Retrieve the Claim named claim.
    auto claim(std::string_view claim) const {
        return detail::string_view(_payload, claim);
    }

    // Retrieve the Algorithm Header Parameter
    // https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.1
    auto alg() const { return detail::string_view(_header, "alg"); }

    // Retrieve the Key ID Header Parameter
    // https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.4
    auto kid() const { return detail::string_view(_header, "kid"); }

    // Retrieve the Type Header Parameter
    // https://www.rfc-editor.org/rfc/rfc7519#section-5.1
    auto typ() const { return detail::string_view(_header, "typ"); }

    // Retrieve the Issuer Claim
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.1
    auto iss() const { return claim("iss"); }

    // Retrieve the Subject Claim
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.2
    auto sub() const { return claim("sub"); }

    // Check for aud in the "aud" Claim
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3
    bool has_aud(std::string_view aud) const {
        const auto is_aud = [aud](auto const& v) {
            return v.IsString()
                   && std::string_view{v.GetString(), v.GetStringLength()}
                        == aud;
        };

        auto it = _payload.FindMember("aud");
        if (it == _payload.MemberEnd()) {
            return false;
        }
        if (is_aud(it->value)) {
            return true;
        }
        if (!it->value.IsArray()) {
            return false;
        }
        return absl::c_any_of(it->value.GetArray(), is_aud);
    }

    // Retrieve the Expiration Time Claim as a Clock::time_point
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.4
    template<typename Clock>
    auto exp() const {
        return detail::time_point<Clock>(_payload, "exp");
    }

    // Retrieve the Not Before Claim as a Clock::time_point
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.5
    template<typename Clock>
    auto nbf() const {
        return detail::time_point<Clock>(_payload, "nbf");
    }

    // Retrieve the Issued At Claim as a Clock::time_point
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.6
    template<typename Clock>
    auto iat() const {
        return detail::time_point<Clock>(_payload, "iat");
    }

    // Retrieve JWT ID Claim
    // https://www.rfc-editor.org/rfc/rfc7519#section-4.1.7
    auto jti() const { return claim("jti"); }

private:
    jwt(json::Document header, json::Document payload)
      : _header{std::move(header)}
      , _payload{std::move(payload)} {}
    json::Document _header;
    json::Document _payload;
};

namespace detail {

// Base class for verifying and signing tokens
template<typename Algo, const char* Alg, const char* Kty>
class signature_base {
public:
    using algorithm = Algo;
    using verifier = algorithm::Verifier;
    using signer = algorithm::Signer;

    constexpr std::string_view alg() const { return Alg; }
    constexpr std::string_view kty() const { return Kty; }
    constexpr std::string_view use() const { return "sig"; }
};

template<typename Algo, const char* Alg, const char* Kty>
class verifier_impl : public signature_base<Algo, Alg, Kty> {
    using base = signature_base<Algo, Alg, Kty>;

public:
    using verifier = base::verifier;
    using public_key = base::algorithm::PublicKey;

    explicit verifier_impl(verifier&& v)
      : _verifier(std::move(v)) {}

    explicit verifier_impl(public_key&& k)
      : _verifier(std::move(k)) {}

    bool verify(cryptopp_byte_view msg, cryptopp_byte_view sig) const {
        return _verifier.VerifyMessage(
          msg.data(), msg.length(), sig.data(), sig.length());
    }

private:
    verifier _verifier;
};

constexpr const char rsa_str[] = "RSA";
constexpr const char rs256_str[] = "RS256";
using rs256_verifier = verifier_impl<
  CryptoPP::RSASS<CryptoPP::PKCS1v15, CryptoPP::SHA256>,
  rs256_str,
  rsa_str>;

// Verify the signature of a message
class verifier {
public:
    template<typename Algo, const char* Alg, const char* Kty>
    explicit verifier(verifier_impl<Algo, Alg, Kty> v)
      : _impl(std::move(v)) {}

    auto alg() const {
        return ss::visit(_impl, [](auto const& impl) { return impl.alg(); });
    }

    auto kty() const {
        return ss::visit(_impl, [](auto const& impl) { return impl.kty(); });
    }

    bool verify(cryptopp_byte_view msg, cryptopp_byte_view sig) const {
        return ss::visit(
          _impl, [=](auto const& impl) { return impl.verify(msg, sig); });
    }

private:
    using verifier_impls = std::variant<rs256_verifier>;
    verifier_impls _impl;
};

inline result<verifier> make_rs256_verifier(
  json::Value const& jwk, CryptoPP::AutoSeededRandomPool& rng) {
    CryptoPP::RSA::PublicKey key;
    try {
        auto n = detail::as_cryptopp_integer(jwk, "n");
        auto e = detail::as_cryptopp_integer(jwk, "e");
        if (!n.has_value() || !e.has_value()) {
            return errc::jwk_invalid;
        }
        key.Initialize(n.value(), e.value());
        if (!key.Validate(rng, 3)) {
            return errc::jwk_invalid;
        }
    } catch (CryptoPP::Exception const& ex) {
        return errc::jwk_invalid;
    }
    return verifier{rs256_verifier{key}};
}

} // namespace detail

} // namespace security::oidc
