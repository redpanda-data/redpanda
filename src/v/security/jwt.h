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

#include "base/oncore.h"
#include "base/outcome.h"
#include "crypto/crypto.h"
#include "json/document.h"
#include "json/ostreamwrapper.h"
#include "json/pointer.h"
#include "json/writer.h"
#include "security/oidc_error.h"
#include "strings/string_switch.h"
#include "strings/utf8.h"
#include "utils/base64.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string/split.hpp>

#include <iosfwd>
#include <optional>
#include <string_view>

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

template<typename ToCharT = char>
std::basic_string_view<ToCharT> char_view_cast(const string_viewable auto& sv) {
    return {reinterpret_cast<const ToCharT*>(sv.data()), sv.size()};
}

struct string_viewable_compare {
    using is_transparent = void;
    bool operator()(
      const string_viewable auto& lhs, const string_viewable auto& rhs) const {
        return char_view_cast(lhs) == char_view_cast(rhs);
    }
};

struct string_viewable_hasher {
    using is_transparent = void;
    size_t operator()(const string_viewable auto& sv) const {
        return std::hash<std::string_view>{}(char_view_cast(sv));
    }
};

template<typename CharT = std::string_view::value_type>
std::basic_string_view<CharT> as_string_view(const json::Value& v) {
    expression_in_debug_mode(
      vassert(
        v.IsString(), "only Strings can be converted to std::string_view"););
    return {reinterpret_cast<const CharT*>(v.GetString()), v.GetStringLength()};
}

template<typename CharT = std::string_view::value_type>
std::optional<std::basic_string_view<CharT>>
string_view(const json::Value& doc, std::string_view field) {
    auto it = doc.FindMember(field.data());
    if (it == doc.MemberEnd() || !it->value.IsString()) {
        return std::nullopt;
    }
    return as_string_view<CharT>(it->value);
}

template<typename Clock>
std::optional<typename Clock::time_point>
time_point(const json::Value& doc, std::string_view field) {
    auto it = doc.FindMember(field.data());
    if (it == doc.MemberEnd() || !it->value.IsInt64()) {
        return std::nullopt;
    }
    return
      typename Clock::time_point(std::chrono::seconds(it->value.GetInt64()));
}

bytes base64_url_decode(std::string_view sv);

std::optional<bytes>
base64_url_decode(const json::Value& v, std::string_view field);

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
        for (const auto& field : {"issuer", "jwks_uri"}) {
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
            return errc::jws_invalid_parts;
        }
        return jws{std::move(encoded)};
    }

private:
    friend class verifier;
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
            return errc::jwt_invalid_json;
        }

        if (payload.HasParseError() || !payload.IsObject()) {
            return errc::jwt_invalid_json;
        }

        // typ header parameter is not required per RFC 7519:
        // https://datatracker.ietf.org/doc/html/rfc7519#section-5.1
        auto typ = detail::string_view(header, "typ");
        if (typ.has_value() && typ != "JWT") {
            return errc::jwt_invalid_typ;
        }

        for (const auto& field :
             {std::make_pair("alg", errc::jwt_invalid_alg),
              std::make_pair("kid", errc::jwt_invalid_kid)}) {
            auto f = detail::string_view(header, field.first);
            if (!f || f->empty()) {
                return field.second;
            }
        }

        return jwt(std::move(header), std::move(payload));
    }

    // Retrieve the Claim named claim.
    auto claim(std::string_view claim) const {
        return detail::string_view(_payload, claim);
    }

    // Retrieve the Claim by JSON Pointer.
    std::optional<std::string_view> claim(const json::Pointer& p) const {
        auto claim = p.Get(_payload);
        if (!claim || !claim->IsString()) {
            return std::nullopt;
        }
        return detail::as_string_view(*claim);
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
        const auto is_aud = [aud](const auto& v) {
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
    friend std::ostream& operator<<(std::ostream& os, const jwt& jwt) {
        const auto write = [](std::ostream& os, const auto& doc) {
            json::OStreamWrapper osw(os);
            json::Writer<json::OStreamWrapper> h{osw};
            doc.Accept(h);
        };
        write(os, jwt._header);
        os << '.';
        write(os, jwt._payload);
        return os;
    }

    jwt(json::Document header, json::Document payload)
      : _header{std::move(header)}
      , _payload{std::move(payload)} {}
    json::Document _header;
    json::Document _payload;
};

namespace detail {

template<crypto::digest_type DigestType>
struct crypto_lib_sig_verifier {
    explicit crypto_lib_sig_verifier(crypto::key&& key)
      : _ctx(DigestType, key) {}

    bool operator()(bytes_view msg, bytes_view sig) const {
        return _ctx.update(msg).reset(sig);
    }

private:
    mutable crypto::verify_ctx _ctx;
};

template<crypto::digest_type DigestType>
struct crypto_lib_algorithm {
    using Verifier = crypto_lib_sig_verifier<DigestType>;
    using PublicKey = crypto::key;
};

// Base class for verifying and signing tokens
template<typename Algo, const char* Alg, const char* Kty>
class signature_base {
public:
    using algorithm = Algo;
    using verifier = algorithm::Verifier;

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

    bool verify(bytes_view msg, bytes_view sig) const {
        return _verifier(msg, sig);
    }

private:
    verifier _verifier;
};

constexpr const char rsa_str[] = "RSA";
constexpr const char rs256_str[] = "RS256";
using rs256_verifier = verifier_impl<
  crypto_lib_algorithm<crypto::digest_type::SHA256>,
  rs256_str,
  rsa_str>;

// Verify the signature of a message
class verifier {
public:
    template<typename Algo, const char* Alg, const char* Kty>
    explicit verifier(verifier_impl<Algo, Alg, Kty> v)
      : _impl(std::move(v)) {}

    auto alg() const {
        return ss::visit(_impl, [](const auto& impl) { return impl.alg(); });
    }

    auto kty() const {
        return ss::visit(_impl, [](const auto& impl) { return impl.kty(); });
    }

    bool verify(bytes_view msg, bytes_view sig) const {
        return ss::visit(
          _impl, [=](const auto& impl) { return impl.verify(msg, sig); });
    }

private:
    using verifier_impls = std::variant<rs256_verifier>;
    verifier_impls _impl;
};

inline result<verifier> make_rs256_verifier(const json::Value& jwk) {
    try {
        auto n = detail::base64_url_decode(jwk, "n");
        auto e = detail::base64_url_decode(jwk, "e");
        if (!n.has_value() || !e.has_value()) {
            return errc::jwk_invalid;
        }
        auto key = crypto::key::load_rsa_public_key(n.value(), e.value());
        return verifier{rs256_verifier{std::move(key)}};
    } catch (const base64_url_decoder_exception&) {
        return errc::jwk_invalid;
    } catch (const crypto::exception&) {
        return errc::jwk_invalid;
    }
}

using verifiers = absl::flat_hash_map<
  ss::sstring,
  detail::verifier,
  detail::string_viewable_hasher,
  detail::string_viewable_compare>;

inline result<verifiers> make_verifiers(const jwks& jwks) {
    auto keys = jwks.keys();
    verifiers vs;
    for (const auto& key : keys) {
        // NOTE(oren): 'alg' field is optional per RFC 7517
        // https://datatracker.ietf.org/doc/html/rfc7517#section-4.4
        // In particular, Azure doesn't include it, so in its absence we can
        // just try to verify as rs256 by default.
        auto alg = detail::string_view(key, "alg").value_or(rs256_str);

        // Use is optional, but must be sig if it exists
        auto use = detail::string_view(key, "use");
        if (use.value_or("sig") != "sig") {
            continue;
        }

        using factory = result<verifier> (*)(const json::Value&);
        auto v = string_switch<std::optional<factory>>(alg)
                   .match(rs256_str, &make_rs256_verifier)
                   .default_match(std::optional<factory>{});
        if (!v) {
            continue;
        }

        auto r = v.value()(key);
        if (!r) {
            continue;
        }

        vs.insert_or_assign(
          detail::string_view(key, "kid").value_or(""),
          std::move(r).assume_value());
    }
    if (vs.empty()) {
        return errc::jwks_invalid;
    }
    return vs;
}

} // namespace detail

class verifier {
public:
    explicit verifier() = default;

    // Verify the JWS signature and return the JWT
    result<jwt> verify(const jws& jws) const {
        std::string_view sv(jws._encoded);
        std::vector<std::string_view> jose_enc;
        jose_enc.reserve(3);
        boost::algorithm::split(
          jose_enc,
          detail::char_view_cast<std::string_view::value_type>(sv),
          [](char c) { return c == '.'; });

        if (jose_enc.size() != 3) {
            return errc::jws_invalid_parts;
        }

        constexpr auto make_dom =
          [](std::string_view bv) -> result<json::Document> {
            try {
                auto bytes = detail::base64_url_decode(bv);
                json::Document dom;
                dom.Parse(
                  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                  reinterpret_cast<const char*>(bytes.data()),
                  bytes.size());
                return dom;
            } catch (const base64_url_decoder_exception&) {
                return errc::jws_invalid_b64;
            }
        };

        auto header_dom = make_dom(jose_enc[0]);
        if (header_dom.has_error()) {
            return header_dom.assume_error();
        }

        auto payload_dom = make_dom(jose_enc[1]);
        if (payload_dom.has_error()) {
            return payload_dom.assume_error();
        }

        auto signature = detail::base64_url_decode(jose_enc[2]);

        auto jwt = jwt::make(
          std::move(header_dom).assume_value(),
          std::move(payload_dom).assume_value());
        if (jwt.has_error()) {
            return jwt.assume_error();
        }

        auto verifier = _verifiers.find(jwt.assume_value().kid().value());
        if (verifier == _verifiers.end()) {
            if (_verifiers.size() != 1) {
                return errc::kid_not_found;
            }
            verifier = _verifiers.begin();
        }

        auto second_dot = jose_enc[0].length() + 1 + jose_enc[1].length();
        auto msg = sv.substr(0, second_dot);
        bytes_view msg_view(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
          reinterpret_cast<const uint8_t*>(msg.data()),
          msg.size());
        if (!verifier->second.verify(msg_view, signature)) {
            return errc::jws_invalid_sig;
        }

        return jwt;
    }

    // Update the verification keys
    result<void> update_keys(const jwks& keys) {
        auto verifiers = detail::make_verifiers(keys);
        if (verifiers.has_error()) {
            return verifiers.assume_error();
        }
        _verifiers = std::move(verifiers).assume_value();
        return outcome::success();
    }

private:
    detail::verifiers _verifiers;
};

} // namespace security::oidc
