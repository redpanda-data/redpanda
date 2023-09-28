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

#include <optional>
#include <string_view>
#include <system_error>

namespace security::oidc {

enum errc {
    success = 0,
    metadata_invalid,
    jwks_invalid,
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

} // namespace security::oidc
