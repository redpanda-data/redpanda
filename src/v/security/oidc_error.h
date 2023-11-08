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

#include <system_error>

namespace security::oidc {

enum class errc {
    success = 0,
    metadata_invalid,
    jwks_invalid,
    jwk_invalid,
    jws_invalid_parts,
    jws_invalid_b64,
    jws_invalid_sig,
    jwt_invalid_json,
    jwt_invalid_typ,
    jwt_invalid_alg,
    jwt_invalid_kid,
    jwt_invalid_iss,
    jwt_invalid_aud,
    jwt_invalid_exp,
    jwt_invalid_iat,
    jwt_invalid_nbf,
    jwt_invalid_sub,
    jwt_invalid_principal,
    kid_not_found,
    invalid_principal_mapping,
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
        case errc::jws_invalid_parts:
            return "Invalid jws: Expected three parts, is the JWT signed?";
        case errc::jws_invalid_b64:
            return "Invalid jws: Base64UrlDecode failed";
        case errc::jws_invalid_sig:
            return "Invalid jws: Signature failed";
        case errc::jwt_invalid_json:
            return "Invalid jwt: Not JSON";
        case errc::jwt_invalid_typ:
            return "Invalid jwt.typ";
        case errc::jwt_invalid_alg:
            return "Invalid jwt.alg";
        case errc::jwt_invalid_kid:
            return "Invalid jwt.kid";
        case errc::jwt_invalid_iss:
            return "Invalid jwt.iss";
        case errc::jwt_invalid_aud:
            return "Invalid jwt.aud";
        case errc::jwt_invalid_exp:
            return "Invalid jwt.exp";
        case errc::jwt_invalid_iat:
            return "Invalid jwt.iat";
        case errc::jwt_invalid_nbf:
            return "Invalid jwt.nbf";
        case errc::jwt_invalid_sub:
            return "Invalid jwt.sub";
        case errc::jwt_invalid_principal:
            return "Invalid jwt claim, it must result in a non-empty string";
        case errc::kid_not_found:
            return "kid not found";
        case errc::invalid_principal_mapping:
            return "invalid principal mapping rule";
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
