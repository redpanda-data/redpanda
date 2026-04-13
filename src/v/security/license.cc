/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "security/license.h"

#include "crypto/crypto.h"
#include "crypto/types.h"
#include "hashing/secure.h"
#include "json/document.h"
#include "json/validator.h"
#include "utils/base64.h"

#include <algorithm>
#include <chrono>

using namespace std::chrono_literals;

namespace security {

namespace crypto {

namespace {
const ::crypto::key& public_key() {
    // Defer the loading of the public key until OpenSSL has been initialized by
    // ossl_context_service
    static const ::crypto::key result = []() {
        static const ss::sstring public_key_material
          = "-----BEGIN PUBLIC KEY-----\n"
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt0Y2jGOLI70xkF4rmpNM\n"
            "hBqU3cUrwYCREgjT9TT77KusvhPVc16cdK83bpaGQy+Or1WyZpN+TCxT2vlaZet6\n"
            "RDo+55jRk7epazAHx9s+DLd6IzhSXakf6Sxh5JRK7Zn/75C1hYJMspcJ75EhLv4H\n"
            "qXj12dkyivcLAecGhWdIGK95J0P7f4EQQGwGL3rilCSlfkVVmE4qaPUaLqULKelq\n"
            "7T2d+AklR+KwgtHINyKDPJ9+cCAMoEOrRBDPjcQ79k0yvP3BdHV394F+2Vt/AYOL\n"
            "dcVQBm3tqIySLGFtiJp+RIa+nJhMrd+G4sqwm4FhsmG35Fbr0XQJY0sM6MaFJcDH\n"
            "swIDAQAB\n"
            "-----END PUBLIC KEY-----\n";
        return ::crypto::key::load_key(
          public_key_material,
          ::crypto::format_type::PEM,
          ::crypto::is_private_key_t::no);
    }();
    return result;
}

/// The redpanda license is comprised of 2 sections seperated by a delimiter.
/// The first section is the data section (base64 encoded), the second being the
/// signature, which is a PCKS1.5 sigature of the contents of the data section.
bool verify_license(const ss::sstring& data, const ss::sstring& signature) {
    return ::crypto::verify_signature(
      ::crypto::digest_type::SHA256, public_key(), data, signature);
}
} // namespace

} // namespace crypto

namespace {
ss::sstring license_type_to_string(license_type type) {
    switch (type) {
    case license_type::free_trial:
        return "free_trial";
    case license_type::enterprise:
        return "enterprise";
    }
    __builtin_unreachable();
}

license_type integer_to_license_type(int type) {
    switch (type) {
    case 0:
        return license_type::free_trial;
    case 1:
        return license_type::enterprise;
    default:
        throw license_invalid_exception(
          ss::format("Unknown license_type: {}", type));
    }
}

struct license_components {
    ss::sstring data;
    ss::sstring signature;
};

license_components parse_license(std::string_view license) {
    static constexpr auto signature_delimiter = ".";
    const auto itr = license.find(signature_delimiter);
    if (itr == ss::sstring::npos) {
        throw license_malformed_exception("Outer envelope malformed");
    }
    /// signature encoded b64 first before encoding as utf-8 string, this is
    /// done so that it can have a utf-8 interpretation so the license file
    /// doesn't have to be in binary format
    return license_components{
      .data = ss::sstring{license.substr(0, itr)},
      .signature = base64_to_string(
        license.substr(itr + strlen(signature_delimiter)))};
}

constexpr std::string_view license_data_validator_schema_v0 = R"(
{
    "type": "object",
    "properties": {
        "version": {
            "type": "number"
        },
        "org": {
            "type": "string"
        },
        "type": {
            "type": "number"
        },
        "expiry": {
            "type": "number"
        }
    },
    "required": [
        "version",
        "org",
        "type",
        "expiry"
    ],
    "additionalProperties": false
}
)";

constexpr std::string_view license_data_validator_schema_v1 = R"(
{
    "type": "object",
    "properties": {
        "version": {
            "type": "number"
        },
        "org": {
            "type": "string"
        },
        "type": {
            "type": "string"
        },
        "expiry": {
            "type": "number"
        },
        "products": {
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    },
    "required": [
        "version",
        "org",
        "type",
        "expiry"
    ],
    "additionalProperties": true
}
)";

struct license_data_parser {
    using data_parser = void (*)(license& lc, const json::Document& doc);

    std::string_view schema;
    data_parser parser;
};

// parse_data_section_v* functions are responsible for parsing all values of the
// license relevant to each version, apart from 'format_version' and 'checksum'
void parse_data_section_v0(license& lc, const json::Document& doc) {
    lc.expiry = std::chrono::seconds(
      doc.FindMember("expiry")->value.GetInt64());
    if (lc.is_expired()) {
        throw license_invalid_exception("Expiry date behind todays date");
    }
    lc.organization = doc.FindMember("org")->value.GetString();
    if (lc.organization == "") {
        throw license_invalid_exception("Cannot have empty string for org");
    }
    lc._type = integer_to_license_type(doc.FindMember("type")->value.GetInt());
}

void parse_data_section_v1(license& lc, const json::Document& doc) {
    lc.expiry = std::chrono::seconds(
      doc.FindMember("expiry")->value.GetInt64());
    if (lc.is_expired()) {
        throw license_invalid_exception("Expiry date behind todays date");
    }
    lc.organization = doc.FindMember("org")->value.GetString();
    if (lc.organization == "") {
        throw license_invalid_exception("Cannot have empty string for org");
    }
    lc._type_str = doc.FindMember("type")->value.GetString();

    const auto it_products = doc.FindMember("products");
    if (it_products != doc.MemberEnd()) {
        lc.products = it_products->value.GetArray()
                      | std::views::transform(&json::Value::GetString)
                      | std::ranges::to<decltype(lc.products)>();
    }
}

license_data_parser get_parser(const uint8_t version) {
    switch (version) {
    case 0:
        return license_data_parser{
          .schema = license_data_validator_schema_v0,
          .parser = parse_data_section_v0};
    case 1:
        return license_data_parser{
          .schema = license_data_validator_schema_v1,
          .parser = parse_data_section_v1};
    default:
        throw license_invalid_exception(
          ss::format("Unsupported version {}", version));
    }
}

void parse_data_section(license& lc, const json::Document& doc) {
    const auto it_version = doc.FindMember("version");
    if (it_version == doc.MemberEnd()) {
        throw license_invalid_exception("Missing required parameter 'version'");
    }
    lc.format_version = it_version->value.GetInt();
    const auto ldf = get_parser(lc.format_version);

    json::validator license_data_validator(ldf.schema);
    if (!doc.Accept(license_data_validator.schema_validator)) {
        json::StringBuffer sb;
        json::Writer<json::StringBuffer> writer(sb);
        license_data_validator.schema_validator.GetError().Accept(writer);
        throw license_malformed_exception(
          ss::format(
            "License data section failed to match schema with error: {}",
            sb.GetString()));
    }
    ldf.parser(lc, doc);
}

ss::sstring calculate_sha256_checksum(std::string_view raw_license) {
    bytes checksum;
    hash_sha256 h;
    h.update(raw_license);
    const auto digest = h.reset();
    checksum.resize(digest.size());
    std::copy_n(digest.begin(), digest.size(), checksum.begin());
    return to_hex(checksum);
}

} // namespace

std::ostream& operator<<(std::ostream& os, const license& lic) {
    fmt::print(os, "{}", lic);
    return os;
}

license make_license(std::string_view raw_license) {
    try {
        license lc{};
        auto components = parse_license(raw_license);
        if (!crypto::verify_license(components.data, components.signature)) {
            throw license_verifcation_exception("RSA signature invalid");
        }
        auto decoded_data = base64_to_string(components.data);
        json::Document doc;
        doc.Parse(decoded_data);
        if (doc.HasParseError()) {
            throw license_malformed_exception("Malformed data section");
        }
        parse_data_section(lc, doc);
        lc.checksum = calculate_sha256_checksum(raw_license);
        return lc;
    } catch (const base64_decoder_exception&) {
        throw license_malformed_exception("Failed to decode data section");
    }
}

ss::sstring license::get_type() const {
    switch (format_version) {
    case 0:
        return license_type_to_string(_type);
    default:
        return _type_str;
    }
}

bool license::is_expired() const noexcept {
    return clock::now() > expiration();
}

license::clock::time_point license::expiration() const noexcept {
    return clock::time_point{expiry};
}

} // namespace security

namespace fmt {
template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::license, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::license& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(
      ctx.out(),
      "[Version: {0}, Organization: {1}, Type: {2}, Expiry(epoch): {3}{4}]",
      r.format_version,
      r.organization,
      r.get_type(),
      r.expiry.count(),
      r.products.empty()
        ? ""
        : fmt::format(", Products: {}", fmt::join(r.products, ",")));
}

} // namespace fmt
