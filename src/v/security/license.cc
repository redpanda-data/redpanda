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

#include "json/document.h"
#include "utils/base64.h"

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/gregorian/parsers.hpp>
#include <boost/filesystem.hpp>
#include <cryptopp/base64.h>
#include <cryptopp/rsa.h>
#include <cryptopp/sha.h>

namespace security {

namespace crypto {

static ss::sstring parse_pem_contents(const ss::sstring& pem_key) {
    static ss::sstring public_key_header = "-----BEGIN PUBLIC KEY-----";
    static ss::sstring public_key_footer = "-----END PUBLIC KEY-----";

    size_t pos1{ss::sstring::npos}, pos2{ss::sstring::npos};
    pos1 = pem_key.find(public_key_header);
    if (pos1 == ss::sstring::npos) {
        throw std::runtime_error(
          "Embedded public key error: PEM header not found");
    }

    pos2 = pem_key.find(public_key_footer, pos1 + 1);
    if (pos2 == ss::sstring::npos) {
        throw std::runtime_error(
          "Embedded public key error: PEM footer not found");
    }

    // Start position and length
    pos1 = pos1 + public_key_header.length();
    pos2 = pos2 - pos1;
    return pem_key.substr(pos1, pos2);
}

static CryptoPP::ByteQueue convert_pem_to_ber(const ss::sstring& pem_key) {
    const ss::sstring keystr = parse_pem_contents(pem_key);
    CryptoPP::StringSource ss{keystr.c_str(), true};

    // Base64 decode, place in a ByteQueue
    CryptoPP::ByteQueue queue;
    CryptoPP::Base64Decoder decoder;
    decoder.Attach(new CryptoPP::Redirector(queue));
    ss.TransferTo(decoder);
    decoder.MessageEnd();
    return queue;
}

static const CryptoPP::RSA::PublicKey public_key = []() {
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
    auto queue = convert_pem_to_ber(public_key_material);
    CryptoPP::RSA::PublicKey public_key;
    public_key.BERDecode(queue);
    return public_key;
}();

/// The redpanda license is comprised of 2 sections seperated by a delimiter.
/// The first section is the data section (base64 encoded), the second being the
/// signature, which is a PCKS1.5 sigature of the contents of the data section.
static bool
verify_license(const ss::sstring& data, const ss::sstring& signature) {
    CryptoPP::RSASS<CryptoPP::PKCS1v15, CryptoPP::SHA256>::Verifier verifier(
      public_key);
    return verifier.VerifyMessage(
      reinterpret_cast<const CryptoPP::byte*>(data.c_str()), // NOLINT
      data.length(),
      reinterpret_cast<const CryptoPP::byte*>(signature.data()), // NOLINT
      signature.size());
}

} // namespace crypto

ss::sstring license_type_to_string(license_type type) {
    switch (type) {
    case license_type::free_trial:
        return "free_trial";
    case license_type::enterprise:
        return "enterprise";
    default:
        __builtin_unreachable();
    }
    return "";
}

static license_type integer_to_license_type(int type) {
    switch (type) {
    case 0:
        return license_type::free_trial;
    case 1:
        return license_type::enterprise;
    default:
        throw license_invalid_exception("Unknown license_type");
    }
}

std::ostream& operator<<(std::ostream& os, const license& lic) {
    fmt::print(os, "{}", lic);
    return os;
}

struct license_components {
    ss::sstring data;
    ss::sstring signature;
};

static license_components parse_license(const ss::sstring& license) {
    static constexpr auto signature_delimiter = ".";
    const auto itr = license.find(signature_delimiter);
    if (itr == ss::sstring::npos) {
        throw license_malformed_exception("Outer envelope malformed");
    }
    /// signature encoded b64 first before encoding as utf-8 string, this is
    /// done so that it can have a utf-8 interpretation so the license file
    /// doesn't have to be in binary format
    return license_components{
      .data = license.substr(0, itr),
      .signature = base64_to_string(
        license.substr(itr + strlen(signature_delimiter)))};
}

static void parse_data_section(license& lc, const json::Document& doc) {
    auto parse_int = [](auto& value) {
        if (!value.IsInt()) {
            throw license_malformed_exception("Bad cast: expected int");
        }
        return value.GetInt();
    };
    auto parse_str = [](auto& value) -> std::string {
        if (!value.IsString()) {
            throw license_malformed_exception("Bad cast: expected string");
        }
        return value.GetString();
    };

    auto parse_expiry = [&](auto& value) -> boost::gregorian::date {
        auto expiry_str = parse_str(value);
        boost::gregorian::date expiry_date;
        try {
            expiry_date = boost::gregorian::from_simple_string(expiry_str);
        } catch (const boost::bad_lexical_cast& ex) {
            throw license_malformed_exception(
              fmt::format("Bad cast: Expiry dateformat: {}", ex.what()));
        }
        if (expiry_date.is_not_a_date()) {
            throw license_malformed_exception(
              "Expiration date not a real calendar date");
        }
        const auto today = boost::gregorian::day_clock::universal_day();
        if (expiry_date < today) {
            throw license_invalid_exception("Expiry date behind todays date");
        }
        return expiry_date;
    };

    static const std::array<ss::sstring, 4> v0_license_schema{
      "version", "org", "type", "expiry"};

    const size_t schema_keys_found = std::count_if(
      doc.MemberBegin(), doc.MemberEnd(), [&](const auto& item) {
          return std::find(
                   v0_license_schema.begin(),
                   v0_license_schema.end(),
                   parse_str(item.name))
                 != v0_license_schema.end();
      });
    if (schema_keys_found < v0_license_schema.size()) {
        throw license_malformed_exception(
          "Missing expected parameters from license");
    }
    lc.format_version = parse_int(doc.FindMember("version")->value);
    if (lc.format_version < 0) {
        throw license_invalid_exception("Invalid format_version, is < 0");
    }

    lc.organization = parse_str(doc.FindMember("org")->value);
    if (lc.organization == "") {
        throw license_invalid_exception("Cannot have empty string for org");
    }
    lc.type = integer_to_license_type(parse_int(doc.FindMember("type")->value));
    lc.expiry = parse_expiry(doc.FindMember("expiry")->value);
}

license make_license(const ss::sstring& raw_license) {
    license lc;
    auto components = parse_license(raw_license);
    if (!crypto::verify_license(components.data, components.signature)) {
        throw license_verifcation_exception("RSA signature invalid");
    }
    try {
        auto decoded_data = base64_to_string(components.data);
        json::Document doc;
        doc.Parse(decoded_data);
        if (doc.HasParseError()) {
            throw license_malformed_exception("Malformed data section");
        }
        parse_data_section(lc, doc);
        return lc;
    } catch (const base64_decoder_exception&) {
        throw license_malformed_exception("Failed to decode data section");
    }
}

bool license::is_expired() const noexcept {
    return expiry < boost::gregorian::day_clock::universal_day();
}

long license::days_until_expires() const noexcept {
    return is_expired()
             ? -1
             : (expiry - boost::gregorian::day_clock::universal_day()).days();
}

void license::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    format_version = read_nested<uint8_t>(in, h._bytes_left_limit);
    type = read_nested<license_type>(in, h._bytes_left_limit);
    organization = read_nested<ss::sstring>(in, h._bytes_left_limit);
    expiry = boost::gregorian::from_simple_string(
      read_nested<ss::sstring>(in, h._bytes_left_limit));
}

void license::serde_write(iobuf& out) {
    using serde::write;
    write(out, format_version);
    write(out, type);
    write(out, organization);
    write(
      out,
      ss::sstring(boost::gregorian::to_iso_extended_string_type<char>(expiry)));
}

} // namespace security

namespace reflection {

void adl<security::license>::to(iobuf& out, security::license&& l) {
    vassert(true, "security::license should always use serde, never adl");
    reflection::serialize(
      out,
      l.format_version,
      l.type,
      std::move(l.organization),
      ss::sstring(
        boost::gregorian::to_iso_extended_string_type<char>(l.expiry)));
}

security::license adl<security::license>::from(iobuf_parser& in) {
    vassert(true, "security::license should always use serde, never adl");
    auto format_version = adl<uint8_t>{}.from(in);
    auto type = adl<security::license_type>{}.from(in);
    auto org = adl<ss::sstring>{}.from(in);
    auto expiry = adl<ss::sstring>{}.from(in);
    return security::license{
      .format_version = format_version,
      .type = type,
      .organization = std::move(org),
      .expiry = boost::gregorian::from_simple_string(expiry)};
}

} // namespace reflection

namespace fmt {
template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::license, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::license& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return format_to(
      ctx.out(),
      "[Version: {0}, Organization: {1}, Type: {2} Expiry(days): {3}]",
      r.format_version,
      r.organization,
      license_type_to_string(r.type),
      r.days_until_expires());
}

} // namespace fmt
