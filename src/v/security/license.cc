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

#include "hashing/secure.h"
#include "json/document.h"
#include "json/validator.h"
#include "utils/base64.h"

#include <cryptopp/base64.h>
#include <cryptopp/rsa.h>
#include <cryptopp/sha.h>

namespace security {

namespace crypto {

static ss::sstring parse_pem_contents(const ss::sstring& pem_key) {
    static constexpr std::string_view public_key_header
      = "-----BEGIN PUBLIC KEY-----";
    static constexpr std::string_view public_key_footer
      = "-----END PUBLIC KEY-----";

    size_t pos1{ss::sstring::npos}, pos2{ss::sstring::npos};
    pos1 = pem_key.find(public_key_header.begin());
    if (pos1 == ss::sstring::npos) {
        throw std::runtime_error(
          "Embedded public key error: PEM header not found");
    }

    pos2 = pem_key.find(public_key_footer.begin(), pos1 + 1);
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
    }
    __builtin_unreachable();
}

static license_type integer_to_license_type(int type) {
    switch (type) {
    case 0:
        return license_type::free_trial;
    case 1:
        return license_type::enterprise;
    default:
        throw license_invalid_exception(
          fmt::format("Unknown license_type: {}", type));
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

static const std::string license_data_validator_schema = R"(
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

static void parse_data_section(license& lc, const json::Document& doc) {
    json::validator license_data_validator(license_data_validator_schema);
    if (!doc.Accept(license_data_validator.schema_validator)) {
        throw license_malformed_exception(
          "License data section failed to match schema");
    }
    lc.expiry = std::chrono::seconds(
      doc.FindMember("expiry")->value.GetInt64());
    if (lc.is_expired()) {
        throw license_invalid_exception("Expiry date behind todays date");
    }
    lc.format_version = doc.FindMember("version")->value.GetInt();
    if (lc.format_version < 0) {
        throw license_invalid_exception("Invalid format_version, is < 0");
    }
    lc.organization = doc.FindMember("org")->value.GetString();
    if (lc.organization == "") {
        throw license_invalid_exception("Cannot have empty string for org");
    }
    lc.type = integer_to_license_type(doc.FindMember("type")->value.GetInt());
}

static ss::sstring calculate_sha256_checksum(const ss::sstring& raw_license) {
    bytes checksum;
    hash_sha256 h;
    h.update(raw_license);
    const auto digest = h.reset();
    checksum.resize(digest.size());
    std::copy_n(digest.begin(), digest.size(), checksum.begin());
    return to_hex(checksum);
}

license make_license(const ss::sstring& raw_license) {
    try {
        license lc;
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

bool license::is_expired() const noexcept {
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch());
    return now > expiry;
}

} // namespace security

namespace fmt {
template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::license, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::license& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return format_to(
      ctx.out(),
      "[Version: {0}, Organization: {1}, Type: {2} Expiry(epoch): {3}]",
      r.format_version,
      r.organization,
      license_type_to_string(r.type),
      r.expiry.count());
}

} // namespace fmt
