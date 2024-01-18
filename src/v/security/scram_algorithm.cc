#include "security/scram_algorithm.h"

#include "base/vlog.h"
#include "ssx/sformat.h"
#include "strings/utf8.h"
#include "utils/base64.h"
#include "utils/to_string.h"

#include <boost/algorithm/string.hpp>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <charconv>

// ALPHA / DIGIT / "/" / "+"
// NOLINTNEXTLINE
#define BASE64_CHAR "[a-zA-Z0-9/+]"

// base64-4 = 4base64-char
// base64-3 = 3base64-char "="
// base64-2 = 2base64-char "=="
// base64   = *base64-4 [base64-3 / base64-2]
// NOLINTNEXTLINE
#define BASE64                                                                 \
    "(?:" BASE64_CHAR "{4})*(?:" BASE64_CHAR "{3}=|" BASE64_CHAR "{2}==)?"

// Printable ASCII except ","
// NOLINTNEXTLINE
#define PRINTABLE "[\\x21-\\x2b\\x2d-\\x7e]+"

// NOLINTNEXTLINE
#define EXTENSIONS "(,[a-zA-Z]+=[\\x01-\\x2b\\x2d-\\x7f]+)*"

// UTF8-char except NUL, "=", and ","
// NOLINTNEXTLINE
#define VALUE_SAFE_CHAR "[\\x01-\\x2b\\x2d-\\x3c\\x3e-\\x7f]"

// NOLINTNEXTLINE
#define VALUE_SAFE VALUE_SAFE_CHAR "+"

// 1*(value-safe-char / "=2C" / "=3D")
// NOLINTNEXTLINE
#define SASLNAME "(?:" VALUE_SAFE_CHAR "|=2C|=3D)+"

#define BARE_SASLNAME "(" SASLNAME ")"

// value-char = value-safe-char / "="
// value = 1*value-char
// NOLINTNEXTLINE
#define VALUE "[\\x01-\\x2b\\x2d-\\x7f]+"

// NOLINTNEXTLINE
#define ALPHA "[a-zA-Z]"

#define CLIENT_FIRST_MESSAGE_RE                                                \
    "n,(?:a=(" SASLNAME "))?,(m=" VALUE ",)?n=(" SASLNAME "),"                 \
    "r=(" PRINTABLE ")(," ALPHA "+=" VALUE ")*"

#define SERVER_FIRST_MESSAGE_RE                                                \
    "(m=" VALUE ",)?r=(" PRINTABLE "),s=(" BASE64 "),i=([0-9]+)" EXTENSIONS

#define CLIENT_FINAL_MESSAGE_RE                                                \
    "c=(" BASE64 "),r=(" PRINTABLE ")" EXTENSIONS ",p=(" BASE64 ")"

#define SERVER_FINAL_MESSAGE_RE                                                \
    "(?:e=(" VALUE_SAFE "))|(?:v=(" BASE64 "))" EXTENSIONS

struct client_first_match {
    ss::sstring authzid;
    ss::sstring username;
    ss::sstring nonce;
    ss::sstring extensions;
};

struct server_first_match {
    ss::sstring nonce;
    bytes salt;
    int iterations;
};

struct client_final_match {
    bytes channel_binding;
    ss::sstring nonce;
    ss::sstring extensions;
    bytes proof;
};

struct server_final_match {
    std::optional<ss::sstring> error;
    bytes signature;
};

/*
 * some older versions of re2 don't have operator for implicit cast to
 * string_view so add this helper to support older re2.
 */
static std::string_view spv(const re2::StringPiece& sp) {
    return {sp.data(), sp.size()};
}

static std::optional<client_first_match>
parse_client_first(std::string_view message) {
    static thread_local const re2::RE2 re(
      CLIENT_FIRST_MESSAGE_RE, re2::RE2::Quiet);
    vassert(re.ok(), "client-first-message regex failure: {}", re.error());

    re2::StringPiece authzid;
    re2::StringPiece username;
    re2::StringPiece nonce;
    re2::StringPiece extensions;

    if (!re2::RE2::FullMatch(
          message, re, &authzid, nullptr, &username, &nonce, &extensions)) {
        return std::nullopt;
    }

    return client_first_match{
      .authzid = ss::sstring(spv(authzid)),
      .username = ss::sstring(spv(username)),
      .nonce = ss::sstring(spv(nonce)),
      .extensions = ss::sstring(spv(extensions)),
    };
}

static std::optional<server_first_match>
parse_server_first(std::string_view message) {
    static thread_local const re2::RE2 re(
      SERVER_FIRST_MESSAGE_RE, re2::RE2::Quiet);
    vassert(re.ok(), "server-first-message regex failure: {}", re.error());

    re2::StringPiece nonce;
    re2::StringPiece salt;
    int iterations; // NOLINT

    if (!re2::RE2::FullMatch(
          message, re, nullptr, &nonce, &salt, &iterations)) {
        return std::nullopt;
    }

    return server_first_match{
      .nonce = ss::sstring(spv(nonce)),
      .salt = base64_to_bytes(spv(salt)),
      .iterations = iterations,
    };
}

static std::optional<client_final_match>
parse_client_final(std::string_view message) {
    static thread_local const re2::RE2 re(
      CLIENT_FINAL_MESSAGE_RE, re2::RE2::Quiet);
    vassert(re.ok(), "client-final-message regex failure: {}", re.error());

    re2::StringPiece channel_binding;
    re2::StringPiece nonce;
    re2::StringPiece extensions;
    re2::StringPiece proof;

    if (!re2::RE2::FullMatch(
          message, re, &channel_binding, &nonce, &extensions, &proof)) {
        return std::nullopt;
    }

    return client_final_match{
      .channel_binding = base64_to_bytes(spv(channel_binding)),
      .nonce = ss::sstring(spv(nonce)),
      .extensions = ss::sstring(spv(extensions)),
      .proof = base64_to_bytes(spv(proof)),
    };
}

static std::optional<server_final_match>
parse_server_final(std::string_view message) {
    static thread_local const re2::RE2 re(
      SERVER_FINAL_MESSAGE_RE, re2::RE2::Quiet);
    vassert(re.ok(), "server-final-message regex failure: {}", re.error());

    re2::StringPiece error;
    re2::StringPiece signature;

    if (!re2::RE2::FullMatch(message, re, &error, &signature)) {
        return std::nullopt;
    }

    if (error.empty()) {
        return server_final_match{
          .signature = base64_to_bytes(spv(signature)),
        };
    }
    return server_final_match{.error = ss::sstring(spv(error))};
}

static std::optional<ss::sstring> parse_saslname(std::string_view message) {
    static thread_local const re2::RE2 re(BARE_SASLNAME, re2::RE2::Quiet);
    vassert(re.ok(), "saslname regex failure: {}", re.error());

    re2::StringPiece username;

    if (!re2::RE2::FullMatch(message, re, &username)) {
        return std::nullopt;
    }

    return ss::sstring(spv(username));
}

namespace security {

client_first_message::client_first_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);
    validate_no_control(view);

    auto match = parse_client_first(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          ssx::sformat, "Invalid SCRAM client first message: {}", view));
    }

    _authzid = std::move(match->authzid);
    _username = std::move(match->username);
    _nonce = std::move(match->nonce);

    if (match->extensions.empty()) {
        return;
    }

    // split on "," following the "," prefix
    std::vector<std::string> extension_pairs;
    boost::split(extension_pairs, match->extensions.substr(1), [](char c) {
        return c == ',';
    });

    // split pairs on first "=". the value part may also contain "="
    for (const auto& pair : extension_pairs) {
        auto it = std::find(pair.cbegin(), pair.cend(), '=');
        if (unlikely(it == pair.cend())) {
            throw scram_exception(fmt_with_ctx(
              ssx::sformat, "Invalid SCRAM client first message: {}", view));
        }
        _extensions.emplace(
          ss::sstring(pair.cbegin(), it), ss::sstring(it + 1, pair.cend()));
    }
}

ss::sstring client_first_message::username_normalized() const {
    auto normalized = boost::replace_all_copy(_username, "=2C", ",");
    const auto num_eq = std::count(normalized.cbegin(), normalized.cend(), '=');
    boost::replace_all(normalized, "=3D", "=");
    if (std::count(normalized.cbegin(), normalized.cend(), '=') != num_eq) {
        throw scram_exception(
          fmt_with_ctx(ssx::sformat, "Invalid SCRAM username: {}", _username));
    }
    return normalized;
}

bool client_first_message::token_authenticated() const {
    if (auto it = _extensions.find("tokenauth"); it != _extensions.end()) {
        return boost::iequals(it->second, "true");
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const client_first_message&) {
    // NOTE: this stream is intentially left minimal to err away from exposing
    // anything that may be useful for an attacker to use.
    return os << "{client_first_message}";
}

ss::sstring server_first_message::sasl_message() const {
    return ssx::sformat(
      "r={},s={},i={}", _nonce, bytes_to_base64(_salt), _iterations);
}

std::ostream& operator<<(std::ostream& os, const server_first_message&) {
    // NOTE: this stream is intentially left minimal to err away from exposing
    // anything that may be useful for an attacker to use.
    return os << "{server_first_message}";
}

client_final_message::client_final_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);
    validate_no_control(view);

    auto match = parse_client_final(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          ssx::sformat, "Invalid SCRAM client final message: {}", view));
    }

    _channel_binding = std::move(match->channel_binding);
    _nonce = std::move(match->nonce);
    _extensions = std::move(match->extensions);
    _proof = std::move(match->proof);
}

ss::sstring client_final_message::msg_no_proof() const {
    return ssx::sformat("c={},r={}", bytes_to_base64(_channel_binding), _nonce);
}

std::ostream& operator<<(std::ostream& os, const client_final_message&) {
    // NOTE: this stream is intentially left minimal to err away from exposing
    // anything that may be useful for an attacker to use.
    return os << "{client_final_message}";
}

ss::sstring server_final_message::sasl_message() const {
    if (_error) {
        return ssx::sformat("e={}", *_error);
    }
    return ssx::sformat("v={}", bytes_to_base64(_signature));
}

std::ostream& operator<<(std::ostream& os, const server_final_message&) {
    // NOTE: this stream is intentially left minimal to err away from exposing
    // anything that may be useful for an attacker to use.
    return os << "{server_final_message}";
}

server_first_message::server_first_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);
    validate_no_control(view);

    auto match = parse_server_first(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          ssx::sformat, "Invalid SCRAM server first message: {}", view));
    }

    _nonce = std::move(match->nonce);
    _salt = std::move(match->salt);
    _iterations = match->iterations;

    if (unlikely(_iterations <= 0)) {
        throw scram_exception(fmt_with_ctx(
          ssx::sformat,
          "Invalid SCRAM server first message iterations: {}",
          _iterations));
    }
}

server_final_message::server_final_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);
    validate_no_control(view);

    auto match = parse_server_final(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          ssx::sformat, "Invalid SCRAM server final message: {}", view));
    }

    _error = std::move(match->error);
    _signature = std::move(match->signature);
}

bool validate_scram_username(std::string_view username) {
    auto match = parse_saslname(username);
    return match.has_value() && match.value() == username;
}

} // namespace security
