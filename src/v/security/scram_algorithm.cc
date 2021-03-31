#include "security/scram_algorithm.h"

#include "utils/base64.h"
#include "utils/to_string.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <boost/algorithm/string.hpp>

/*
 * Likely an issue with gcc, memory usage is unacceptably high using c++20 with
 * these regular expressions and the ctre library. Since this is not a problem
 * with clang and our release builds use clang, we fall back to slow std::regex
 * when using gcc. The following ticket is tracking the issue, and the fallback
 * can be removed once the issue is resolved:
 *
 * https://github.com/hanickadot/compile-time-regular-expressions/issues/155
 */
#undef USE_CTRE
#ifdef __clang__
#define USE_CTRE
#include <ctll.hpp>
#include <ctre.hpp>
#else
#include <regex>
#endif

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

// 1*(value-safe-char / "=2C" / "=3D")
// NOLINTNEXTLINE
#define SASLNAME "(?:" VALUE_SAFE_CHAR "|=2C|=3D)+"

// value-char = value-safe-char / "="
// value = 1*value-char
// NOLINTNEXTLINE
#define VALUE "[\\x01-\\x2b\\x2d-\\x7f]+"

// NOLINTNEXTLINE
#define ALPHA "[a-zA-Z]"

#define CLIENT_FIRST_MESSAGE_RE                                                \
    "n,(?:a=(" SASLNAME "))?,(m=" VALUE ",)?n=(" SASLNAME "),"                 \
    "r=(" PRINTABLE ")(," ALPHA "+=" VALUE ")*"

#define CLIENT_FINAL_MESSAGE_RE                                                \
    "c=(" BASE64 "),r=(" PRINTABLE ")" EXTENSIONS ",p=(" BASE64 ")"

/*
 * {ctre,std_re}_parse_client_{first,final} implementations are defined. ctre_*
 * with clang, and std_re* for gcc. a generic wrapper compiles with the correct
 * version which scram algorithms use directly.
 */
namespace details {
struct client_first_match {
    ss::sstring authzid;
    ss::sstring username;
    ss::sstring nonce;
    ss::sstring extensions;
};

struct client_final_match {
    bytes channel_binding;
    ss::sstring nonce;
    ss::sstring extensions;
    bytes proof;
};

#ifdef USE_CTRE
static constexpr auto client_first_message_re = ctll::fixed_string{
  CLIENT_FIRST_MESSAGE_RE};

static constexpr auto client_final_message_re = ctll::fixed_string{
  CLIENT_FINAL_MESSAGE_RE};

static inline std::optional<client_first_match>
ctre_parse_client_first(std::string_view message) {
    auto match = ctre::match<client_first_message_re>(message);
    if (unlikely(!match)) {
        return std::nullopt;
    }
    return client_first_match{
      .authzid = match.get<1>().to_string(),
      .username = match.get<3>().to_string(),
      .nonce = match.get<4>().to_string(),
      .extensions = match.get<5>().to_string(), // NOLINT
    };
}

static inline std::optional<client_final_match>
ctre_parse_client_final(std::string_view message) {
    auto match = ctre::match<client_final_message_re>(message);
    if (unlikely(!match)) {
        return std::nullopt;
    }
    return client_final_match{
      .channel_binding = base64_to_bytes(match.get<1>().to_view()),
      .nonce = match.get<2>().to_string(),
      .extensions = match.get<3>().to_string(),
      .proof = base64_to_bytes(match.get<4>().to_view()),
    };
}

#else
static const char* client_first_message_re = CLIENT_FIRST_MESSAGE_RE;
static const char* client_final_message_re = CLIENT_FINAL_MESSAGE_RE;

static inline std::optional<client_first_match>
std_re_parse_client_first(std::string_view message) {
    static const std::regex re(
      client_first_message_re, std::regex::ECMAScript | std::regex::optimize);
    std::smatch match;
    std::string m(message);
    if (unlikely(!std::regex_match(m, match, re))) {
        return std::nullopt;
    }
    return client_first_match{
      .authzid = match[1].str(),
      .username = match[3].str(),
      .nonce = match[4].str(),
      .extensions = match[5].str(), // NOLINT
    };
}

static inline std::optional<client_final_match>
std_re_parse_client_final(std::string_view message) {
    static const std::regex re(
      client_final_message_re, std::regex::ECMAScript | std::regex::optimize);
    std::smatch match;
    std::string m(message);
    if (unlikely(!std::regex_match(m, match, re))) {
        return std::nullopt;
    }
    return client_final_match{
      .channel_binding = base64_to_bytes(match[1].str()),
      .nonce = match[2].str(),
      .extensions = match[3].str(),
      .proof = base64_to_bytes(match[4].str()),
    };
}
#endif
} // namespace details

static inline std::optional<details::client_first_match>
parse_client_first(std::string_view message) {
#ifdef USE_CTRE
    return details::ctre_parse_client_first(message);
#else
    return details::std_re_parse_client_first(message);
#endif
}

static inline std::optional<details::client_final_match>
parse_client_final(std::string_view message) {
#ifdef USE_CTRE
    return details::ctre_parse_client_final(message);
#else
    return details::std_re_parse_client_final(message);
#endif
}

namespace security {

std::ostream& operator<<(std::ostream& os, const scram_credential& cred) {
    fmt::print(
      os,
      "salt {} server_key {} stored_key {} iterations {}",
      cred._salt,
      cred._server_key,
      cred._stored_key,
      cred._iterations);
    return os;
}

client_first_message::client_first_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);

    auto match = parse_client_first(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          fmt::format, "Invalid SCRAM client first message: {}", view));
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
              fmt::format, "Invalid SCRAM client first message: {}", view));
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
          fmt_with_ctx(fmt::format, "Invalid SCRAM username: {}", _username));
    }
    return normalized;
}

bool client_first_message::token_authenticated() const {
    if (auto it = _extensions.find("tokenauth"); it != _extensions.end()) {
        return boost::iequals(it->second, "true");
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const client_first_message& msg) {
    fmt::print(
      os,
      "authzid {} username {} nonce {}",
      msg._authzid,
      msg._username,
      msg._nonce);
    return os;
}

ss::sstring server_first_message::sasl_message() const {
    return fmt::format(
      "r={},s={},i={}", _nonce, bytes_to_base64(_salt), _iterations);
}

std::ostream& operator<<(std::ostream& os, const server_first_message& msg) {
    fmt::print(
      os,
      "nonce {} salt {} iterations {}",
      msg._nonce,
      msg._salt,
      msg._iterations);
    return os;
}

client_final_message::client_final_message(bytes_view data) {
    auto view = std::string_view(
      reinterpret_cast<const char*>(data.data()), data.size()); // NOLINT
    validate_utf8(view);

    auto match = parse_client_final(view);
    if (unlikely(!match)) {
        throw scram_exception(fmt_with_ctx(
          fmt::format, "Invalid SCRAM client final message: {}", view));
    }

    _channel_binding = std::move(match->channel_binding);
    _nonce = std::move(match->nonce);
    _extensions = std::move(match->extensions);
    _proof = std::move(match->proof);
}

ss::sstring client_final_message::msg_no_proof() const {
    return fmt::format("c={},r={}", bytes_to_base64(_channel_binding), _nonce);
}

std::ostream& operator<<(std::ostream& os, const client_final_message& msg) {
    fmt::print(
      os,
      "channel {} nonce {} extensions {} proof {}",
      msg._channel_binding,
      msg._nonce,
      msg._extensions,
      msg._proof);
    return os;
}

ss::sstring server_final_message::sasl_message() const {
    if (_error) {
        return fmt::format("e={}", *_error);
    }
    return fmt::format("v={}", bytes_to_base64(_signature));
}

std::ostream& operator<<(std::ostream& os, const server_final_message& msg) {
    fmt::print(os, "error {} signature {}", msg._error, msg._signature);
    return os;
}

} // namespace security
