/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/signature.h"

#include "bytes/bytes.h"
#include "cloud_roles/logger.h"
#include "hashing/secure.h"
#include "ssx/sformat.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string/compare.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <system_error>

namespace cloud_roles {

constexpr int sha256_digest_length = 32;
using hmac_digest = std::array<char, sha256_digest_length>;

constexpr std::string_view algorithm = "AWS4-HMAC-SHA256";

struct s3_error_category final : std::error_category {
    const char* name() const noexcept final { return "s3"; }
    std::string message(int ec) const final {
        switch (static_cast<s3_client_error_code>(ec)) {
        case s3_client_error_code::invalid_uri:
            return "Target URI shouldn't be empty or include domain name";
        case s3_client_error_code::invalid_uri_params:
            return "Target URI contains invalid query parameters";
        case s3_client_error_code::not_enough_arguments:
            return "Can't make request, not enough arguments";
        }
        return "unknown";
    }
};

std::error_code make_error_code(s3_client_error_code ec) noexcept {
    static s3_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

// time_source //

time_source::time_source()
  : time_source(&default_source, 0) {}

time_source::time_source(timestamp instant)
  : time_source([instant]() { return instant; }, 0) {}

ss::sstring time_source::format(const char* fmt) const {
    std::array<char, formatted_datetime_len> out_str{};
    auto point = _gettime_fn();
    std::time_t time = std::chrono::system_clock::to_time_t(point);
    std::tm* gm = std::gmtime(&time);
    auto ret = std::strftime(out_str.data(), out_str.size(), fmt, gm);
    vassert(ret > 0, "Invalid date format string");
    return ss::sstring(out_str.data(), ret);
}

ss::sstring time_source::format_date() const { return format("%Y%m%d"); }

ss::sstring time_source::format_datetime() const {
    return format("%Y%m%dT%H%M%SZ");
}

timestamp time_source::default_source() {
    return std::chrono::system_clock::now();
}

// signature_v4 //

inline ss::sstring hexdigest(const ss::sstring& digest) {
    std::array<uint8_t, sha256_digest_length> result{};
    std::memcpy(result.data(), digest.data(), digest.size());
    return to_hex(bytes_view{result.data(), result.size()});
}

inline ss::sstring hexdigest(const hmac_digest& digest) {
    return to_hex(digest);
}

static hmac_digest hmac(std::string_view key, std::string_view value) {
    hmac_sha256 h(key);
    h.update(value);
    return h.reset();
}

static ss::sstring sha_256(std::string_view str) {
    hash_sha256 s;
    s.update(str);
    auto hash = s.reset();
    return to_hex(hash);
}

ss::sstring signature_v4::gen_sig_key(
  std::string_view key,
  std::string_view datestr,
  std::string_view region,
  std::string_view service) {
    hmac_digest digest{};
    ss::sstring initial_key = "AWS4";
    initial_key.append(key.data(), key.size());
    digest = hmac(std::string_view(initial_key), datestr);
    digest = hmac(std::string_view(digest.data(), digest.size()), region);
    digest = hmac(std::string_view(digest.data(), digest.size()), service);
    digest = hmac(
      std::string_view(digest.data(), digest.size()), "aws4_request");
    return {digest.data(), digest.size()};
}

inline void tolower(ss::sstring& str) {
    for (auto& c : str) {
        c = std::tolower(c);
    }
}

inline void append_hex_utf8(ss::sstring& result, char ch) {
    bytes b = {static_cast<uint8_t>(ch)};
    result.append("%", 1);
    auto h = to_hex(b);
    result.append(h.data(), h.size());
}

ss::sstring uri_encode(const ss::sstring& input, bool encode_slash) {
    // The function defined here:
    //     https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    ss::sstring result;
    for (auto ch : input) {
        if (
          (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || (ch >= '0' && ch <= '9') || (ch == '_') || (ch == '-')
          || (ch == '~') || (ch == '.')) {
            result.append(&ch, 1);
        } else if (ch == '/') {
            if (encode_slash) {
                result.append("%2F", 3);
            } else {
                result.append(&ch, 1);
            }
        } else {
            append_hex_utf8(result, ch);
        }
    }
    return result;
}

/// \brief Get canonical URI of the request
/// Canonical URI is everything that follows domain name starting with '/'
/// without parameters (everythng after '?' including '?'). The uri is uri
/// encoded. e.g. https://foo.bar/canonical-url?param=value
///
/// \param uri is a target of the http query (everything excluding the domain
/// name)
static result<ss::sstring> get_canonical_uri(ss::sstring target) {
    if (target.empty() || target[0] != '/') {
        vlog(clrl_log.error, "invalid URI {}", target);
        return make_error_code(s3_client_error_code::invalid_uri);
    }
    auto pos = target.find('?');
    if (pos != ss::sstring::npos) {
        target.resize(pos);
    }
    return uri_encode(target, false);
}

/// CanonicalQueryString specifies the URI-encoded query string parameters.
/// You URI-encode name and values individually. You must also sort the
/// parameters in the canonical query string alphabetically by key name.
/// The sorting occurs after encoding. The query string in the following URI
/// example is prefix=somePrefix&marker=someMarker&max-keys=20:
/// "http://s3.amazonaws.com/examplebucket?prefix=somePrefix&marker=someMarker&max-keys=20"
///
/// The canonical query string is as follows (line breaks are added to this
/// example for readability):
///   UriEncode("marker")+"="+UriEncode("someMarker")+"&"+
///   UriEncode("max-keys")+"="+UriEncode("20") + "&" +
///   UriEncode("prefix")+"="+UriEncode("somePrefix")
///
/// \param target is a target of the http query (url - domain name)
static result<ss::sstring> get_canonical_query_string(ss::sstring target) {
    if (target.empty() || target[0] != '/') {
        vlog(clrl_log.error, "invalid URI {}", target);
        return make_error_code(s3_client_error_code::invalid_uri);
    }
    auto pos = target.find('?');
    if (pos == ss::sstring::npos || pos == target.size() - 1) {
        return "";
    }
    auto query_str = target.substr(pos + 1);
    std::vector<ss::sstring> params;
    boost::split(params, query_str, boost::is_any_of("&"));
    std::vector<std::pair<ss::sstring, ss::sstring>> query_params;
    for (const auto& param : params) {
        auto p = param.find('=');
        if (p == ss::sstring::npos) {
            // parameter with empty value
            query_params.emplace_back(param, "");
        } else {
            if (p == 0) {
                // parameter value can be empty but name can't
                return make_error_code(
                  s3_client_error_code::invalid_uri_params);
            }
            ss::sstring pname = param.substr(0, p);
            ss::sstring pvalue = param.substr(p + 1);
            query_params.emplace_back(pname, pvalue);
        }
    }
    std::sort(query_params.begin(), query_params.end());

    // Generate canonical query string
    ss::sstring result;
    int cnt = 0;
    for (const auto& [pname, pvalue] : query_params) {
        if (cnt++ > 0) {
            result.append("&", 1);
        }
        result += ssx::sformat(
          "{}={}", uri_encode(pname, true), uri_encode(pvalue, true));
    }
    return result;
}

struct canonical_headers {
    ss::sstring canonical_headers; // string that contains canonical headers
    ss::sstring signed_headers;    // string that contains list of header names
};

/// CanonicalHeaders is a list of request headers with their values.
/// Individual header name and value pairs are separated by the newline
/// character ("\n"). Header names must be in lowercase. You must sort the
/// header names alphabetically to construct the string, as shown in the
/// following example:
///
///   Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n"
///   Lowercase(<HeaderName2>)+":"+Trim(<value>)+"\n"
///   ...
///   Lowercase(<HeaderNameN>)+":"+Trim(<value>)+"\n"
///
/// The CanonicalHeaders list must include the following:
///
///   - HTTP host header.
///   - If the Content-Type header is present in the request, you must add it to
///   the
///     CanonicalHeaders list.
///   - Any x-amz-* headers that you plan to include in your request must also
///   be added.
///     For example, if you are using temporary security credentials, you need
///     to include x-amz-security-token in your request. You must add this
///     header in the list of CanonicalHeaders.
///
///
static result<canonical_headers>
get_canonical_headers(const http::client::request_header& request) {
    std::vector<std::pair<ss::sstring, ss::sstring>> headers;
    for (const auto& it : request) {
        auto name = it.name_string();
        auto value = it.value();
        ss::sstring n{name.data(), name.size()};
        ss::sstring v{value.data(), value.size()};
        tolower(n);
        boost::trim(v);
        headers.emplace_back(std::move(n), std::move(v));
    }
    std::sort(headers.begin(), headers.end());
    ss::sstring cheaders;
    ss::sstring snames;
    int cnt = 0;
    for (const auto& [name, value] : headers) {
        cheaders += ssx::sformat("{}:{}\n", name, value);
        if (cnt++ > 0) {
            snames.append(";", 1);
        }
        snames.append(name.data(), name.size());
    }
    return canonical_headers{
      .canonical_headers = cheaders, .signed_headers = snames};
}

/// Create canonical request:
/// <HTTPMethod>\n
/// <CanonicalURI>\n
/// <CanonicalQueryString>\n
/// <CanonicalHeaders>\n
/// <SignedHeaders>\n
/// <HashedPayload>
inline result<ss::sstring> create_canonical_request(
  const canonical_headers& hdr,
  const http::client::request_header& header,
  std::string_view hashed_payload) {
    auto method = std::string(header.method_string());
    auto target = std::string(header.target());
    if (target.empty() || target.at(0) != '/') {
        target = "/" + target;
    }
    auto canonical_uri = get_canonical_uri(target);
    if (!canonical_uri) {
        return canonical_uri.error();
    }
    auto canonical_query = get_canonical_query_string(target);
    if (!canonical_query) {
        return canonical_query.error();
    }
    return ssx::sformat(
      "{}\n{}\n{}\n{}\n{}\n{}",
      method,
      canonical_uri.value(),
      canonical_query.value(),
      hdr.canonical_headers,
      hdr.signed_headers,
      hashed_payload);
}

/// Genertes string-to-sign (in spec terms), example:
///
///   "AWS4-HMAC-SHA256" + "\n" +
///   timeStampISO8601Format + "\n" +
///   <Scope> + "\n" +
///   Hex(SHA256Hash(<CanonicalRequest>))
///
/// \param timestamp is an ISO8601 format timestamp
/// \param is a scope string (date/region/service)
/// \param canonical_req is a canonical request
inline ss::sstring get_string_to_sign(
  const ss::sstring& timestamp,
  const ss::sstring& scope,
  const ss::sstring& canonical_req) {
    auto digest = sha_256(canonical_req);
    return ssx::sformat("{}\n{}\n{}\n{}", algorithm, timestamp, scope, digest);
}

ss::sstring signature_v4::sha256_hexdigest(std::string_view payload) {
    return sha_256(payload);
}

std::error_code signature_v4::sign_header(
  http::client::request_header& header, std::string_view sha256) const {
    ss::sstring date_str = _sig_time.format_date();
    ss::sstring service = "s3";
    auto sign_key = gen_sig_key(_private_key(), date_str, _region(), service);
    auto cred_scope = ssx::sformat(
      "{}/{}/{}/aws4_request", date_str, _region(), service);
    vlog(clrl_log.trace, "Credentials updated:\n[scope]\n{}\n", cred_scope);
    auto amz_date = _sig_time.format_datetime();
    header.set("x-amz-date", {amz_date.data(), amz_date.size()});
    header.set("x-amz-content-sha256", {sha256.data(), sha256.size()});
    auto canonical_headers = get_canonical_headers(header);
    if (!canonical_headers) {
        return canonical_headers.error();
    }
    auto canonical_req = create_canonical_request(
      canonical_headers.value(), header, sha256);
    if (!canonical_req) {
        return canonical_req.error();
    }
    vlog(clrl_log.trace, "\n[canonical-request]\n{}\n", canonical_req.value());
    auto str_to_sign = get_string_to_sign(
      amz_date, cred_scope, canonical_req.value());
    auto digest = hmac(sign_key, str_to_sign);
    auto auth_header = fmt::format(
      "{} Credential={}/{},SignedHeaders={},Signature={}",
      algorithm,
      _access_key(),
      cred_scope,
      canonical_headers.value().signed_headers,
      hexdigest(digest));
    header.set(boost::beast::http::field::authorization, auth_header);
    vlog(
      clrl_log.trace, "\n[signed-header]\n\n{}", http::redacted_header(header));
    return {};
}

signature_v4::signature_v4(
  aws_region_name region,
  public_key_str access_key,
  private_key_str private_key,
  time_source&& ts)
  : _sig_time(std::move(ts))
  , _region(std::move(region))
  , _access_key(std::move(access_key))
  , _private_key(std::move(private_key)) {}
} // namespace cloud_roles
