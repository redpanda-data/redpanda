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
#include "utils/base64.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string/compare.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <fmt/compile.h>

#include <system_error>

namespace {
constexpr auto iso_8061_date_fmt = FMT_COMPILE("{:%Y%m%d}");
constexpr auto iso_8061_datetime_fmt = FMT_COMPILE("{:%Y%m%dT%H%M%SZ}");
constexpr auto rfc_9110_datetime_fmt = FMT_COMPILE(
  "{:%a, %d %b %Y %H:%M:%S %Z}");
} // namespace

namespace cloud_roles {

constexpr int sha256_digest_length = 32;
using hmac_digest = std::array<char, sha256_digest_length>;

constexpr std::string_view algorithm = "AWS4-HMAC-SHA256";

struct signing_error_category final : std::error_category {
    const char* name() const noexcept final { return "s3"; }
    std::string message(int ec) const final {
        switch (static_cast<signing_error_code>(ec)) {
        case signing_error_code::invalid_uri:
            return "Target URI shouldn't be empty or include domain name";
        case signing_error_code::invalid_uri_params:
            return "Target URI contains invalid query parameters";
        case signing_error_code::not_enough_arguments:
            return "Can't make request, not enough arguments";
        }
        return "unknown";
    }
};

std::error_code make_error_code(signing_error_code ec) noexcept {
    static signing_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

// time_source //

time_source::time_source()
  : time_source(&default_source, 0) {}

time_source::time_source(timestamp instant)
  : time_source([instant]() { return instant; }, 0) {}

ss::sstring time_source::format_date() const {
    return format(iso_8061_date_fmt);
}

ss::sstring time_source::format_datetime() const {
    return format(iso_8061_datetime_fmt);
}

ss::sstring time_source::format_http_datetime() const {
    return format(rfc_9110_datetime_fmt);
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

ss::sstring time_source::format(auto fmt) const {
    const auto point = _gettime_fn();
    const std::time_t time = std::chrono::system_clock::to_time_t(point);
    const std::tm gm = fmt::gmtime(time);

    return fmt::format(fmt, gm);
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

struct target_parts {
    /// \brief URI Encoded canonical URI
    /// Canonical URI is everything that follows domain name starting with '/'
    /// without parameters (everythng after '?' including '?'). The uri is URI
    /// encoded. e.g. https://foo.bar/canonical-url?param=value
    ss::sstring canonical_uri;
    /// \brief Query parameters extracted from target
    /// They are sorted by key. Note that both the key and the value are *not*
    /// URI encoded.
    std::vector<std::pair<ss::sstring, ss::sstring>> query_params;
};

/// \brief Split the target string into the canonical URI and a sorted list
/// of query params.
static result<target_parts> split_target(ss::sstring target) {
    if (target.empty() || target[0] != '/') {
        vlog(clrl_log.error, "invalid URI {}", target);
        return make_error_code(signing_error_code::invalid_uri);
    }

    ss::sstring canonical_uri{};
    auto pos = target.find('?');
    if (pos == ss::sstring::npos) {
        return target_parts{
          .canonical_uri = uri_encode(target, false), .query_params = {}};
    } else {
        auto canonical_uri = uri_encode(target.substr(0, pos), false);
        if (pos == target.size() - 1) {
            return target_parts{
              .canonical_uri = canonical_uri, .query_params = {}};
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
                      signing_error_code::invalid_uri_params);
                }
                ss::sstring pname = param.substr(0, p);
                ss::sstring pvalue = param.substr(p + 1);
                query_params.emplace_back(pname, pvalue);
            }
        }

        std::sort(query_params.begin(), query_params.end());

        return target_parts{
          .canonical_uri = canonical_uri, .query_params = query_params};
    }
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
static ss::sstring get_canonical_query_string(
  const std::vector<std::pair<ss::sstring, ss::sstring>>& query_params) {
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

    auto split_result = split_target(target);
    if (!split_result) {
        return split_result.error();
    }

    auto& [canonical_uri, query_params] = split_result.value();
    auto canonical_query = get_canonical_query_string(query_params);

    return ssx::sformat(
      "{}\n{}\n{}\n{}\n{}\n{}",
      method,
      canonical_uri,
      canonical_query,
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
    vlog(clrl_log.trace, "\n[signed-header]\n\n{}", header);
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

static constexpr auto required_headers = {
  "Content-Encoding",
  "Content-Language",
  "Content-Length",
  "Content-MD5",
  "Content-Type",
  "Date",
  "If-Modified-Since",
  "If-Match",
  "If-None-Match",
  "If-UnmodifiedSince",
  "Range"};

result<ss::sstring> signature_abs::get_canonicalized_resource(
  const http::client::request_header& header) const {
    const auto target_view = header.target();
    const ss::sstring target{target_view.data(), target_view.size()};
    auto split_result = split_target(target);
    if (!split_result) {
        return split_result.error();
    }

    auto& [canonical_uri, query_params] = split_result.value();

    ss::sstring result = ssx::sformat(
      "/{}{}", _storage_account(), canonical_uri);

    for (const auto& [pname, pvalue] : query_params) {
        // TODO(vlad): List values are not supported.
        result += ssx::sformat("\n{}:{}", pname, pvalue);
    }

    return result;
}

ss::sstring signature_abs::get_canonicalized_headers(
  const http::client::request_header& header) const {
    std::vector<std::pair<ss::sstring, ss::sstring>> ms_headers;
    for (const auto& it : header) {
        auto name = it.name_string();
        auto value = it.value();

        if (name.starts_with("x-ms-")) {
            ss::sstring n{name.data(), name.size()};
            ss::sstring v{value.data(), value.size()};
            boost::trim(v);

            ms_headers.emplace_back(n, v);
        }
    }

    std::sort(ms_headers.begin(), ms_headers.end());

    ss::sstring result{};
    for (auto& ms_header : ms_headers) {
        result += ssx::sformat(
          "{}:{}\n", std::move(ms_header.first), std::move(ms_header.second));
    }

    return result;
}

result<ss::sstring>
signature_abs::get_string_to_sign(http::client::request_header& header) const {
    ss::sstring non_ms_headers{};
    for (const auto& header_name : required_headers) {
        auto header_value_view = header[header_name];
        ss::sstring header_value{
          header_value_view.data(), header_value_view.size()};

        boost::trim(header_value);

        // Represent 0 content-size as empty string as per
        // https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#content-length-header-in-version-2014-02-14-and-earlier
        if (
          std::string_view{header_name} == "Content-Length"
          && header_value == "0") {
            header_value = "";
        }

        non_ms_headers += ssx::sformat("{}\n", header_value);
    }

    const auto canonicalized_ms_headers = get_canonicalized_headers(header);
    auto canonicalized_res = get_canonicalized_resource(header);
    if (!canonicalized_res) {
        return canonicalized_res;
    }

    return ssx::sformat(
      "{}\n{}{}{}",
      header.method_string(),
      non_ms_headers,
      canonicalized_ms_headers,
      canonicalized_res.value());
}

signature_abs::signature_abs(
  storage_account storage_account, private_key_str shared_key, time_source ts)
  : _sig_time(std::move(ts))
  , _storage_account(std::move(storage_account))
  , _shared_key(std::move(shared_key)) {}

std::error_code
signature_abs::sign_header(http::client::request_header& header) const {
    auto ms_date = _sig_time.format_http_datetime();
    header.set("x-ms-date", {ms_date.data(), ms_date.size()});
    header.set("x-ms-version", azure_storage_api_version);

    const auto to_sign = get_string_to_sign(header);
    if (!to_sign) {
        return to_sign.error();
    }

    auto digest = hmac(base64_to_string(_shared_key()), to_sign.value());

    auto auth_header = fmt::format(
      "SharedKey {}:{}",
      _storage_account(),
      bytes_to_base64(to_bytes_view(digest)));
    header.set(boost::beast::http::field::authorization, auth_header);

    return {};
}

} // namespace cloud_roles
