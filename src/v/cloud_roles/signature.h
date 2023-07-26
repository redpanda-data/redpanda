/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/types.h"
#include "http/client.h"
#include "outcome.h"
#include "seastarx.h"
#include "units.h"
#include "utils/named_type.h"
#include "vassert.h"

#include <fmt/chrono.h>

#include <chrono>
#include <string_view>

namespace cloud_roles {

/// \brief Internal s3 client error code
enum class signing_error_code : int {
    invalid_uri,
    invalid_uri_params,
    not_enough_arguments,
};

std::error_code make_error_code(signing_error_code ec) noexcept;

/// Time source for signature_v4 and signature_abs. Supports
/// two formats: ISO8601 and RFC9110.
/// Can be used to get and format current time or to
/// format the pre-defined time for testing.
class time_source {
public:
    /// \brief Initialize time-source
    /// Defult time-source uses std::chrono::system_clock.
    time_source();

    /// \brief Initialize time-source using the hardcoded
    /// value for testing.
    ///
    /// \param instant is a timestamp that time_source should use
    explicit time_source(timestamp instant);

    time_source(const time_source&) = delete;
    time_source& operator=(const time_source&) = delete;
    time_source(time_source&&) noexcept = default;
    time_source& operator=(time_source&&) noexcept = default;
    ~time_source() noexcept = default;

    /// Return formatted date in ISO8601 format
    /// Example: 20201231
    ss::sstring format_date() const;

    /// Return formatted date in ISO8601 format
    /// Example: 20201231T123100Z
    ss::sstring format_datetime() const;

    /// Return formatted datetime according to RFC9110
    /// Example: Tue, 15 Nov 2010 08:12:31 GMT
    ss::sstring format_http_datetime() const;

private:
    template<class Fn>
    explicit time_source(Fn&& fn, int);

    /// Format date-time according to format string
    ss::sstring format(auto fmt) const;

    static timestamp default_source();
    ss::noncopyable_function<timestamp()> _gettime_fn;
};

/// AWS Signature V4 generator
class signature_v4 {
public:
    /// \brief Initialize signature generator
    ///
    /// \param region is an AWS region that we're going to send request to
    /// \param access_key is an AWS access key
    /// \param private_key is an AWS private key
    /// \param time_source is a source of timestamps for the signature
    signature_v4(
      aws_region_name region,
      public_key_str access_key,
      private_key_str private_key,
      time_source&& c = time_source());

    /// \brief Sign http header
    /// Calculate the digest based on the header fields and add auth fields to
    /// header.
    ///
    /// \param header is an in/out parameter that contains request headers
    /// \param sha256 is a hash of the payload if payload is signed or defult
    /// value otherwise
    std::error_code sign_header(
      http::client::request_header& header, std::string_view sha256) const;

    static ss::sstring gen_sig_key(
      std::string_view key,
      std::string_view datestr,
      std::string_view region,
      std::string_view service);

private:
    /// \brief Calculate SHA256 digest
    ///
    /// \param payload is ref to payload of the query
    /// \return sha256 digest in hex format (compatible with AWS signature
    /// requirements)
    static ss::sstring sha256_hexdigest(std::string_view payload);

    /// Time of the signing key
    time_source _sig_time;
    /// AWS region
    aws_region_name _region;
    /// Access key
    public_key_str _access_key;
    /// Secret key
    private_key_str _private_key;
};

class signature_abs {
public:
    /// \brief Initialize the ABS signature generator
    ///
    /// \param storage_account is the storage account to which the
    /// request will be sent
    /// \param shared_key is a user provided Shared Key
    /// \param time_source is a source of timestamps for the signature
    signature_abs(
      storage_account storage_account,
      private_key_str shared_key,
      time_source c = time_source());

    /// \brief Sign http header
    /// Calculate the digest based on the header fields and add auth fields to
    /// header.
    ///
    /// \param header is an in/out parameter that contains request headers
    std::error_code sign_header(http::client::request_header& header) const;

private:
    // Azure expects every request to Blob Storage to contain an
    // 'x-ms-version' header that specifies the API version to use.
    // This version is hardcoded in Redpanda to ensure that an API
    // version that we've tested with is used in field.
    //
    // Update this version to use a different storage API version.
    static constexpr auto azure_storage_api_version = "2023-01-03";

    result<ss::sstring>
    get_string_to_sign(http::client::request_header& header) const;

    result<ss::sstring> get_canonicalized_resource(
      const http::client::request_header& header) const;

    ss::sstring
    get_canonicalized_headers(const http::client::request_header& header) const;

    /// Time of the signing key
    time_source _sig_time;
    /// Name of the storage account in use
    storage_account _storage_account;
    /// Shared key
    private_key_str _shared_key;
};

template<class Fn>
time_source::time_source(Fn&& fn, int)
  : _gettime_fn(std::forward<Fn>(fn)) {}

ss::sstring uri_encode(const ss::sstring& input, bool encode_slash);

} // namespace cloud_roles
