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

#include "bytes/iobuf.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <system_error>
#include <variant>

namespace cloud_storage_clients {

/// \brief AWS S3 error codes
///
/// \note GCS uses these codes but adds it's own so implementation
///       needs to be ready for that
enum class s3_error_code {
    access_denied,
    account_problem,
    all_access_disabled,
    ambiguous_grant_by_email_address,
    authorization_header_malformed,
    bad_digest,
    bucket_already_exists,
    bucket_already_owned_by_you,
    bucket_not_empty,
    credentials_not_supported,
    cross_location_logging_prohibited,
    entity_too_small,
    entity_too_large,
    expired_token,
    illegal_location_constraint_exception,
    illegal_versioning_configuration_exception,
    incomplete_body,
    incorrect_number_of_files_in_post_request,
    inline_data_too_large,
    internal_error,
    invalid_access_key_id,
    invalid_access_point,
    invalid_addressing_header,
    invalid_argument,
    invalid_bucket_name,
    invalid_bucket_state,
    invalid_digest,
    invalid_encryption_algorithm_error,
    invalid_location_constraint,
    invalid_object_state,
    invalid_part,
    invalid_part_order,
    invalid_payer,
    invalid_policy_document,
    invalid_range,
    invalid_request,
    invalid_security,
    invalid_soaprequest,
    invalid_storage_class,
    invalid_target_bucket_for_logging,
    invalid_token,
    invalid_uri,
    key_too_long_error,
    malformed_aclerror,
    malformed_postrequest,
    malformed_xml,
    max_message_length_exceeded,
    max_post_pre_data_length_exceeded_error,
    metadata_too_large,
    method_not_allowed,
    missing_attachment,
    missing_content_length,
    missing_request_body_error,
    missing_security_element,
    missing_security_header,
    no_logging_status_for_key,
    no_such_bucket,
    no_such_bucket_policy,
    no_such_key,
    no_such_lifecycle_configuration,
    no_such_tag_set,
    no_such_upload,
    no_such_version,
    not_implemented,
    not_signed_up,
    operation_aborted,
    permanent_redirect,
    precondition_failed,
    redirect,
    request_header_section_too_large,
    request_is_not_multi_part_content,
    request_timeout,
    request_time_too_skewed,
    request_torrent_of_bucket_error,
    restore_already_in_progress,
    server_side_encryption_configuration_not_found_error,
    service_unavailable,
    signature_does_not_match,
    slow_down,
    temporary_redirect,
    token_refresh_required,
    too_many_access_points,
    too_many_buckets,
    unexpected_content,
    unresolvable_grant_by_email_address,
    user_key_must_be_specified,
    no_such_access_point,
    invalid_tag,
    malformed_policy,
    _unknown
};

/// Operators to use with lexical_cast
std::ostream& operator<<(std::ostream& o, s3_error_code code);
std::istream& operator>>(std::istream& i, s3_error_code& code);

/// Error received in a response from the server
class rest_error_response : std::exception {
public:
    rest_error_response(
      std::string_view code,
      std::string_view message,
      std::string_view request_id,
      std::string_view resource);

    const char* what() const noexcept override;

    s3_error_code code() const noexcept;
    std::string_view code_string() const noexcept;
    std::string_view message() const noexcept;
    std::string_view request_id() const noexcept;
    std::string_view resource() const noexcept;

private:
    s3_error_code _code;
    /// Error code string representation, this string is almost always short
    /// enough for SSA
    ss::sstring _code_str;
    ss::sstring _message;
    ss::sstring _request_id;
    ss::sstring _resource;
};

} // namespace cloud_storage_clients
