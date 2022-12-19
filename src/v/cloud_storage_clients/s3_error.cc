/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_error.h"

#include <boost/lexical_cast.hpp>

#include <map>

namespace cloud_storage_clients {

std::ostream& operator<<(std::ostream& o, s3_error_code code) {
    switch (code) {
    case s3_error_code::access_denied:
        o << "AccessDenied";
        break;
    case s3_error_code::account_problem:
        o << "AccountProblem";
        break;
    case s3_error_code::all_access_disabled:
        o << "AllAccessDisabled";
        break;
    case s3_error_code::ambiguous_grant_by_email_address:
        o << "AmbiguousGrantByEmailAddress";
        break;
    case s3_error_code::authorization_header_malformed:
        o << "AuthorizationHeaderMalformed";
        break;
    case s3_error_code::bad_digest:
        o << "BadDigest";
        break;
    case s3_error_code::bucket_already_exists:
        o << "BucketAlreadyExists";
        break;
    case s3_error_code::bucket_already_owned_by_you:
        o << "BucketAlreadyOwnedByYou";
        break;
    case s3_error_code::bucket_not_empty:
        o << "BucketNotEmpty";
        break;
    case s3_error_code::credentials_not_supported:
        o << "CredentialsNotSupported";
        break;
    case s3_error_code::cross_location_logging_prohibited:
        o << "CrossLocationLoggingProhibited";
        break;
    case s3_error_code::entity_too_small:
        o << "EntityTooSmall";
        break;
    case s3_error_code::entity_too_large:
        o << "EntityTooLarge";
        break;
    case s3_error_code::expired_token:
        o << "ExpiredToken";
        break;
    case s3_error_code::illegal_location_constraint_exception:
        o << "IllegalLocationConstraintException";
        break;
    case s3_error_code::illegal_versioning_configuration_exception:
        o << "IllegalVersioningConfigurationException";
        break;
    case s3_error_code::incomplete_body:
        o << "IncompleteBody";
        break;
    case s3_error_code::incorrect_number_of_files_in_post_request:
        o << "IncorrectNumberOfFilesInPostRequest";
        break;
    case s3_error_code::inline_data_too_large:
        o << "InlineDataTooLarge";
        break;
    case s3_error_code::internal_error:
        o << "InternalError";
        break;
    case s3_error_code::invalid_access_key_id:
        o << "InvalidAccessKeyId";
        break;
    case s3_error_code::invalid_access_point:
        o << "InvalidAccessPoint";
        break;
    case s3_error_code::invalid_addressing_header:
        o << "InvalidAddressingHeader";
        break;
    case s3_error_code::invalid_argument:
        o << "InvalidArgument";
        break;
    case s3_error_code::invalid_bucket_name:
        o << "InvalidBucketName";
        break;
    case s3_error_code::invalid_bucket_state:
        o << "InvalidBucketState";
        break;
    case s3_error_code::invalid_digest:
        o << "InvalidDigest";
        break;
    case s3_error_code::invalid_encryption_algorithm_error:
        o << "InvalidEncryptionAlgorithmError";
        break;
    case s3_error_code::invalid_location_constraint:
        o << "InvalidLocationConstraint";
        break;
    case s3_error_code::invalid_object_state:
        o << "InvalidObjectState";
        break;
    case s3_error_code::invalid_part:
        o << "InvalidPart";
        break;
    case s3_error_code::invalid_part_order:
        o << "InvalidPartOrder";
        break;
    case s3_error_code::invalid_payer:
        o << "InvalidPayer";
        break;
    case s3_error_code::invalid_policy_document:
        o << "InvalidPolicyDocument";
        break;
    case s3_error_code::invalid_range:
        o << "InvalidRange";
        break;
    case s3_error_code::invalid_request:
        o << "InvalidRequest";
        break;
    case s3_error_code::invalid_security:
        o << "InvalidSecurity";
        break;
    case s3_error_code::invalid_soaprequest:
        o << "InvalidSOAPRequest";
        break;
    case s3_error_code::invalid_storage_class:
        o << "InvalidStorageClass";
        break;
    case s3_error_code::invalid_target_bucket_for_logging:
        o << "InvalidTargetBucketForLogging";
        break;
    case s3_error_code::invalid_token:
        o << "InvalidToken";
        break;
    case s3_error_code::invalid_uri:
        o << "InvalidURI";
        break;
    case s3_error_code::key_too_long_error:
        o << "KeyTooLongError";
        break;
    case s3_error_code::malformed_aclerror:
        o << "MalformedACLError";
        break;
    case s3_error_code::malformed_postrequest:
        o << "MalformedPOSTRequest";
        break;
    case s3_error_code::malformed_xml:
        o << "MalformedXML";
        break;
    case s3_error_code::max_message_length_exceeded:
        o << "MaxMessageLengthExceeded";
        break;
    case s3_error_code::max_post_pre_data_length_exceeded_error:
        o << "MaxPostPreDataLengthExceededError";
        break;
    case s3_error_code::metadata_too_large:
        o << "MetadataTooLarge";
        break;
    case s3_error_code::method_not_allowed:
        o << "MethodNotAllowed";
        break;
    case s3_error_code::missing_attachment:
        o << "MissingAttachment";
        break;
    case s3_error_code::missing_content_length:
        o << "MissingContentLength";
        break;
    case s3_error_code::missing_request_body_error:
        o << "MissingRequestBodyError";
        break;
    case s3_error_code::missing_security_element:
        o << "MissingSecurityElement";
        break;
    case s3_error_code::missing_security_header:
        o << "MissingSecurityHeader";
        break;
    case s3_error_code::no_logging_status_for_key:
        o << "NoLoggingStatusForKey";
        break;
    case s3_error_code::no_such_bucket:
        o << "NoSuchBucket";
        break;
    case s3_error_code::no_such_bucket_policy:
        o << "NoSuchBucketPolicy";
        break;
    case s3_error_code::no_such_key:
        o << "NoSuchKey";
        break;
    case s3_error_code::no_such_lifecycle_configuration:
        o << "NoSuchLifecycleConfiguration";
        break;
    case s3_error_code::no_such_tag_set:
        o << "NoSuchTagSet";
        break;
    case s3_error_code::no_such_upload:
        o << "NoSuchUpload";
        break;
    case s3_error_code::no_such_version:
        o << "NoSuchVersion";
        break;
    case s3_error_code::not_implemented:
        o << "NotImplemented";
        break;
    case s3_error_code::not_signed_up:
        o << "NotSignedUp";
        break;
    case s3_error_code::operation_aborted:
        o << "OperationAborted";
        break;
    case s3_error_code::permanent_redirect:
        o << "PermanentRedirect";
        break;
    case s3_error_code::precondition_failed:
        o << "PreconditionFailed";
        break;
    case s3_error_code::redirect:
        o << "Redirect";
        break;
    case s3_error_code::request_header_section_too_large:
        o << "RequestHeaderSectionTooLarge";
        break;
    case s3_error_code::request_is_not_multi_part_content:
        o << "RequestIsNotMultiPartContent";
        break;
    case s3_error_code::request_timeout:
        o << "RequestTimeout";
        break;
    case s3_error_code::request_time_too_skewed:
        o << "RequestTimeTooSkewed";
        break;
    case s3_error_code::request_torrent_of_bucket_error:
        o << "RequestTorrentOfBucketError";
        break;
    case s3_error_code::restore_already_in_progress:
        o << "RestoreAlreadyInProgress";
        break;
    case s3_error_code::server_side_encryption_configuration_not_found_error:
        o << "ServerSideEncryptionConfigurationNotFoundError";
        break;
    case s3_error_code::service_unavailable:
        o << "ServiceUnavailable";
        break;
    case s3_error_code::signature_does_not_match:
        o << "SignatureDoesNotMatch";
        break;
    case s3_error_code::slow_down:
        o << "SlowDown";
        break;
    case s3_error_code::temporary_redirect:
        o << "TemporaryRedirect";
        break;
    case s3_error_code::token_refresh_required:
        o << "TokenRefreshRequired";
        break;
    case s3_error_code::too_many_access_points:
        o << "TooManyAccessPoints";
        break;
    case s3_error_code::too_many_buckets:
        o << "TooManyBuckets";
        break;
    case s3_error_code::unexpected_content:
        o << "UnexpectedContent";
        break;
    case s3_error_code::unresolvable_grant_by_email_address:
        o << "UnresolvableGrantByEmailAddress";
        break;
    case s3_error_code::user_key_must_be_specified:
        o << "UserKeyMustBeSpecified";
        break;
    case s3_error_code::no_such_access_point:
        o << "NoSuchAccessPoint";
        break;
    case s3_error_code::invalid_tag:
        o << "InvalidTag";
        break;
    case s3_error_code::malformed_policy:
        o << "MalformedPolicy";
        break;
    case s3_error_code::_unknown:
        o << "_unknown_error_code_";
        break;
    }
    return o;
}

// NOLINTNEXTLINE
static const std::map<ss::sstring, s3_error_code> known_aws_error_codes = {
  {"AccessDenied", s3_error_code::access_denied},
  {"AccountProblem", s3_error_code::account_problem},
  {"AllAccessDisabled", s3_error_code::all_access_disabled},
  {"AmbiguousGrantByEmailAddress",
   s3_error_code::ambiguous_grant_by_email_address},
  {"AuthorizationHeaderMalformed",
   s3_error_code::authorization_header_malformed},
  {"BadDigest", s3_error_code::bad_digest},
  {"BucketAlreadyExists", s3_error_code::bucket_already_exists},
  {"BucketAlreadyOwnedByYou", s3_error_code::bucket_already_owned_by_you},
  {"BucketNotEmpty", s3_error_code::bucket_not_empty},
  {"CredentialsNotSupported", s3_error_code::credentials_not_supported},
  {"CrossLocationLoggingProhibited",
   s3_error_code::cross_location_logging_prohibited},
  {"EntityTooSmall", s3_error_code::entity_too_small},
  {"EntityTooLarge", s3_error_code::entity_too_large},
  {"ExpiredToken", s3_error_code::expired_token},
  {"IllegalLocationConstraintException",
   s3_error_code::illegal_location_constraint_exception},
  {"IllegalVersioningConfigurationException",
   s3_error_code::illegal_versioning_configuration_exception},
  {"IncompleteBody", s3_error_code::incomplete_body},
  {"IncorrectNumberOfFilesInPostRequest",
   s3_error_code::incorrect_number_of_files_in_post_request},
  {"InlineDataTooLarge", s3_error_code::inline_data_too_large},
  {"InternalError", s3_error_code::internal_error},
  {"InvalidAccessKeyId", s3_error_code::invalid_access_key_id},
  {"InvalidAccessPoint", s3_error_code::invalid_access_point},
  {"InvalidAddressingHeader", s3_error_code::invalid_addressing_header},
  {"InvalidArgument", s3_error_code::invalid_argument},
  {"InvalidBucketName", s3_error_code::invalid_bucket_name},
  {"InvalidBucketState", s3_error_code::invalid_bucket_state},
  {"InvalidDigest", s3_error_code::invalid_digest},
  {"InvalidEncryptionAlgorithmError",
   s3_error_code::invalid_encryption_algorithm_error},
  {"InvalidLocationConstraint", s3_error_code::invalid_location_constraint},
  {"InvalidObjectState", s3_error_code::invalid_object_state},
  {"InvalidPart", s3_error_code::invalid_part},
  {"InvalidPartOrder", s3_error_code::invalid_part_order},
  {"InvalidPayer", s3_error_code::invalid_payer},
  {"InvalidPolicyDocument", s3_error_code::invalid_policy_document},
  {"InvalidRange", s3_error_code::invalid_range},
  {"InvalidRequest", s3_error_code::invalid_request},
  {"InvalidSecurity", s3_error_code::invalid_security},
  {"InvalidSOAPRequest", s3_error_code::invalid_soaprequest},
  {"InvalidStorageClass", s3_error_code::invalid_storage_class},
  {"InvalidTargetBucketForLogging",
   s3_error_code::invalid_target_bucket_for_logging},
  {"InvalidToken", s3_error_code::invalid_token},
  {"InvalidURI", s3_error_code::invalid_uri},
  {"KeyTooLongError", s3_error_code::key_too_long_error},
  {"MalformedACLError", s3_error_code::malformed_aclerror},
  {"MalformedPOSTRequest", s3_error_code::malformed_postrequest},
  {"MalformedXML", s3_error_code::malformed_xml},
  {"MaxMessageLengthExceeded", s3_error_code::max_message_length_exceeded},
  {"MaxPostPreDataLengthExceededError",
   s3_error_code::max_post_pre_data_length_exceeded_error},
  {"MetadataTooLarge", s3_error_code::metadata_too_large},
  {"MethodNotAllowed", s3_error_code::method_not_allowed},
  {"MissingAttachment", s3_error_code::missing_attachment},
  {"MissingContentLength", s3_error_code::missing_content_length},
  {"MissingRequestBodyError", s3_error_code::missing_request_body_error},
  {"MissingSecurityElement", s3_error_code::missing_security_element},
  {"MissingSecurityHeader", s3_error_code::missing_security_header},
  {"NoLoggingStatusForKey", s3_error_code::no_logging_status_for_key},
  {"NoSuchBucket", s3_error_code::no_such_bucket},
  {"NoSuchBucketPolicy", s3_error_code::no_such_bucket_policy},
  {"NoSuchKey", s3_error_code::no_such_key},
  {"NoSuchLifecycleConfiguration",
   s3_error_code::no_such_lifecycle_configuration},
  {"NoSuchTagSet", s3_error_code::no_such_tag_set},
  {"NoSuchUpload", s3_error_code::no_such_upload},
  {"NoSuchVersion", s3_error_code::no_such_version},
  {"NotImplemented", s3_error_code::not_implemented},
  {"NotSignedUp", s3_error_code::not_signed_up},
  {"OperationAborted", s3_error_code::operation_aborted},
  {"PermanentRedirect", s3_error_code::permanent_redirect},
  {"PreconditionFailed", s3_error_code::precondition_failed},
  {"Redirect", s3_error_code::redirect},
  {"RequestHeaderSectionTooLarge",
   s3_error_code::request_header_section_too_large},
  {"RequestIsNotMultiPartContent",
   s3_error_code::request_is_not_multi_part_content},
  {"RequestTimeout", s3_error_code::request_timeout},
  {"RequestTimeTooSkewed", s3_error_code::request_time_too_skewed},
  {"RequestTorrentOfBucketError",
   s3_error_code::request_torrent_of_bucket_error},
  {"RestoreAlreadyInProgress", s3_error_code::restore_already_in_progress},
  {"ServerSideEncryptionConfigurationNotFoundError",
   s3_error_code::server_side_encryption_configuration_not_found_error},
  {"ServiceUnavailable", s3_error_code::service_unavailable},
  {"SignatureDoesNotMatch", s3_error_code::signature_does_not_match},
  {"SlowDown", s3_error_code::slow_down},
  {"TemporaryRedirect", s3_error_code::temporary_redirect},
  {"TokenRefreshRequired", s3_error_code::token_refresh_required},
  {"TooManyAccessPoints", s3_error_code::too_many_access_points},
  {"TooManyBuckets", s3_error_code::too_many_buckets},
  {"UnexpectedContent", s3_error_code::unexpected_content},
  {"UnresolvableGrantByEmailAddress",
   s3_error_code::unresolvable_grant_by_email_address},
  {"UserKeyMustBeSpecified", s3_error_code::user_key_must_be_specified},
  {"NoSuchAccessPoint", s3_error_code::no_such_access_point},
  {"InvalidTag", s3_error_code::invalid_tag},
  {"MalformedPolicy", s3_error_code::malformed_policy}};

std::istream& operator>>(std::istream& i, s3_error_code& code) {
    ss::sstring c;
    i >> c;
    auto it = known_aws_error_codes.find(c);
    if (it != known_aws_error_codes.end()) {
        code = it->second;
    } else {
        code = s3_error_code::_unknown;
    }
    return i;
}

rest_error_response::rest_error_response(
  std::string_view code,
  std::string_view message,
  std::string_view request_id,
  std::string_view resource)
  : _code(boost::lexical_cast<s3_error_code>(code))
  , _code_str(code)
  , _message(message)
  , _request_id(request_id)
  , _resource(resource) {}

const char* rest_error_response::what() const noexcept {
    return _message.c_str();
}
s3_error_code rest_error_response::code() const noexcept { return _code; }
std::string_view rest_error_response::code_string() const noexcept {
    return _code_str;
}
std::string_view rest_error_response::message() const noexcept {
    return _message;
}
std::string_view rest_error_response::request_id() const noexcept {
    return _request_id;
}
std::string_view rest_error_response::resource() const noexcept {
    return _resource;
}

} // namespace cloud_storage_clients
