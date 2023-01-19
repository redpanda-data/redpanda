/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/abs_error.h"

#include "utils/string_switch.h"

#include <boost/lexical_cast.hpp>

#include <map>

namespace cloud_storage_clients {

// Documentation for ABS error codes can be found at:
// * https://learn.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
// * https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
std::istream& operator>>(std::istream& i, abs_error_code& code) {
    ss::sstring c;
    i >> c;

    code
      = string_switch<abs_error_code>(c)
          .match("BlobNotFound", abs_error_code::blob_not_found)
          .match("ServerBusy", abs_error_code::server_busy)
          .match("InternalError", abs_error_code::internal_error)
          .match("OperationTimedOut", abs_error_code::operation_timed_out)
          .match("SystemInUse", abs_error_code::system_in_use)
          .match("BlobNotFound", abs_error_code::blob_not_found)
          .match("AccountBeingCreated", abs_error_code::account_being_created)
          .match(
            "ResourceAlreadyExists", abs_error_code::resource_already_exists)
          .match("BlobAlreadyExists", abs_error_code::blob_already_exists)
          .match("InvalidBlobOrBlock", abs_error_code::invalid_blob)
          .match("PendingCopyOperation", abs_error_code::pending_copy_operation)
          .match("SnapshotPresent", abs_error_code::snapshot_present)
          .match("BlobBeingRehydrated", abs_error_code::blob_being_rehydrated)
          .match("ContainerDisabled", abs_error_code::container_being_disabled)
          .match(
            "ContainerBeingDeleted", abs_error_code::container_being_deleted)
          .match("ContainerNotFound", abs_error_code::container_not_found)
          .default_match(abs_error_code::_unknown);

    return i;
}

abs_rest_error_response::abs_rest_error_response(
  ss::sstring code, ss::sstring message, boost::beast::http::status http_code)
  : _code(boost::lexical_cast<abs_error_code>(code))
  , _code_str(std::move(code))
  , _message(std::move(message))
  , _http_code(http_code) {}

const char* abs_rest_error_response::what() const noexcept {
    return _message.c_str();
}

abs_error_code abs_rest_error_response::code() const noexcept { return _code; }

std::string_view abs_rest_error_response::code_string() const noexcept {
    return _code_str;
}

std::string_view abs_rest_error_response::message() const noexcept {
    return _message;
}

boost::beast::http::status abs_rest_error_response::http_code() const noexcept {
    return _http_code;
}

} // namespace cloud_storage_clients
