/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/protobuf/rpc.h"

#include <seastar/http/reply.hh>
#include <seastar/json/formatter.hh>

#include <unordered_map>

namespace serde::pb::rpc {

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-cpp)
ss::logger logger{"connectrpc"};

base_exception::base_exception(
  int status_code, ss::sstring code, ss::sstring message)
  : _status_code(status_code)
  , _code(std::move(code))
  , _message(std::move(message)) {}

std::unique_ptr<ss::http::reply>
base_exception::handle(std::unique_ptr<ss::http::reply> reply) const {
    std::unordered_map<ss::sstring, ss::sstring> body = {
      {"code", _code},
      {"message", _message},
    };
    reply->set_status(static_cast<ss::http::reply::status_type>(_status_code));
    reply->write_body("json", ss::json::formatter::to_json(body));
    return reply;
}

cancelled_exception::cancelled_exception()
  : cancelled_exception("Canceled") {}
cancelled_exception::cancelled_exception(ss::sstring message)
  : base_exception(
      // TODO(rpc): Should be 499
      static_cast<int>(ss::http::reply::status_type::unprocessable_entity),
      "canceled",
      std::move(message)) {}

unknown_exception::unknown_exception()
  : unknown_exception("Unknown error") {}
unknown_exception::unknown_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::internal_server_error),
      "unknown",
      std::move(message)) {}

invalid_argument_exception::invalid_argument_exception()
  : invalid_argument_exception("Invalid argument") {}
invalid_argument_exception::invalid_argument_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::bad_request),
      "invalid_argument",
      std::move(message)) {}

deadline_exceeded_exception::deadline_exceeded_exception()
  : deadline_exceeded_exception("Deadline exceeded") {}
deadline_exceeded_exception::deadline_exceeded_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::gateway_timeout),
      "deadline_exceeded",
      std::move(message)) {}

not_found_exception::not_found_exception()
  : not_found_exception("Resource not found") {}
not_found_exception::not_found_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::not_found),
      "not_found",
      std::move(message)) {}

already_exists_exception::already_exists_exception()
  : already_exists_exception("Resource already exists") {}
already_exists_exception::already_exists_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::conflict),
      "already_exists",
      std::move(message)) {}

permission_denied_exception::permission_denied_exception()
  : permission_denied_exception("Permission denied") {}
permission_denied_exception::permission_denied_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::forbidden),
      "permission_denied",
      std::move(message)) {}

resource_exhausted_exception::resource_exhausted_exception()
  : resource_exhausted_exception("Resource exhausted") {}
resource_exhausted_exception::resource_exhausted_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::too_many_requests),
      "resource_exhausted",
      std::move(message)) {}

failed_precondition_exception::failed_precondition_exception()
  : failed_precondition_exception("Failed precondition") {}
failed_precondition_exception::failed_precondition_exception(
  ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::bad_request),
      "failed_precondition",
      std::move(message)) {}

aborted_exception::aborted_exception()
  : aborted_exception("Operation aborted") {}
aborted_exception::aborted_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::conflict),
      "aborted",
      std::move(message)) {}

out_of_range_exception::out_of_range_exception()
  : out_of_range_exception("Out of range") {}
out_of_range_exception::out_of_range_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::bad_request),
      "out_of_range",
      std::move(message)) {}

unimplemented_exception::unimplemented_exception()
  : unimplemented_exception("Not implemented") {}
unimplemented_exception::unimplemented_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::not_implemented),
      "unimplemented",
      std::move(message)) {}

internal_exception::internal_exception()
  : internal_exception("Internal error") {}
internal_exception::internal_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::internal_server_error),
      "internal",
      std::move(message)) {}

unavailable_exception::unavailable_exception()
  : unavailable_exception("Service unavailable") {}
unavailable_exception::unavailable_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::service_unavailable),
      "unavailable",
      std::move(message)) {}

data_loss_exception::data_loss_exception()
  : data_loss_exception("Data loss") {}
data_loss_exception::data_loss_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::internal_server_error),
      "data_loss",
      std::move(message)) {}

unauthenticated_exception::unauthenticated_exception()
  : unauthenticated_exception("Unauthenticated") {}
unauthenticated_exception::unauthenticated_exception(ss::sstring message)
  : base_exception(
      static_cast<int>(ss::http::reply::status_type::unauthorized),
      "unauthenticated",
      std::move(message)) {}

} // namespace serde::pb::rpc
