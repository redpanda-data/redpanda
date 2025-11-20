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

#include "bytes/iostream.h"
#include "serde/json/writer.h"
#include "serde/protobuf/wire_format.h"

#include <seastar/http/reply.hh>
#include <seastar/json/formatter.hh>

namespace serde::pb::rpc {

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-cpp)
ss::logger logger{"connectrpc"};

namespace {
iobuf error_info_to_proto(const error_info& ei) {
    iobuf buf;
    // reason
    serde::pb::tag::write(
      {.wire_type = serde::pb::wire_type::length, .field_number = 1}, &buf);
    write_length(static_cast<int32_t>(ei.reason.size()), &buf);
    buf.append_str(ei.reason);
    // domain
    serde::pb::tag::write(
      {.wire_type = serde::pb::wire_type::length, .field_number = 2}, &buf);
    write_length(static_cast<int32_t>(ei.domain.size()), &buf);
    buf.append_str(ei.domain);
    // metadata
    for (const auto& [key, value] : ei.metadata) {
        iobuf entry;
        {
            // key
            serde::pb::tag::write(
              {.wire_type = serde::pb::wire_type::length, .field_number = 1},
              &entry);
            write_length(static_cast<int32_t>(key.size()), &entry);
            entry.append_str(key);
            // value
            serde::pb::tag::write(
              {.wire_type = serde::pb::wire_type::length, .field_number = 2},
              &entry);
            write_length(static_cast<int32_t>(value.size()), &entry);
            entry.append_str(value);
        }
        serde::pb::tag::write(
          {.wire_type = serde::pb::wire_type::length, .field_number = 3}, &buf);
        write_length(static_cast<int32_t>(entry.size_bytes()), &buf);
        buf.append(std::move(entry));
    }
    return buf;
}

void error_info_to_json(const error_info& ei, serde::json::writer* w) {
    w->begin_object();
    w->key("reason");
    w->string(ei.reason);
    w->key("domain");
    w->string(ei.domain);
    if (!ei.metadata.empty()) {
        w->key("metadata");
        w->begin_object();
        for (const auto& [k, v] : ei.metadata) {
            w->key(k);
            w->string(v);
        }
        w->end_object();
    }
    w->end_object();
}

} // namespace

base_exception::base_exception(
  int status_code,
  ss::sstring code,
  ss::sstring message,
  std::optional<error_info> error_info)
  : _status_code(status_code)
  , _code(std::move(code))
  , _message(std::move(message))
  , _error_info(std::move(error_info)) {}

const char* base_exception::what() const noexcept { return _message.c_str(); }
const ss::sstring& base_exception::message() const { return _message; }
const std::optional<error_info>& base_exception::info() const {
    return _error_info;
}

std::unique_ptr<ss::http::reply>
base_exception::handle(std::unique_ptr<ss::http::reply> reply) const {
    serde::json::writer w;
    w.begin_object();
    w.key("code");
    w.string(_code);
    w.key("message");
    w.string(_message);
    if (_error_info) {
        w.key("details");
        w.begin_array();
        {
            w.begin_object();
            w.key("type");
            w.string("google.rpc.ErrorInfo");
            w.key("value");
            w.base64_string(error_info_to_proto(_error_info.value()));
            w.key("debug");
            error_info_to_json(_error_info.value(), &w);
            w.end_object();
        }
        w.end_array();
    }
    w.end_object();
    reply->set_status(static_cast<ss::http::reply::status_type>(_status_code));
    reply->write_body(
      "json", [b = std::move(w).finish()](ss::output_stream<char>& w) mutable {
          return write_iobuf_to_output_stream(std::move(b), w);
      });
    return reply;
}

#define CONNECTRPC_EXCEPTION_IMPL(name, default_message, http_code)            \
    name##_exception::name##_exception()                                       \
      : name##_exception(default_message) {}                                   \
    name##_exception::name##_exception(ss::sstring message)                    \
      : name##_exception(std::move(message), std::nullopt) {}                  \
    name##_exception::name##_exception(std::optional<error_info> ei)           \
      : name##_exception(default_message, std::move(ei)) {}                    \
    name##_exception::name##_exception(                                        \
      ss::sstring message, std::optional<error_info> ei)                       \
      : base_exception(                                                        \
          static_cast<int>(ss::http::reply::status_type::http_code),           \
          #name,                                                               \
          std::move(message),                                                  \
          std::move(ei)) {}

// TODO(rpc): Use 499 when supported by seastar
CONNECTRPC_EXCEPTION_IMPL(cancelled, "Canceled", unprocessable_entity)
CONNECTRPC_EXCEPTION_IMPL(unknown, "Unknown error", internal_server_error)
CONNECTRPC_EXCEPTION_IMPL(invalid_argument, "Invalid argument", bad_request)
CONNECTRPC_EXCEPTION_IMPL(
  deadline_exceeded, "Deadline exceeded", gateway_timeout)
CONNECTRPC_EXCEPTION_IMPL(not_found, "Resource not found", not_found)
CONNECTRPC_EXCEPTION_IMPL(already_exists, "Resource already exists", conflict)
CONNECTRPC_EXCEPTION_IMPL(permission_denied, "Permission denied", forbidden)
CONNECTRPC_EXCEPTION_IMPL(
  resource_exhausted, "Resource exhausted", too_many_requests)
CONNECTRPC_EXCEPTION_IMPL(
  failed_precondition, "Failed precondition", bad_request)
CONNECTRPC_EXCEPTION_IMPL(aborted, "Operation aborted", conflict)
CONNECTRPC_EXCEPTION_IMPL(out_of_range, "Out of range", bad_request)
CONNECTRPC_EXCEPTION_IMPL(unimplemented, "Not implemented", not_implemented)
CONNECTRPC_EXCEPTION_IMPL(internal, "Internal error", internal_server_error)
CONNECTRPC_EXCEPTION_IMPL(
  unavailable, "Service unavailable", service_unavailable)
CONNECTRPC_EXCEPTION_IMPL(data_loss, "Data loss", internal_server_error)
CONNECTRPC_EXCEPTION_IMPL(unauthenticated, "Unauthenticated", unauthorized)

#undef CONNECTRPC_DEFINE_EXCEPTION

} // namespace serde::pb::rpc
