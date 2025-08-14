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

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace seastar::http {
struct request;
struct reply;
} // namespace seastar::http

// Supporting functions for ConnectRPC protocol support in seastar
//
// See: https://connectrpc.com/docs/protocol

namespace serde::pb::rpc {

// NOLINTNEXTLINE(*-non-const-global-variables)
extern ss::logger logger;

// The level at which the route has been declared to having access restricted
// to.
enum class authz_level : uint8_t {
    unauthenticated,
    user,
    superuser,
};

// A small descriptor for a route, as well as a method for handling a route
//
// All routes should be POST requests, but the request/reply parsing will be
// handled by the handler method.
struct route_descriptor {
    // Name of the method such as "redpanda.core.admin.AdminService.GetRoutes"
    ss::sstring name;
    // Path of the route such as "/redpanda.core.admin.AdminService/GetRoutes"
    ss::sstring path;
    // The authentication and authorization level required to access this
    // handler.
    authz_level authz_level;

    std::function<ss::future<std::unique_ptr<ss::http::reply>>(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>)>
      handler;
};

// A base class that all ConnectRPC services inherit from to provide a discovery
// mechanism for handlers and their routes.
class base_service {
public:
    base_service() = default;
    // We delete move and copy constructors because `all_routes` captures
    // `this`.
    base_service(const base_service&) = delete;
    base_service(base_service&&) = delete;
    base_service& operator=(const base_service&) = delete;
    base_service& operator=(base_service&&) = delete;
    virtual ~base_service() = default;

    // The name of the RPC service such as "redpanda.core.admin.AdminService".
    virtual std::string_view name() const = 0;
    // Returns a vector of all the routes that this service has registered.
    virtual std::vector<route_descriptor> all_routes() = 0;
};

// Base Exception when handling RPC requests.
//
// See: https://connectrpc.com/docs/protocol#error-codes
class base_exception : public std::exception {
protected:
    base_exception(int status_code, ss::sstring code, ss::sstring message);

public:
    // Handle the HTTP reply to report the error as suggested.
    std::unique_ptr<ss::http::reply>
      handle(std::unique_ptr<ss::http::reply>) const;

private:
    int _status_code;
    ss::sstring _code;
    ss::sstring _message;
};

// RPC canceled, usually by the caller.
class cancelled_exception : public base_exception {
public:
    cancelled_exception();
    explicit cancelled_exception(ss::sstring message);
};

// Catch-all for errors of unclear origin and errors without a more appropriate
// code.
class unknown_exception : public base_exception {
public:
    unknown_exception();
    explicit unknown_exception(ss::sstring message);
};

// Request is invalid, regardless of system state.
class invalid_argument_exception : public base_exception {
public:
    invalid_argument_exception();
    explicit invalid_argument_exception(ss::sstring message);
};

// Deadline expired before RPC could complete or before the client received the
// response.
class deadline_exceeded_exception : public base_exception {
public:
    deadline_exceeded_exception();
    explicit deadline_exceeded_exception(ss::sstring message);
};

// User requested a resource (for example, a file or directory) that can't be
// found.
class not_found_exception : public base_exception {
public:
    not_found_exception();
    explicit not_found_exception(ss::sstring message);
};

// Caller attempted to create a resource that already exists.
class already_exists_exception : public base_exception {
public:
    already_exists_exception();
    explicit already_exists_exception(ss::sstring message);
};

// Caller isn't authorized to perform the operation.
class permission_denied_exception : public base_exception {
public:
    permission_denied_exception();
    explicit permission_denied_exception(ss::sstring message);
};

// Operation can't be completed because some resource is exhausted. Use
// unavailable if the server is temporarily overloaded and the caller should
// retry later.
class resource_exhausted_exception : public base_exception {
public:
    resource_exhausted_exception();
    explicit resource_exhausted_exception(ss::sstring message);
};

// Operation can't be completed because the system isn't in the required state.
class failed_precondition_exception : public base_exception {
public:
    failed_precondition_exception();
    explicit failed_precondition_exception(ss::sstring message);
};

// The operation was aborted, often because of concurrency issues like a
// database transaction abort.
class aborted_exception : public base_exception {
public:
    aborted_exception();
    explicit aborted_exception(ss::sstring message);
};

// The operation was attempted past the valid range.
class out_of_range_exception : public base_exception {
public:
    out_of_range_exception();
    explicit out_of_range_exception(ss::sstring message);
};

// The operation isn't implemented, supported, or enabled.
class unimplemented_exception : public base_exception {
public:
    unimplemented_exception();
    explicit unimplemented_exception(ss::sstring message);
};

// An invariant expected by the underlying system has been broken. Reserved for
// serious errors.
class internal_exception : public base_exception {
public:
    internal_exception();
    explicit internal_exception(ss::sstring message);
};

// The service is currently unavailable, usually transiently. Clients should
// back off and retry idempotent operations.
class unavailable_exception : public base_exception {
public:
    unavailable_exception();
    explicit unavailable_exception(ss::sstring message);
};

// Unrecoverable data loss or corruption.
class data_loss_exception : public base_exception {
public:
    data_loss_exception();
    explicit data_loss_exception(ss::sstring message);
};

// Caller doesn't have valid authentication credentials for the operation.
class unauthenticated_exception : public base_exception {
public:
    unauthenticated_exception();
    explicit unauthenticated_exception(ss::sstring message);
};

} // namespace serde::pb::rpc
