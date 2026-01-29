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
#include "bytes/iobuf.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <any>
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

enum class content_type : uint8_t {
    json,
    proto,
};

// Context about an RPC request being handled.
struct context {
    ss::sstring service_name;
    ss::sstring method_name;
    content_type content_type;
    // Arbitrary extra values to be able to pass through this context.
    // Access this map using `get_value` and `set_value`.
    // Types in this map may be copied, so ensure types are cheap to copy.
    //
    // Types used in this map:
    // - std::vector<model::node_id> nodes `Via` like header to prevent proxy
    //   loops
    // - request_auth_result the authorization result for the Admin V2 API
    std::map<std::type_index, std::any> values;

    // Return a value associated with this context.
    template<typename T>
    auto get_value(this auto&& self) -> std::conditional_t<
      std::is_const_v<std::remove_reference_t<decltype(self)>>,
      const T&,
      T&> {
        using result_type = std::conditional_t<
          std::is_const_v<std::remove_reference_t<decltype(self)>>,
          const T&,
          T&>;
        return std::any_cast<result_type>(
          self.values.at(std::type_index(typeid(T))));
    }
    // Return a value associated with this context if it exists.
    template<typename T>
    auto get_optional_value(this auto&& self) -> std::conditional_t<
      std::is_const_v<std::remove_reference_t<decltype(self)>>,
      const T*,
      T*> {
        auto it = self.values.find(std::type_index(typeid(T)));
        using result_type = std::conditional_t<
          std::is_const_v<std::remove_reference_t<decltype(self)>>,
          const T&,
          T&>;
        return it == self.values.end()
                 ? nullptr
                 : &std::any_cast<result_type>(it->second);
    }
    // Associate some value with this context.
    template<typename T>
    void set_value(T&& t) {
        values.insert_or_assign(std::type_index(typeid(T)), std::forward<T>(t));
    }
};

// A small descriptor for a route, as well as a method for handling a route
//
// All routes should be POST requests, but the request/reply parsing will be
// handled by the handler method.
struct route_descriptor {
    // Name of the method such as "redpanda.core.admin.AdminService"
    ss::sstring service_name;
    // Name of the method such as "GetRoutes"
    ss::sstring method_name;
    // Path of the route such as "/redpanda.core.admin.AdminService/GetRoutes"
    ss::sstring path;
    // The authentication and authorization level required to access this
    // handler.
    authz_level authz_level;

    // The handler function that will be called to handle the request.
    // This takes a context and the message body as an iobuf, and returns a
    // the resulting serialized iobuf.
    //
    // The resulting future may fail only with exception type inheriting from
    // `serde::pb::rpc::base_exception`.
    std::function<ss::future<iobuf>(context, iobuf)> handler;
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

// Describes the cause of the error with structured details.
//
// Example of an error when contacting the "pubsub.googleapis.com" API when it
// is not enabled:
//     { "reason":   "API_DISABLED"
//       "domain": "googleapis.com"
//       "metadata": {
//         "resource": "projects/123",
//         "service": "pubsub.googleapis.com"
//       }
//     }
// This response indicates that the pubsub.googleapis.com API is not enabled.
//
// Example of an error that is returned when attempting to create a Spanner
// instance in a region that is out of stock:
//     { "reason":   "STOCKOUT"
//       "domain": "spanner.googleapis.com",
//       "metadata": {
//         "availableRegions": "us-central1,us-east2"
//       }
//     }
struct error_info {
    // The reason of the error. This is a constant value that identifies the
    // proximate cause of the error. Error reasons are unique within a
    // particular domain of errors. This should be at most 63 characters and
    // match
    // /[A-Z0-9_]+/.
    ss::sstring reason;

    // The domain of errors that originate from Redpanda core.
    constexpr static std::string_view redpanda_core_domain
      = "redpanda.com/core";

    // The logical grouping to which the "reason" belongs.  Often "domain" will
    // contain the registered service name of the tool or product that is the
    // source of the error. Example: "pubsub.googleapis.com". If the error is
    // common across many APIs, the first segment of the example above will be
    // omitted.  The value will be, "googleapis.com".
    ss::sstring domain = ss::sstring(redpanda_core_domain);

    // Additional structured details about this error.
    //
    // Keys should match /[a-zA-Z0-9-_]/ and be limited to 64 characters in
    // length. When identifying the current value of an exceeded limit, the
    // units should be contained in the key, not the value.  For example, rather
    // than
    // {"instanceLimit": "100/request"}, should be returned as,
    // {"instanceLimitPerRequest": "100"}, if the client exceeds the number of
    // instances that can be created in a single (batch) request.
    chunked_hash_map<ss::sstring, ss::sstring> metadata;
};

// Base Exception when handling RPC requests.
//
// See: https://connectrpc.com/docs/protocol#error-codes
class base_exception : public std::exception {
protected:
    base_exception(
      int status_code,
      ss::sstring code,
      ss::sstring message,
      std::optional<error_info> error_info);

public:
    const char* what() const noexcept override;
    const ss::sstring& message() const;
    const std::optional<error_info>& info() const;

    // Handle the HTTP reply to report the error as suggested.
    std::unique_ptr<ss::http::reply>
      handle(std::unique_ptr<ss::http::reply>) const;

private:
    int _status_code;
    ss::sstring _code;
    ss::sstring _message;
    std::optional<error_info> _error_info;
};

#define CONNECTRPC_DECLARE_EXCEPTION(name)                                     \
    class name##_exception : public base_exception {                           \
    public:                                                                    \
        name##_exception();                                                    \
        explicit name##_exception(ss::sstring message);                        \
        explicit name##_exception(std::optional<error_info>);                  \
        name##_exception(ss::sstring message, std::optional<error_info>);      \
    }

// RPC canceled, usually by the caller.
CONNECTRPC_DECLARE_EXCEPTION(cancelled);

// Catch-all for errors of unclear origin and errors without a more appropriate
// code.
CONNECTRPC_DECLARE_EXCEPTION(unknown);

// Request is invalid, regardless of system state.
CONNECTRPC_DECLARE_EXCEPTION(invalid_argument);

// Deadline expired before RPC could complete or before the client received the
// response.
CONNECTRPC_DECLARE_EXCEPTION(deadline_exceeded);

// User requested a resource (for example, a file or directory) that can't be
// found.
CONNECTRPC_DECLARE_EXCEPTION(not_found);

// Caller attempted to create a resource that already exists.
CONNECTRPC_DECLARE_EXCEPTION(already_exists);

// Caller isn't authorized to perform the operation.
CONNECTRPC_DECLARE_EXCEPTION(permission_denied);

// Operation can't be completed because some resource is exhausted. Use
// unavailable if the server is temporarily overloaded and the caller should
// retry later.
CONNECTRPC_DECLARE_EXCEPTION(resource_exhausted);

// Operation can't be completed because the system isn't in the required state.
CONNECTRPC_DECLARE_EXCEPTION(failed_precondition);

// The operation was aborted, often because of concurrency issues like a
// database transaction abort.
CONNECTRPC_DECLARE_EXCEPTION(aborted);

// The operation was attempted past the valid range.
CONNECTRPC_DECLARE_EXCEPTION(out_of_range);

// The operation isn't implemented, supported, or enabled.
CONNECTRPC_DECLARE_EXCEPTION(unimplemented);

// An invariant expected by the underlying system has been broken. Reserved for
// serious errors.
CONNECTRPC_DECLARE_EXCEPTION(internal);

// The service is currently unavailable, usually transiently. Clients should
// back off and retry idempotent operations.
CONNECTRPC_DECLARE_EXCEPTION(unavailable);

// Unrecoverable data loss or corruption.
CONNECTRPC_DECLARE_EXCEPTION(data_loss);

// Caller doesn't have valid authentication credentials for the operation.
CONNECTRPC_DECLARE_EXCEPTION(unauthenticated);

#undef CONNECTRPC_DECLARE_EXCEPTION

} // namespace serde::pb::rpc
