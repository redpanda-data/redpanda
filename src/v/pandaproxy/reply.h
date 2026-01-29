/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/client/exceptions.h"
#include "kafka/protocol/exceptions.h"
#include "pandaproxy/error.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/requests/error_reply.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/server.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include <fmt/format.h>

#include <memory>
#include <system_error>

namespace pandaproxy {

inline ss::http::reply::status_type
error_code_to_status(std::error_condition ec) {
    vassert(
      ec.category() == reply_category(),
      "unexpected error_category: {}",
      ec.category().name());

    if (!ec) {
        return ss::http::reply::status_type::ok;
    }

    // Errors are either in the range of
    // * http status codes: [400,600)
    // * proxy error codes: [40000,60000)
    // Proxy error code divided by 100 translates to the http status
    auto value = ec.value() < 600 ? ec.value() : ec.value() / 100;

    vassert(
      value >= 400 && value < 600,
      "unexpected reply_category value: {}",
      ec.value());
    return static_cast<ss::http::reply::status_type>(value);
}

class err_reply_builder {
public:
    using body_t = pandaproxy::json::error_body;

    err_reply_builder()
      : _body{std::nullopt}
      , _rep{std::make_unique<ss::http::reply>()} {}
    explicit err_reply_builder(std::unique_ptr<ss::http::reply> rep)
      : _rep(std::move(rep)) {};

    auto& set_status(ss::http::reply::status_type status) {
        _rep->set_status(status);
        return *this;
    }

    auto& set_json_body(body_t body) {
        _body = std::move(body);
        return *this;
    }

    auto& add_header(const ss::sstring& h, const ss::sstring& value) {
        _rep->add_header(h, value);
        return *this;
    }

    auto& set_mime_type(const ss::sstring& mime) {
        _rep->set_content_type(mime);
        return *this;
    }

    auto& set_reply_unavailable() {
        return set_status(ss::http::reply::status_type::service_unavailable)
          .add_header("Retry-After", "0");
    }

    auto& set_reply_too_many_requests() {
        return set_status(ss::http::reply::status_type::too_many_requests)
          .add_header("Retry-After", "0");
    }

    auto& set_reply_payload_too_large() {
        return set_status(ss::http::reply::status_type::payload_too_large);
    }

    const std::optional<body_t>& get_json_body() const { return _body; }

    std::unique_ptr<ss::http::reply> build() && {
        if (_body) {
            _rep->write_body(
              "json", pandaproxy::json::rjson_serialize(std::move(*_body)));
        }
        return std::move(_rep);
    }

private:
    std::optional<body_t> _body{};
    std::unique_ptr<ss::http::reply> _rep{};
};

inline auto errored_body(std::error_condition ec, ss::sstring msg) {
    err_reply_builder rep;
    rep.set_status(error_code_to_status(ec));
    rep.set_json_body({.ec = ec, .message = std::move(msg)});
    return rep;
}

inline auto errored_body(std::error_code ec, ss::sstring msg) {
    return errored_body(make_error_condition(ec), std::move(msg));
}

inline auto unprocessable_entity(ss::sstring msg) {
    return errored_body(
      make_error_condition(reply_error_code::kafka_bad_request),
      std::move(msg));
}

inline auto shutdown_exception_reply(const std::exception& e) {
    auto eb = errored_body(reply_error_code::kafka_retriable_error, e.what());
    eb.set_reply_unavailable();
    return eb;
}

inline auto exception_reply(ss::logger& log, std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        return shutdown_exception_reply(e);
    } catch (const ss::abort_requested_exception& e) {
        return shutdown_exception_reply(e);
    } catch (const json::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const parse::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const kafka::exception_base& e) {
        return errored_body(e.error, e.what());
    } catch (const schema_registry::exception_base& e) {
        return errored_body(e.code(), e.message());
    } catch (const seastar::httpd::base_exception& e) {
        return errored_body(make_error_condition(e.status()), e.what());
    } catch (...) {
        auto ise = reply_error_code::internal_server_error;
        auto eb = errored_body(ise, make_error_condition(ise).message());
        auto& content = eb.get_json_body();
        vlog(
          log.error,
          "exception_reply: {:?}, exception: {:?}",
          content ? json::rjson_serialize_str(*content) : ss::sstring{},
          std::current_exception());
        return eb;
    }
}

struct exception_replier {
    ss::sstring mime_type;
    ss::logger& log;
    std::unique_ptr<ss::http::reply> operator()(const std::exception_ptr& e) {
        auto res = exception_reply(log, e);
        res.set_mime_type(mime_type);
        return std::move(res).build();
    }
};

} // namespace pandaproxy
