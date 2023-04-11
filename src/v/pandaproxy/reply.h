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

#include "kafka/client/exceptions.h"
#include "kafka/protocol/exceptions.h"
#include "pandaproxy/error.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>

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

inline ss::http::reply& set_reply_unavailable(ss::http::reply& rep) {
    return rep.set_status(ss::http::reply::status_type::service_unavailable)
      .add_header("Retry-After", "0");
}

inline std::unique_ptr<ss::http::reply> reply_unavailable() {
    auto rep = std::make_unique<ss::http::reply>(ss::http::reply{});
    set_reply_unavailable(*rep);
    return rep;
}

inline std::unique_ptr<ss::http::reply>
errored_body(std::error_condition ec, ss::sstring msg) {
    pandaproxy::json::error_body body{.ec = ec, .message = std::move(msg)};
    auto rep = std::make_unique<ss::http::reply>();
    rep->set_status(error_code_to_status(ec));
    auto b = json::rjson_serialize(body);
    rep->write_body("json", b);
    return rep;
}

inline std::unique_ptr<ss::http::reply>
errored_body(std::error_code ec, ss::sstring msg) {
    return errored_body(make_error_condition(ec), std::move(msg));
}

inline std::unique_ptr<ss::http::reply> unprocessable_entity(ss::sstring msg) {
    return errored_body(
      make_error_condition(reply_error_code::kafka_bad_request),
      std::move(msg));
}

inline std::unique_ptr<ss::http::reply> exception_reply(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        auto eb = errored_body(
          reply_error_code::kafka_retriable_error, e.what());
        set_reply_unavailable(*eb);
        return eb;
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
        vlog(
          plog.error,
          "exception_reply: {}, exception: {}",
          eb->_content,
          std::current_exception());
        return eb;
    }
}

struct exception_replier {
    ss::sstring mime_type;
    std::unique_ptr<ss::http::reply> operator()(const std::exception_ptr& e) {
        auto res = exception_reply(e);
        res->set_mime_type(mime_type);
        return res;
    }
};

} // namespace pandaproxy
