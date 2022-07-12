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
#include "pandaproxy/probe.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>

#include <system_error>

namespace pandaproxy {

inline ss::httpd::reply::status_type
error_code_to_status(std::error_condition ec) {
    vassert(
      ec.category() == reply_category(),
      "unexpected error_category: {}",
      ec.category().name());

    if (!ec) {
        return ss::httpd::reply::status_type::ok;
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
    return static_cast<ss::httpd::reply::status_type>(value);
}

inline ss::httpd::reply& set_reply_unavailable(ss::httpd::reply& rep) {
    return rep.set_status(ss::httpd::reply::status_type::service_unavailable)
      .add_header("Retry-After", "0");
}

inline std::unique_ptr<ss::httpd::reply> reply_unavailable() {
    auto rep = std::make_unique<ss::httpd::reply>(ss::httpd::reply{});
    set_reply_unavailable(*rep);
    return rep;
}

inline std::unique_ptr<ss::httpd::reply>
errored_body(std::error_condition ec, ss::sstring msg) {
    pandaproxy::json::error_body body{.ec = ec, .message = std::move(msg)};
    auto rep = std::make_unique<ss::httpd::reply>();
    rep->set_status(error_code_to_status(ec));
    auto b = json::rjson_serialize(body);
    rep->write_body("json", b);
    return rep;
}

inline std::unique_ptr<ss::httpd::reply>
errored_body(std::error_code ec, ss::sstring msg) {
    return errored_body(make_error_condition(ec), std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply>
handle_errors(std::error_condition ec, ss::sstring msg, error_probe& eprobe) {
    auto rep = errored_body(ec, std::move(msg));

    using status_type = ss::httpd::reply::status_type;

    // See seastar/http/reply.hh::reply::status_type for a list
    // of supported error status codes.
    if (rep->_status >= status_type{500}) {
        eprobe.increment_5xx();
    } else if (rep->_status >= status_type{400}) {
        eprobe.increment_4xx();
    } else if (rep->_status >= status_type{300}) {
        eprobe.increment_3xx();
    }

    return rep;
}

inline std::unique_ptr<ss::httpd::reply>
handle_errors(std::error_code ec, ss::sstring msg, error_probe& eprobe) {
    return handle_errors(make_error_condition(ec), std::move(msg), eprobe);
}

inline std::unique_ptr<ss::httpd::reply>
exception_reply(std::exception_ptr e, error_probe& eprobe) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        auto eb = handle_errors(
          reply_error_code::kafka_retriable_error, e.what(), eprobe);
        set_reply_unavailable(*eb);
        return eb;
    } catch (const json::exception_base& e) {
        return handle_errors(e.error, e.what(), eprobe);
    } catch (const parse::exception_base& e) {
        return handle_errors(e.error, e.what(), eprobe);
    } catch (const kafka::exception_base& e) {
        return handle_errors(e.error, e.what(), eprobe);
    } catch (const schema_registry::exception_base& e) {
        return handle_errors(e.code(), e.message(), eprobe);
    } catch (const seastar::httpd::base_exception& e) {
        return handle_errors(
          reply_error_code::kafka_bad_request, e.what(), eprobe);
    } catch (...) {
        vlog(plog.error, "{}", std::current_exception());
        eprobe.increment_5xx();
        throw;
    }
}

class exception_replier {
public:
    ss::sstring mime_type;

    exception_replier(ss::sstring mt, error_probe& eprobe)
      : mime_type(mt)
      , _eprobe(eprobe) {}

    std::unique_ptr<ss::httpd::reply> operator()(const std::exception_ptr& e) {
        auto res = exception_reply(e, _eprobe);
        res->set_mime_type(mime_type);
        return res;
    }

private:
    error_probe& _eprobe;
};

} // namespace pandaproxy
