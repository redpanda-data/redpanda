/*
 * Copyright 2020 Vectorized, Inc.
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
#include "pandaproxy/json/requests/error_reply.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>

namespace pandaproxy {

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
errored_body(ss::httpd::reply::status_type status, ss::sstring msg) {
    pandaproxy::json::error_body body{
      .error_code = status, .message = std::move(msg)};
    auto rep = std::make_unique<ss::httpd::reply>();
    rep->set_status(body.error_code);
    auto b = json::rjson_serialize(body);
    rep->write_body("json", b);
    return rep;
}

inline std::unique_ptr<ss::httpd::reply> unprocessable_entity(ss::sstring msg) {
    return errored_body(ss::httpd::reply::status_type(422), std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply> not_found(ss::sstring msg) {
    return errored_body(
      ss::httpd::reply::status_type::not_found, std::move(msg));
}

inline std::unique_ptr<ss::httpd::reply> exception_reply(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        return reply_unavailable();
    } catch (const pandaproxy::json::parse_error& e) {
        return unprocessable_entity(e.what());
    } catch (const kafka::client::consumer_error& e) {
        if (e.error == kafka::error_code::unknown_member_id) {
            return not_found(e.what());
        } else {
            throw;
        }
    } catch (...) {
        vlog(plog.error, "{}", std::current_exception());
        throw;
    }
}

} // namespace pandaproxy
