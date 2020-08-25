#pragma once

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

inline std::unique_ptr<ss::httpd::reply> exception_reply(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        return reply_unavailable();
    } catch (...) {
        vlog(plog.error, "{}", std::current_exception());
        throw;
    }
}

} // namespace pandaproxy
