#pragma once

#include "json/json.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>

namespace pandaproxy::json {

struct error_body {
    ss::httpd::reply::status_type error_code;
    ss::sstring message;
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const error_body& v) {
    w.StartObject();
    w.Key("error_code");
    ::json::rjson_serialize(w, v.error_code);
    w.Key("message");
    ::json::rjson_serialize(w, v.message);
    w.EndObject();
}

} // namespace pandaproxy::json
