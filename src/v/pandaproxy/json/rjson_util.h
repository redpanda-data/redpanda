#pragma once

#include "json/json.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/sstring.hh>

#include <rapidjson/reader.h>
#include <rapidjson/stream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::json {

template<typename T>
ss::sstring rjson_serialize(const T& v) {
    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> wrt(str_buf);

    using ::json::rjson_serialize;
    using ::pandaproxy::json::rjson_serialize;
    rjson_serialize(wrt, v);

    return {str_buf.GetString(), str_buf.GetLength()};
}

template<typename Handler>
CONCEPT(requires std::is_same_v<
        decltype(std::declval<Handler>().result),
        typename Handler::rjson_parse_result>)
typename Handler::rjson_parse_result
  rjson_parse(const char* const s, Handler&& handler) {
    rapidjson::Reader reader;
    rapidjson::StringStream ss(s);
    reader.Parse(ss, handler);
    return std::move(handler.result);
}

} // namespace pandaproxy::json
