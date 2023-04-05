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

#include "json/json.h"
#include "json/prettywriter.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/types.h"

#include <seastar/core/sstring.hh>

#include <stdexcept>

namespace pandaproxy::json {

template<typename T>
ss::sstring rjson_serialize(const T& v) {
    ::json::StringBuffer str_buf;
    ::json::Writer<::json::StringBuffer> wrt(str_buf);

    using ::json::rjson_serialize;
    using ::pandaproxy::json::rjson_serialize;
    rjson_serialize(wrt, v);

    return ss::sstring(str_buf.GetString(), str_buf.GetSize());
}

struct rjson_serialize_fmt_impl {
    explicit rjson_serialize_fmt_impl(serialization_format fmt)
      : fmt{fmt} {}

    serialization_format fmt;
    template<typename T>
    bool operator()(T&& t) {
        return rjson_serialize_impl<std::remove_reference_t<T>>{fmt}(
          std::forward<T>(t));
    }
    template<typename T>
    bool operator()(::json::Writer<::json::StringBuffer>& w, T&& t) {
        return rjson_serialize_impl<std::remove_reference_t<T>>{fmt}(
          w, std::forward<T>(t));
    }
};

inline rjson_serialize_fmt_impl rjson_serialize_fmt(serialization_format fmt) {
    return rjson_serialize_fmt_impl{fmt};
}

template<typename Handler>
requires std::is_same_v<
  decltype(std::declval<Handler>().result),
  typename Handler::rjson_parse_result>
typename Handler::rjson_parse_result
rjson_parse(const char* const s, Handler&& handler) {
    ::json::Reader reader;
    ::json::StringStream ss(s);
    if (!reader.Parse(ss, handler)) {
        throw parse_error(reader.GetErrorOffset());
    }
    return std::move(handler.result);
}

inline ss::sstring minify(std::string_view json) {
    ::json::Reader r;
    ::json::StringStream in(json.data());
    ::json::StringBuffer out;
    ::json::Writer<::json::StringBuffer> w{out};
    r.Parse(in, w);
    return ss::sstring(out.GetString(), out.GetSize());
}

inline ss::sstring prettify(std::string_view json) {
    ::json::Reader r;
    ::json::StringStream in(json.data());
    ::json::StringBuffer out;
    ::json::PrettyWriter<::json::StringBuffer> w{out};
    r.Parse(in, w);
    return ss::sstring(out.GetString(), out.GetSize());
}

} // namespace pandaproxy::json
