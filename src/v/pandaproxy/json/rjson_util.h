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

#include "bytes/iostream.h"
#include "json/chunked_buffer.h"
#include "json/json.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/types.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::json {

template<typename T>
class rjson_parse_impl;

template<typename T>
class rjson_serialize_impl;

template<typename Buffer, typename T>
Buffer rjson_serialize_buf(T&& v) {
    Buffer buf;
    ::json::Writer<Buffer> wrt{buf};

    using ::json::rjson_serialize;
    using ::pandaproxy::json::rjson_serialize;
    rjson_serialize(wrt, std::forward<T>(v));

    return buf;
}

template<typename T>
ss::sstring rjson_serialize_str(T&& v) {
    auto str_buf = rjson_serialize_buf<::json::StringBuffer>(
      std::forward<T>(v));
    return {str_buf.GetString(), str_buf.GetSize()};
}

template<typename T>
iobuf rjson_serialize_iobuf(T&& v) {
    return rjson_serialize_buf<::json::chunked_buffer>(std::forward<T>(v))
      .as_iobuf();
}

inline ss::noncopyable_function<ss::future<>(ss::output_stream<char>&& os)>
as_body_writer(iobuf&& buf) {
    return [buf{std::move(buf)}](ss::output_stream<char>&& os) mutable {
        return ss::do_with(
          std::move(buf), std::move(os), [](auto& buf, auto& os) {
              return write_iobuf_to_output_stream(std::move(buf), os)
                .finally([&os] { return os.close(); });
          });
    };
}

template<typename T>
ss::noncopyable_function<ss::future<>(ss::output_stream<char>&& os)>
rjson_serialize(T&& v) {
    return as_body_writer(rjson_serialize_iobuf(std::forward<T>(v)));
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
    template<typename Buffer, typename T>
    bool operator()(::json::Writer<Buffer>& w, T&& t) {
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

} // namespace pandaproxy::json
