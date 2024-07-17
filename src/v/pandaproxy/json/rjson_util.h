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
#include "json/chunked_input_stream.h"
#include "json/iobuf_writer.h"
#include "json/json.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/types.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/request.hh>

#include <concepts>
#include <memory>

namespace pandaproxy::json {

template<typename T>
class rjson_parse_impl;

template<typename T>
class rjson_serialize_impl;

template<typename Buffer, typename T>
Buffer rjson_serialize_buf(T&& v) {
    Buffer buf;
    ::json::iobuf_writer<Buffer> wrt{buf};

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
    bool operator()(::json::iobuf_writer<Buffer>& w, T&& t) {
        return rjson_serialize_impl<std::remove_reference_t<T>>{fmt}(
          w, std::forward<T>(t));
    }
};

inline rjson_serialize_fmt_impl rjson_serialize_fmt(serialization_format fmt) {
    return rjson_serialize_fmt_impl{fmt};
}

namespace impl {

template<typename Handler>
concept RjsonParseHandler = requires {
    std::same_as<
      decltype(std::declval<Handler>().result),
      typename Handler::rjson_parse_result>;
};

template<typename IStream, typename Arg, impl::RjsonParseHandler Handler>
typename Handler::rjson_parse_result
rjson_parse_buf(Arg&& arg, Handler&& handler) {
    ::json::Reader reader;
    IStream ss(std::forward<Arg>(arg));
    if (!reader.Parse(ss, handler)) {
        throw parse_error(reader.GetErrorOffset());
    }
    return std::forward<Handler>(handler).result;
}

/// \brief Parse a payload using the handler.
///
/// \warning rjson_parse is preferred, since it can be chunked.
template<impl::RjsonParseHandler Handler>
typename Handler::rjson_parse_result
rjson_parse(const char* const s, Handler&& handler) {
    return impl::rjson_parse_buf<::json::StringStream>(
      s, std::forward<Handler>(handler));
}

} // namespace impl

///\brief Parse a payload using the handler.
template<impl::RjsonParseHandler Handler>
typename Handler::rjson_parse_result rjson_parse(iobuf buf, Handler&& handler) {
    return impl::rjson_parse_buf<::json::chunked_input_stream>(
      std::move(buf), std::forward<Handler>(handler));
}

///\brief Parse a request body using the handler.
template<impl::RjsonParseHandler Handler>
typename ss::future<typename Handler::rjson_parse_result>
rjson_parse(std::unique_ptr<ss::http::request> req, Handler handler) {
    if (!req->content.empty()) {
        co_return impl::rjson_parse(req->content.data(), std::move(handler));
    }

    iobuf buf;
    auto is = req->content_stream;
    co_await ss::repeat([&buf, &is]() {
        return is->read().then([&buf](ss::temporary_buffer<char> tmp_buf) {
            if (tmp_buf.empty()) {
                return ss::make_ready_future<ss::stop_iteration>(
                  ss::stop_iteration::yes);
            }
            buf.append(std::make_unique<iobuf::fragment>(std::move(tmp_buf)));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        });
    });

    co_return rjson_parse(std::move(buf), std::move(handler));
}

} // namespace pandaproxy::json
