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

#include "json/json.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/sstring.hh>

#include <rapidjson/reader.h>
#include <rapidjson/stream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <stdexcept>

namespace pandaproxy::json {

class parse_error final : public std::exception {
public:
    explicit parse_error(size_t offset)
      : _what(fmt::format("parse error at offset {}", offset)) {}
    const char* what() const noexcept final { return _what.data(); }

private:
    std::string _what;
};

template<typename T>
ss::sstring rjson_serialize(const T& v) {
    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> wrt(str_buf);

    using ::json::rjson_serialize;
    using ::pandaproxy::json::rjson_serialize;
    rjson_serialize(wrt, v);

    return ss::sstring(str_buf.GetString(), str_buf.GetSize());
}

template<typename Handler>
CONCEPT(requires std::is_same_v<
        decltype(std::declval<Handler>().result),
        typename Handler::rjson_parse_result>)
typename Handler::rjson_parse_result
  rjson_parse(const char* const s, Handler&& handler) {
    rapidjson::Reader reader;
    rapidjson::StringStream ss(s);
    if (!reader.Parse(ss, handler)) {
        throw parse_error(reader.GetErrorOffset());
    }
    return std::move(handler.result);
}

} // namespace pandaproxy::json
