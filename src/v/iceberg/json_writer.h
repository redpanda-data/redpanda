// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "json/chunked_buffer.h"
#include "json/iobuf_writer.h"
#include "strings/utf8.h"

namespace iceberg {

using json_writer = json::iobuf_writer<json::chunked_buffer>;

// NOTE: uses std::string since we're likely parsing JSON alongside a
// thirdparty library that uses std::string.
template<typename T>
std::string to_json_str(const T& t) {
    json::chunked_buffer buf;
    iceberg::json_writer w(buf);
    rjson_serialize(w, t);
    auto p = iobuf_parser(std::move(buf).as_iobuf());
    std::string str;
    str.resize(p.bytes_left());
    p.consume_to(p.bytes_left(), str.data());
    validate_utf8(str);
    return str;
}

} // namespace iceberg
