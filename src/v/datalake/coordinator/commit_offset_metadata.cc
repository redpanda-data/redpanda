/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/commit_offset_metadata.h"

#include "bytes/iobuf_parser.h"
#include "json/chunked_buffer.h"
#include "json/document.h"
#include "json/iobuf_writer.h"

#include <exception>

namespace datalake::coordinator {

checked<commit_offset_metadata, parse_offset_error>
parse_commit_offset_json(std::string_view s) {
    json::Document doc;
    try {
        doc.Parse(s.data(), s.size());
    } catch (...) {
        return parse_offset_error{fmt::format(
          "Exception while parsing commit offset: {}: {}",
          std::current_exception(),
          s)};
    }
    if (!doc.IsObject()) {
        return parse_offset_error{fmt::format("Not an object: {}", s)};
    }
    const auto& obj = doc.GetObject();
    auto iter = obj.FindMember("offset");
    if (iter == obj.MemberEnd()) {
        return parse_offset_error{fmt::format("Missing 'offset' field: {}", s)};
    }
    if (!iter->value.IsInt64()) {
        return parse_offset_error{
          fmt::format("'offset' field is not an int64: {}", s)};
    }
    return commit_offset_metadata{
      .offset = model::offset{iter->value.GetInt64()},
    };
}

std::string to_json_str(const commit_offset_metadata& m) {
    json::chunked_buffer buf;
    json::iobuf_writer<json::chunked_buffer> w(buf);
    w.StartObject();
    // TODO: would be more future proof if we add the control topic revision
    // and cluster UUID in here too.
    w.Key("offset");
    w.Int64(m.offset());
    w.EndObject();
    auto p = iobuf_parser(std::move(buf).as_iobuf());
    std::string str;
    str.resize(p.bytes_left());
    p.consume_to(p.bytes_left(), str.data());
    validate_utf8(str);
    return str;
}

} // namespace datalake::coordinator
