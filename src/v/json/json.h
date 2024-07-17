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

#include "bytes/iobuf.h"
#include "json/_include_first.h"
#include "json/prettywriter.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>

#include <chrono>
#include <type_traits>
#include <unordered_map>

namespace json {

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, short v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, bool v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, long long v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, int v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, unsigned int v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, long v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, unsigned long v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, double v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, std::string_view s);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, const net::unresolved_address& v);

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w, const std::chrono::milliseconds& v);

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, const std::chrono::seconds& v);

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w, const std::filesystem::path& path);

template<
  typename Buffer,
  typename T,
  typename = std::enable_if_t<std::is_enum_v<T>>>
void rjson_serialize(json::Writer<Buffer>& w, T v) {
    rjson_serialize(w, static_cast<std::underlying_type_t<T>>(v));
}

template<typename Buffer, typename T, typename Tag>
void rjson_serialize(json::Writer<Buffer>& w, const named_type<T, Tag>& v) {
    rjson_serialize(w, v());
}

template<typename Buffer, typename T>
void rjson_serialize(json::Writer<Buffer>& w, const std::optional<T>& v) {
    if (v) {
        rjson_serialize(w, *v);
        return;
    }
    w.Null();
}

template<typename Buffer, typename T, typename A>
void rjson_serialize(json::Writer<Buffer>& w, const std::vector<T, A>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename Buffer, typename T, size_t chunk_size>
void rjson_serialize(
  json::Writer<Buffer>& w, const ss::chunked_fifo<T, chunk_size>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename Buffer, typename T>
void rjson_serialize(
  json::Writer<Buffer>& w,
  const std::unordered_map<typename T::key_type, T>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e.second);
    }
    w.EndArray();
}

template<typename Buffer, typename T, typename A>
void rjson_serialize(
  json::Writer<Buffer>& w, const ss::circular_buffer<T, A>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

ss::sstring minify(std::string_view json);
iobuf minify(iobuf json);

ss::sstring prettify(std::string_view json);

} // namespace json
