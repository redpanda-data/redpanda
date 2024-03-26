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

#include "json/_include_first.h"
#include "json/prettywriter.h"
#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "net/unresolved_address.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>

#include <chrono>
#include <type_traits>
#include <unordered_map>

namespace json {

void rjson_serialize(json::Writer<json::StringBuffer>& w, short v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, bool v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, long long v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, int v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, unsigned int v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, long v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, unsigned long v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, double v);

void rjson_serialize(json::Writer<json::StringBuffer>& w, std::string_view s);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const net::unresolved_address& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::milliseconds& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::seconds& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::filesystem::path& path);

template<typename T, typename = std::enable_if_t<std::is_enum_v<T>>>
void rjson_serialize(json::Writer<json::StringBuffer>& w, T v) {
    rjson_serialize(w, static_cast<std::underlying_type_t<T>>(v));
}

template<typename T, typename Tag>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const named_type<T, Tag>& v) {
    rjson_serialize(w, v());
}

template<typename T>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::optional<T>& v) {
    if (v) {
        rjson_serialize(w, *v);
        return;
    }
    w.Null();
}

template<typename T, typename A>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::vector<T, A>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename T, size_t chunk_size = 128>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const ss::chunked_fifo<T, chunk_size>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename T>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::unordered_map<typename T::key_type, T>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e.second);
    }
    w.EndArray();
}

template<typename T, typename A>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::circular_buffer<T, A>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

ss::sstring minify(std::string_view json);

ss::sstring prettify(std::string_view json);

} // namespace json
