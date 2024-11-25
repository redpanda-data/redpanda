// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "json/json.h"

#include "json/chunked_buffer.h"
#include "json/chunked_input_stream.h"
#include "json/stringbuffer.h"

namespace json {

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, short v) {
    w.Int(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, bool v) {
    w.Bool(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, long long v) {
    w.Int64(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, int v) {
    w.Int(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, unsigned int v) {
    w.Uint(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, long v) {
    w.Int64(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, unsigned long v) {
    w.Uint64(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, double v) {
    w.Double(v);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, std::string_view v) {
    w.String(v.data(), v.size());
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, const ss::socket_address& v) {
    w.StartObject();

    std::ostringstream a;
    a << v.addr();
    if (!a.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "failed to format socket_address, state: {}",
          a.rdstate()));
    }

    w.Key("address");
    w.String(a.str());

    w.Key("port");
    auto prt = v.port();

    if constexpr (std::is_unsigned_v<decltype(prt)>) {
        w.Uint(prt);
    } else {
        w.Int(prt);
    }
    w.EndObject();
}

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w, const net::unresolved_address& v) {
    w.StartObject();

    w.Key("address");
    w.String(v.host().c_str());

    w.Key("port");
    w.Uint(v.port());

    w.EndObject();
}

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w, const std::chrono::milliseconds& v) {
    uint64_t _tmp = v.count();
    rjson_serialize(w, _tmp);
}

template<typename Buffer>
void rjson_serialize(json::Writer<Buffer>& w, const std::chrono::seconds& v) {
    uint64_t _tmp = v.count();
    rjson_serialize(w, _tmp);
}

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w, const std::filesystem::path& path) {
    rjson_serialize(w, std::string_view{path.native()});
}

ss::sstring minify(std::string_view json) {
    json::Reader r;
    json::StringStream in(json.data());
    json::StringBuffer out;
    json::Writer<json::StringBuffer> w{out};
    r.Parse(in, w);
    return ss::sstring(out.GetString(), out.GetSize());
}

iobuf minify(iobuf json) {
    json::Reader r;
    json::chunked_input_stream in(std::move(json));
    json::chunked_buffer out;
    json::Writer<json::chunked_buffer> w{out};
    r.Parse(in, w);
    return std::move(out).as_iobuf();
}

ss::sstring prettify(std::string_view json) {
    json::Reader r;
    json::StringStream in(json.data());
    json::StringBuffer out;
    json::PrettyWriter<json::StringBuffer> w{out};
    r.Parse(in, w);
    return ss::sstring(out.GetString(), out.GetSize());
}

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, short v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, bool v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, long long v);

template void
rjson_serialize<json::StringBuffer>(json::Writer<json::StringBuffer>& w, int v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, unsigned int v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, long v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, unsigned long v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, double v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, std::string_view s);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, const net::unresolved_address& v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, const std::chrono::milliseconds& v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, const std::chrono::seconds& v);

template void rjson_serialize<json::StringBuffer>(
  json::Writer<json::StringBuffer>& w, const std::filesystem::path& path);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, short v);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, bool v);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, long long v);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, int v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, unsigned int v);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, long v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, unsigned long v);

template void
rjson_serialize<chunked_buffer>(json::Writer<chunked_buffer>& w, double v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, std::string_view s);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, const net::unresolved_address& v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, const std::chrono::milliseconds& v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, const std::chrono::seconds& v);

template void rjson_serialize<chunked_buffer>(
  json::Writer<chunked_buffer>& w, const std::filesystem::path& path);

} // namespace json
