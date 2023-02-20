// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "json/json.h"

namespace json {

void rjson_serialize(json::Writer<json::StringBuffer>& w, short v) { w.Int(v); }

void rjson_serialize(json::Writer<json::StringBuffer>& w, bool v) { w.Bool(v); }

void rjson_serialize(json::Writer<json::StringBuffer>& w, long long v) {
    w.Int64(v);
}

void rjson_serialize(json::Writer<json::StringBuffer>& w, int v) { w.Int(v); }

void rjson_serialize(json::Writer<json::StringBuffer>& w, unsigned int v) {
    w.Uint(v);
}

void rjson_serialize(json::Writer<json::StringBuffer>& w, long v) {
    w.Int64(v);
}

void rjson_serialize(json::Writer<json::StringBuffer>& w, unsigned long v) {
    w.Uint64(v);
}

void rjson_serialize(json::Writer<json::StringBuffer>& w, double v) {
    w.Double(v);
}

void rjson_serialize(json::Writer<json::StringBuffer>& w, std::string_view v) {
    w.String(v.data(), v.size());
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::socket_address& v) {
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

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const net::unresolved_address& v) {
    w.StartObject();

    w.Key("address");
    w.String(v.host().c_str());

    w.Key("port");
    w.Uint(v.port());

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::milliseconds& v) {
    uint64_t _tmp = v.count();
    rjson_serialize(w, _tmp);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::seconds& v) {
    uint64_t _tmp = v.count();
    rjson_serialize(w, _tmp);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::broker_endpoint& ep) {
    w.StartObject();
    w.Key("name");
    w.String(ep.name);
    w.Key("address");
    w.String(ep.address.host());
    w.Key("port");
    w.Uint(ep.address.port());
    w.EndObject();
}

} // namespace json
