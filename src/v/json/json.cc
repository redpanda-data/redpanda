#include "json/json.h"

namespace json {

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, short v) {
    w.Int(v);
}

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, bool v) {
    w.Bool(v);
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, long long v) {
    w.Int64(v);
}

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, int v) {
    w.Int(v);
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, unsigned int v) {
    w.Uint(v);
}

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, long v) {
    w.Int64(v);
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, unsigned long v) {
    w.Uint64(v);
}

void rjson_serialize(rapidjson::Writer<rapidjson::StringBuffer>& w, double v) {
    w.Double(v);
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, std::string_view v) {
    w.String(v.data(), v.size());
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const ss::socket_address& v) {
    w.StartObject();

    std::ostringstream a;
    a << v.addr();

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
  rapidjson::Writer<rapidjson::StringBuffer>& w, const unresolved_address& v) {
    w.StartObject();

    w.Key("address");
    w.String(v.host().c_str());

    w.Key("port");
    w.Uint(v.port());

    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::chrono::milliseconds& v) {
    uint64_t _tmp = v.count();
    rjson_serialize(w, _tmp);
}

} // namespace json
