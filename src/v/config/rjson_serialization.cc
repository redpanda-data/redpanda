#include "config/rjson_serialization.h"

namespace config {

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
  rapidjson::Writer<rapidjson::StringBuffer>& w, const ss::sstring& v) {
    w.String(v.c_str());
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

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const data_directory_path& v) {
    w.StartObject();

    w.Key("data_directory");
    w.String(v.path.c_str());

    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const seed_server& v) {
    w.StartObject();
    w.Key("node_id");
    rjson_serialize(w, v.id());

    w.Key("host");
    rjson_serialize(w, v.addr);
    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const key_cert& v) {
    w.StartObject();
    w.Key("key_file");
    w.String(v.key_file.c_str());

    w.Key("cert_file");
    w.String(v.cert_file.c_str());
    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const tls_config& v) {
    w.StartObject();
    w.Key("enabled");
    w.Bool(v.is_enabled());

    w.Key("client_auth");
    w.Bool(v.get_require_client_auth());

    if (v.get_key_cert_files()) {
        w.Key("key_cert");
        rjson_serialize(w, *(v.get_key_cert_files()));
    }

    if (v.get_truststore_file()) {
        w.Key("truststore_file");
        w.String((*(v.get_truststore_file())).c_str());
    }

    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<seed_server>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<ss::sstring>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

} // namespace config
