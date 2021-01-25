// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/rjson_serialization.h"

namespace json {

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const config::data_directory_path& v) {
    w.StartObject();

    w.Key("data_directory");
    w.String(v.path.c_str());

    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::seed_server& v) {
    w.StartObject();
    w.Key("host");
    rjson_serialize(w, v.addr);
    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::key_cert& v) {
    w.StartObject();
    w.Key("key_file");
    w.String(v.key_file.c_str());

    w.Key("cert_file");
    w.String(v.cert_file.c_str());
    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::tls_config& v) {
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
  const std::vector<config::seed_server>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const custom_aggregate& v) {
    w.StartObject();

    w.Key("string_value");
    w.String(v.string_value);

    w.Key("int_value");
    w.Int(v.int_value);

    w.EndObject();
}

} // namespace json
