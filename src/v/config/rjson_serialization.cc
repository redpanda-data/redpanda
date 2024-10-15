// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/rjson_serialization.h"

#include "config/tls_config.h"
#include "config/types.h"

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::data_directory_path& v) {
    w.StartObject();

    w.Key("data_directory");
    w.String(v.path.c_str());

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::seed_server& v) {
    w.StartObject();
    w.Key("host");
    rjson_serialize(w, v.addr);
    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::key_cert& v) {
    w.StartObject();
    w.Key("key_file");
    w.String(v.key_file.c_str());

    w.Key("cert_file");
    w.String(v.cert_file.c_str());
    w.EndObject();
}

void rjson_serialize_impl(
  json::Writer<json::StringBuffer>& w, const config::tls_config& v) {
    w.Key("enabled");
    w.Bool(v.is_enabled());

    w.Key("require_client_auth");
    w.Bool(v.get_require_client_auth());

    if (v.get_key_cert_files()) {
        ss::visit(
          v.get_key_cert_files().value(),
          [&w](const config::key_cert& k) {
              w.Key("key_file");
              w.String(k.key_file.c_str());
              w.Key("cert_file");
              w.String(k.cert_file.c_str());
          },
          [&w](const config::p12_container& p) {
              w.Key("p12_file");
              w.String(p.p12_path.c_str());
              w.Key("p12_password");
              w.String("REDACTED");
          });
    }

    if (v.get_truststore_file()) {
        w.Key("truststore_file");
        w.String((*(v.get_truststore_file())).c_str());
    }

    if (v.get_crl_file()) {
        w.Key("crl_file");
        w.String((*(v.get_crl_file())).c_str());
    }
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::tls_config& v) {
    w.StartObject();
    rjson_serialize_impl(w, v);
    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::vector<config::seed_server>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const testing::custom_aggregate& v) {
    w.StartObject();

    w.Key("string_value");
    w.String(v.string_value);

    w.Key("int_value");
    w.Int(v.int_value);

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::endpoint_tls_config& v) {
    w.StartObject();

    w.Key("name");
    w.String(v.name.c_str());
    rjson_serialize_impl(w, v.config);

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::vector<config::endpoint_tls_config>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

/**
 * Helper for enum/bitfield types that implement operator<< for ostream.
 * Otherwise they would be JSON-ized as their integer representation.
 */
template<typename T>
static void stringize(json::Writer<json::StringBuffer>& w, const T& v) {
    w.String(fmt::format("{}", v));
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::compression& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::timestamp_type& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cleanup_policy_bitflags& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::s3_url_style& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_credentials_source& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::partition_autobalancing_mode& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::cloud_storage_backend& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::fetch_read_strategy& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::write_caching_mode& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_storage_chunk_eviction_strategy& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const pandaproxy::schema_registry::schema_id_validation_mode& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::recovery_validation_mode& v) {
    stringize(w, v);
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

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::fips_mode_flag& f) {
    stringize(w, f);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::tls_version& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::node_uuid& v) {
    stringize(w, v);
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::node_id_override& v) {
    w.StartObject();

    w.Key("current_uuid");
    stringize(w, v.key);
    w.Key("new_uuid");
    stringize(w, v.uuid);
    w.Key("new_id");
    stringize(w, v.id);

    w.EndObject();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::vector<config::node_id_override>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::leaders_preference& lp) {
    stringize(w, lp);
}

} // namespace json
