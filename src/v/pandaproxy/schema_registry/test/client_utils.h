// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "http/client.h"
#include "json/document.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/reply.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/utils.h"

#include <absl/algorithm/container.h>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>

#include <iterator>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

inline iobuf make_body(const ss::sstring& body) {
    iobuf buf;
    buf.append(body.data(), body.size());
    return buf;
}

inline auto put_config(
  http::client& client, const pps::subject& sub, pps::compatibility_level lvl) {
    return http_request(
      client,
      fmt::format("/config/{}", sub()),
      make_body(
        fmt::format(R"({{"compatibility": "{}"}})", to_string_view(lvl))),
      boost::beast::http::verb::put,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}

inline auto post_schema(
  http::client& client, const pps::subject& sub, const ss::sstring& payload) {
    return http_request(
      client,
      fmt::format("/subjects/{}/versions", sub()),
      make_body(payload),
      boost::beast::http::verb::post,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}

inline auto delete_subject(
  http::client& client,
  const pps::subject& sub,
  pps::permanent_delete del = {}) {
    return http_request(
      client,
      fmt::format("/subjects/{}?permanent={}", sub(), del),
      boost::beast::http::verb::delete_,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}

inline auto delete_subject_version(
  http::client& client,
  const pps::subject& sub,
  pps::schema_version ver,
  pps::permanent_delete del = {}) {
    return http_request(
      client,
      fmt::format("/subjects/{}/versions/{}?permanent={}", sub(), ver(), del),
      boost::beast::http::verb::delete_,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}

inline auto get_subject_versions(
  http::client& client,
  const pps::subject& sub,
  pps::include_deleted del = {}) {
    return http_request(
      client,
      fmt::format("/subjects/{}/versions?deleted={}", sub(), del),
      boost::beast::http::verb::get,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}

inline std::vector<pps::schema_version>
get_body_versions(const ss::sstring& body) {
    json::Document doc;
    if (doc.Parse(body).HasParseError()) {
        throw ppj::parse_error(doc.GetErrorOffset());
    }
    if (!doc.IsArray()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "Body is not an array"};
    }
    const auto& arr = doc.GetArray();
    std::vector<pps::schema_version> found_versions;
    found_versions.reserve(arr.Size());
    absl::c_transform(
      arr, std::back_inserter(found_versions), [](const auto& v) {
          return pps::schema_version{v.template Get<int>()};
      });

    return found_versions;
}

inline int get_body_error_code(const ss::sstring& body) {
    json::Document doc;
    if (doc.Parse(body).HasParseError()) {
        throw ppj::parse_error(doc.GetErrorOffset());
    }
    if (!doc.IsObject()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "Body is not an object"};
    }
    auto obj = doc.GetObject();
    if (!obj["error_code"].IsInt()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "error_code not an int"};
    }
    return obj["error_code"].Get<int>();
}

inline ppj::error_body get_error_body(const ss::sstring& body) {
    json::Document doc;
    if (doc.Parse(body).HasParseError()) {
        throw ppj::parse_error(doc.GetErrorOffset());
    }
    if (!doc.IsObject()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "Body is not an object"};
    }
    auto obj = doc.GetObject();
    auto e_it = obj.FindMember("error_code");
    if (e_it == obj.MemberEnd()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "error_code field doesn't exist"};
    }
    if (!e_it->value.IsInt()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "error_code field not an int"};
    }

    auto m_it = obj.FindMember("message");
    if (m_it == obj.MemberEnd()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "message field doesn't exist"};
    }
    if (!m_it->value.IsString()) {
        throw ppj::exception_base{
          ppj::error_code::invalid_json, "message field not a string"};
    }
    return {
      pp::reply_error_code{static_cast<uint16_t>(e_it->value.Get<unsigned>())},
      m_it->value.Get<std::string>()};
}
