#pragma once

#include "config/data_directory_path.h"
#include "config/seed_server.h"
#include "config/tests/custom_aggregate.h"
#include "config/tls_config.h"
#include "json/json.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace json {

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const config::data_directory_path& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::seed_server& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::key_cert& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const config::tls_config& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<config::seed_server>& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const custom_aggregate& v);

} // namespace json