/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/data_directory_path.h"
#include "config/endpoint_tls_config.h"
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

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const config::endpoint_tls_config& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const std::vector<config::endpoint_tls_config>& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const model::compression& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const model::timestamp_type& v);

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const model::cleanup_policy_bitflags& v);

} // namespace json
