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

#include "base/seastarx.h"
#include "config/data_directory_path.h"
#include "config/endpoint_tls_config.h"
#include "config/node_overrides.h"
#include "config/seed_server.h"
#include "config/tests/custom_aggregate.h"
#include "config/tls_config.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"

#include <seastar/core/sstring.hh>

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::data_directory_path& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::seed_server& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::key_cert& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::tls_config& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::vector<config::seed_server>& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const custom_aggregate& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::endpoint_tls_config& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const std::vector<config::endpoint_tls_config>& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::compression& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::timestamp_type& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::cleanup_policy_bitflags& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_credentials_source& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::partition_autobalancing_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::cloud_storage_backend& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::leader_balancer_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::fetch_read_strategy& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::write_caching_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_storage_chunk_eviction_strategy& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const pandaproxy::schema_registry::subject_name_strategy& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const pandaproxy::schema_registry::schema_id_validation_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::broker_endpoint&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::recovery_validation_mode&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::node_uuid&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const config::node_id_override&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const std::vector<config::node_id_override>&);

} // namespace json
