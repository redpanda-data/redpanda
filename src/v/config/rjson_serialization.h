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
#include "config/leaders_preference.h"
#include "config/node_overrides.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "config/types.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"

#include <seastar/core/sstring.hh>

/*
 * this type is only used for config/tests but the rjson_serialize overload
 * needs to be here so it can be picked up by the configuration/property bits
 * (at least without some additional c++ tricks).
 */
namespace testing {
struct custom_aggregate {
    ss::sstring string_value;
    int int_value;

    bool operator==(const custom_aggregate& rhs) const {
        return string_value == rhs.string_value && int_value == rhs.int_value;
    }

    static consteval std::string_view type_name() { return "custom_aggregate"; }
};
} // namespace testing

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
  json::Writer<json::StringBuffer>& w, const testing::custom_aggregate& v);

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
  json::Writer<json::StringBuffer>& w, const config::s3_url_style& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_credentials_source& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::partition_autobalancing_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::cloud_storage_backend& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::fetch_read_strategy& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::write_caching_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const model::cloud_storage_chunk_eviction_strategy& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const pandaproxy::schema_registry::schema_id_validation_mode& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::broker_endpoint&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::recovery_validation_mode&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const config::fips_mode_flag& f);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const config::tls_version& v);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const model::node_uuid&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const config::node_id_override&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&,
  const std::vector<config::node_id_override>&);

void rjson_serialize(
  json::Writer<json::StringBuffer>&, const config::leaders_preference&);

} // namespace json
