/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/json_writer.h"
#include "iceberg/table_requests.h"
#include "json/document.h"

namespace iceberg {

load_table_result parse_load_table_result(const json::Value&);
commit_table_response parse_commit_table_response(const json::Value&);
create_namespace_response parse_create_namespace_response(const json::Value&);

} // namespace iceberg
namespace json {

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::create_table_request& r);

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::table_identifier& r);

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::commit_table_request& r);

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::create_namespace_request& r);

} // namespace json
