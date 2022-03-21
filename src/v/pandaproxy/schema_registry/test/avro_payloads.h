// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/avro.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

const ss::sstring avro_string_payload{
  R"(
{
  "schema": "\"string\"",
  "schemaType": "AVRO"
})"};
const ss::sstring expected_avro_string_def{R"({"string"})"};

const ss::sstring avro_int_payload{
  R"(
{
  "schema": "\"int\"",
  "schemaType": "AVRO"
})"};
const ss::sstring expected_avro_int_def{R"({"int"})"};

const ss::sstring avro_long_payload{
  R"(
{
  "schema": "\"long\"",
  "schemaType": "AVRO"
})"};
const ss::sstring expected_avro_long_def{R"({"long"})"};
