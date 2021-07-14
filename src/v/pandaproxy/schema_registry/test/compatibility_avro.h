// Copyright 2021 Vectorized, Inc.
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

const auto enum2 = pps::make_avro_schema_definition(R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"]
})")
                     .value();

const auto enum3 = pps::make_avro_schema_definition(R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two", "Three"]
})")
                     .value();

const auto enum_2def = pps::make_avro_schema_definition(
                         R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"],
  "default": "One"
})")
                         .value();

// Schemas defined in AvroCompatibilityTest.java. Used here to ensure
// compatibility with the schema-registry
const auto schema1
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]})")
      .value();
const auto schema2
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]})")
      .value();
const auto schema3
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2"}]})")
      .value();
const auto schema4
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1_new","aliases":["f1"]}]})")
      .value();
const auto schema6
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","doc":"doc of f1"}]})")
      .value();
const auto schema7
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":["null","string","int"],"name":"f1","doc":"doc of f1"}]})")
      .value();
const auto schema8
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]},{"type":"string","name":"f3","default":"bar"}]})")
      .value();
const auto badDefaultNullString_def = pps::make_avro_schema_definition(
  R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","default":"null"},{"type":"string","name":"f2","default":"foo"},{"type":"string","name":"f3","default":"bar"}]})");
