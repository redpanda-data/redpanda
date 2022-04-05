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

const auto enum2 = pps::sanitize_avro_schema_definition(
                     {R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"]
})",
                      pps::schema_type::avro})
                     .value();

const auto enum3 = pps::sanitize_avro_schema_definition(
                     {R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two", "Three"]
})",
                      pps::schema_type::avro})
                     .value();

const auto enum2_def = pps::sanitize_avro_schema_definition(
                         {
                           R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"],
  "default": "One"
})",
                           pps::schema_type::avro})
                         .value();

const auto enum1_mat = pps::sanitize_avro_schema_definition(
                         {
                           R"({
  "type": "record",
  "name": "schema_enum",
  "fields": [
    {
      "name": "f1",
      "type": {
        "type": "enum",
        "name": "enum1",
        "symbols": [
          "E1",
          "E2",
          "E3",
          "E4",
          "E_DEFAULT"
        ]
      }
    }
  ]
})",
                           pps::schema_type::avro})
                         .value();

const auto enum2_mat = pps::sanitize_avro_schema_definition(
                         {
                           R"({
  "type": "record",
  "name": "schema_enum",
  "fields": [
    {
      "name": "f1",
      "type": {
        "type": "enum",
        "name": "enum1",
        "symbols": [
          "E2",
          "E3",
          "E4",
          "E5",
          "E_DEFAULT"
        ],
        "default": "E_DEFAULT"
      }
    }
  ]
})",
                           pps::schema_type::avro})
                         .value();

const auto union0 = pps::sanitize_avro_schema_definition(
                      {R"({
    "name": "init",
    "type": "record",
    "fields": [{
        "name": "inner",
        "type": ["string", "int"]}]
})",
                       pps::schema_type::avro})
                      .value();

const auto union1 = pps::sanitize_avro_schema_definition(
                      {R"({
    "name": "init",
    "type": "record",
    "fields": [{
        "name": "inner",
        "type": ["null", "string"]}]
})",
                       pps::schema_type::avro})
                      .value();

const auto union2 = pps::sanitize_avro_schema_definition(
                      {R"({
    "name": "init",
    "type": "record",
    "fields": [{
        "name": "inner",
        "type": [
            "int", "string", {
                "type": "record",
                "name": "foobar_fields",
                "fields": [{
                    "name": "foo",
                    "type": "string"
                }]
            }
        ]
    }]
})",
                       pps::schema_type::avro})
                      .value();

const auto int_array = pps::sanitize_avro_schema_definition(
                         {R"({
  "name": "test2",
  "type": "array",
  "items": "int"
})",
                          pps::schema_type::avro})
                         .value();

const auto long_array = pps::sanitize_avro_schema_definition(
                          {R"({
  "name": "test2",
  "type": "array",
  "items": "long"
})",
                           pps::schema_type::avro})
                          .value();

// Schemas defined in AvroCompatibilityTest.java. Used here to ensure
// compatibility with the schema-registry
const auto schema1
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]})",
       pps::schema_type::avro})
      .value();
const auto schema2
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]})",
       pps::schema_type::avro})
      .value();
const auto schema2_union_null_first
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":["null","int"],"name":"f2_enum","default":null}]})",
       pps::schema_type::avro})
      .value();
const auto schema3
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2"}]})",
       pps::schema_type::avro})
      .value();
const auto schema4
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1_new","aliases":["f1"]}]})",
       pps::schema_type::avro})
      .value();
const auto schema6
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","doc":"doc of f1"}]})",
       pps::schema_type::avro})
      .value();
const auto schema7
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":["null","string","int"],"name":"f1","doc":"doc of f1"}]})",
       pps::schema_type::avro})
      .value();
const auto schema8
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]},{"type":"string","name":"f3","default":"bar"}]})",
       pps::schema_type::avro})
      .value();
const auto badDefaultNullString_def = pps::sanitize_avro_schema_definition(
  {R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","default":"null"},{"type":"string","name":"f2","default":"foo"},{"type":"string","name":"f3","default":"bar"}]})",
   pps::schema_type::avro});
const auto schema_int
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"int"}]})",
       pps::schema_type::avro})
      .value();
const auto schema_long
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"long"}]})",
       pps::schema_type::avro})
      .value();
const auto schema_float
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"float"}]})",
       pps::schema_type::avro})
      .value();
const auto schema_double
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"double"}]})",
       pps::schema_type::avro})
      .value();
const auto schema_bytes
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"bytes"}]})",
       pps::schema_type::avro})
      .value();
const auto schema_string
  = pps::sanitize_avro_schema_definition(
      {R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]})",
       pps::schema_type::avro})
      .value();
