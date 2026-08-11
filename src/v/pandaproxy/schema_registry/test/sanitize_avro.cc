// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/test/compatibility_avro.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

const pps::schema_definition not_minimal{
  R"({
   "type": "record",
   "name": "myrecord",
   "fields": [{"type":"string","name":"f1"}]
})",
  pps::schema_type::avro};

const pps::schema_definition not_minimal_sanitized{
  R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]})",
  pps::schema_type::avro};

const pps::schema_definition leading_dot{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".r1","type":"record"}]},{"name":"two","type":["null",".r1"]}]})",
  pps::schema_type::avro};

const pps::schema_definition leading_dot_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","string"]}]}]},{"name":"two","type":["null","r1"]}]})",
  pps::schema_type::avro};

const pps::schema_definition leading_dot_ns{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".ns.r1","type":"record"}]},{"name":"two","type":["null",".ns.r1"]}]})",
  pps::schema_type::avro};

const pps::schema_definition leading_dot_ns_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"type":"record","name":"r1","namespace":".ns","fields":[{"name":"f1","type":["null","string"]}]}]},{"name":"two","type":["null",".ns.r1"]}]})",
  pps::schema_type::avro};

const pps::schema_definition record_not_sorted{
  R"({"name":"sort_record","type":"record","aliases":["alias"],"fields":[{"type":"string","name":"one"}],"namespace":"ns","doc":"doc"})",
  pps::schema_type::avro};

const pps::schema_definition record_sorted_sanitized{
  R"({"type":"record","name":"sort_record","namespace":"ns","doc":"doc","fields":[{"name":"one","type":"string"}],"aliases":["alias"]})",
  pps::schema_type::avro};

const pps::schema_definition enum_not_sorted{
  R"({"name":"ns.sort_enum","type":"enum","aliases":["alias"],"symbols":["one", "two", "three"],"default":"two","doc":"doc"})",
  pps::schema_type::avro};

const pps::schema_definition enum_sorted_sanitized{
  R"({"type":"enum","name":"sort_enum","namespace":"ns","doc":"doc","symbols":["one","two","three"],"default":"two","aliases":["alias"]})",
  pps::schema_type::avro};

const pps::schema_definition array_not_sorted{
  R"({"type": "array", "default": [], "items" : "string"})",
  pps::schema_type::avro};

const pps::schema_definition array_sorted_sanitized{
  R"({"type":"array","items":"string","default":[]})", pps::schema_type::avro};

const pps::schema_definition map_not_sorted{
  R"({"type": "map", "default": {}, "values" : "string"})",
  pps::schema_type::avro};

const pps::schema_definition map_sorted_sanitized{
  R"({"type":"map","values":"string","default":{}})", pps::schema_type::avro};

const pps::schema_definition fixed_not_sorted{
  R"({"size":16, "type": "fixed", "aliases":["fixed"], "name":"ns.sorted_fixed"})",
  pps::schema_type::avro};

const pps::schema_definition fixed_sorted_sanitized{
  R"({"type":"fixed","name":"sorted_fixed","namespace":"ns","size":16,"aliases":["fixed"]})",
  pps::schema_type::avro};

const pps::schema_definition record_of_obj_unsanitized{
  R"({"name":"sort_record_of_obj","type":"record","fields":[{"type":{"type":"string","connect.parameters":{"tidb_type":"TEXT"}},"default":"","name":"field"}]})",
  pps::schema_type::avro};

const pps::schema_definition record_of_obj_sanitized{
  R"({"type":"record","name":"sort_record_of_obj","fields":[{"name":"field","type":{"type":"string","connect.parameters":{"tidb_type":"TEXT"}},"default":""}]})",
  pps::schema_type::avro};

const pps::schema_definition namespace_nested_same_unsanitized{
  R"({
  "type": "record",
  "name": "Example",
  "doc": "A simple name (attribute) and no namespace attribute: use the null namespace; the fullname is 'Example'.",
  "fields": [
    {
      "name": "inheritNull",
      "type": {
        "type": "enum",
        "name": "Simple",
        "doc": "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.",
        "symbols": [
          "a",
          "b"
        ]
      }
    },
    {
      "name": "fullName",
      "type": {
        "type": "fixed",
        "name": "a.full.Name",
        "namespace": "explicit",
        "doc": "A name (attribute) and a namespace (attribute). The fullname is 'a.full.Name', and the namespace is 'a.full'.",
        "size": 12
      }
    },
    {
      "name": "explicitNamespace",
      "type": {
        "type": "record",
        "name": "Simple",
        "namespace": "explicit",
        "doc": "A simple name (attribute) and a namespace (attribute); the fullname is 'explicit.Simple' (this is a different type than of the 'inheritNull' field).",
        "fields": [
          {
            "name": "inheritNamespace",
            "type": {
              "type": "enum",
              "name": "Understanding",
              "doc": "A simple name (attribute) and no namespace attribute: inherit the namespace of the enclosing type 'explicit.Simple'. The fullname is 'explicit.Understanding'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "duplicateNamespace",
            "type": {
              "type": "enum",
              "name": "Reduction",
              "namespace": "explicit",
              "doc": "A simple name (attribute) and namespace attribute: same namespace of the enclosing type 'explicit'. The fullname is 'explicit.Reduction'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "emptyNamespace",
            "type": {
              "type": "enum",
              "name": "NullNamespace",
              "namespace": "",
              "doc": "A simple name (attribute) and namespace attribute: namespace is explicitly null. The fullname is 'NullNamespace'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "emptyFullname",
            "type": {
              "type": "enum",
              "name": ".NullFullname",
              "doc": "A name (attribute) and no namespace attribute: namespace is null. The fullname is 'NullFullname'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          }
        ]
      }
    }
  ]
})",
  pps::schema_type::avro};

const pps::schema_definition namespace_nested_same_sanitized{
  ::json::minify(
    R"({
  "type": "record",
  "name": "Example",
  "doc": "A simple name (attribute) and no namespace attribute: use the null namespace; the fullname is 'Example'.",
  "fields": [
    {
      "name": "inheritNull",
      "type": {
        "type": "enum",
        "name": "Simple",
        "doc": "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.",
        "symbols": [
          "a",
          "b"
        ]
      }
    },
    {
      "name": "fullName",
      "type": {
        "type": "fixed",
        "name": "Name",
        "namespace": "a.full",
        "doc": "A name (attribute) and a namespace (attribute). The fullname is 'a.full.Name', and the namespace is 'a.full'.",
        "size": 12
      }
    },
    {
      "name": "explicitNamespace",
      "type": {
        "type": "record",
        "name": "Simple",
        "namespace": "explicit",
        "doc": "A simple name (attribute) and a namespace (attribute); the fullname is 'explicit.Simple' (this is a different type than of the 'inheritNull' field).",
        "fields": [
          {
            "name": "inheritNamespace",
            "type": {
              "type": "enum",
              "name": "Understanding",
              "doc": "A simple name (attribute) and no namespace attribute: inherit the namespace of the enclosing type 'explicit.Simple'. The fullname is 'explicit.Understanding'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "duplicateNamespace",
            "type": {
              "type": "enum",
              "name": "Reduction",
              "doc": "A simple name (attribute) and namespace attribute: same namespace of the enclosing type 'explicit'. The fullname is 'explicit.Reduction'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "emptyNamespace",
            "type": {
              "type": "enum",
              "name": "NullNamespace",
              "namespace": "",
              "doc": "A simple name (attribute) and namespace attribute: namespace is explicitly null. The fullname is 'NullNamespace'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          },
          {
            "name": "emptyFullname",
            "type": {
              "type": "enum",
              "name": "NullFullname",
              "namespace": "",
              "doc": "A name (attribute) and no namespace attribute: namespace is null. The fullname is 'NullFullname'.",
              "symbols": [
                "d",
                "e"
              ]
            }
          }
        ]
      }
    }
  ]
})"),
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_minify) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(not_minimal.share()).value(),
      not_minimal_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot.share()).value(),
      leading_dot_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name_ns) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot_ns.share()).value(),
      leading_dot_ns_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_record_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(record_not_sorted.share()).value(),
      record_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_enum_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(enum_not_sorted.share()).value(),
      enum_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_array_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(array_not_sorted.share()).value(),
      array_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_map_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(map_not_sorted.share()).value(),
      map_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_fixed_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(fixed_not_sorted.share()).value(),
      fixed_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_record_of_obj_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(record_of_obj_unsanitized.share())
        .value(),
      record_of_obj_sanitized);
}

BOOST_AUTO_TEST_CASE(test_namespace_nested_same) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(
        namespace_nested_same_unsanitized.share())
        .value(),
      namespace_nested_same_sanitized);
}

pps::schema_definition debezium_schema{
  R"({"type":"record","name":"SchemaChangeKey","namespace":"io.debezium.connector.mysql","fields":[{"name":"databaseName","type":"string"}],"connect.name":"io.debezium.connector.mysql.SchemaChangeKey"})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_debzium) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(debezium_schema.share()).value(),
      debezium_schema);
}

// Schemas with qualified and unqualified named type references should
// normalize to the same form. Per the Avro spec (Names), an unqualified
// name is resolved relative to the enclosing namespace.

const pps::schema_definition qualified_items_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"items","type":{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}}},{"name":"more","type":{"type":"array","items":"com.example.Inner"}}]})",
  pps::schema_type::avro};

const pps::schema_definition unqualified_items_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"items","type":{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}}},{"name":"more","type":{"type":"array","items":"Inner"}}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_normalize_items_type_reference) {
    auto sanitized_a = pps::sanitize_avro_schema_definition(
                         qualified_items_ref.share())
                         .value();
    auto sanitized_b = pps::sanitize_avro_schema_definition(
                         unqualified_items_ref.share())
                         .value();
    BOOST_REQUIRE_EQUAL(sanitized_a, sanitized_b);
}

const pps::schema_definition qualified_union_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":["null","com.example.Inner"]}]})",
  pps::schema_type::avro};

const pps::schema_definition unqualified_union_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":["null","Inner"]}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_normalize_union_type_reference) {
    auto sanitized_a = pps::sanitize_avro_schema_definition(
                         qualified_union_ref.share())
                         .value();
    auto sanitized_b = pps::sanitize_avro_schema_definition(
                         unqualified_union_ref.share())
                         .value();
    BOOST_REQUIRE_EQUAL(sanitized_a, sanitized_b);
}

const pps::schema_definition qualified_field_type_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":"com.example.Inner"}]})",
  pps::schema_type::avro};

const pps::schema_definition unqualified_field_type_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":"Inner"}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_normalize_field_type_reference) {
    auto sanitized_a = pps::sanitize_avro_schema_definition(
                         qualified_field_type_ref.share())
                         .value();
    auto sanitized_b = pps::sanitize_avro_schema_definition(
                         unqualified_field_type_ref.share())
                         .value();
    BOOST_REQUIRE_EQUAL(sanitized_a, sanitized_b);
}

const pps::schema_definition qualified_map_values_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"lookup","type":{"type":"map","values":"com.example.Inner"}}]})",
  pps::schema_type::avro};

const pps::schema_definition unqualified_map_values_ref{
  R"({"type":"record","name":"Outer","namespace":"com.example","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"lookup","type":{"type":"map","values":"Inner"}}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_normalize_map_values_reference) {
    auto sanitized_a = pps::sanitize_avro_schema_definition(
                         qualified_map_values_ref.share())
                         .value();
    auto sanitized_b = pps::sanitize_avro_schema_definition(
                         unqualified_map_values_ref.share())
                         .value();
    BOOST_REQUIRE_EQUAL(sanitized_a, sanitized_b);
}

// When there is no enclosing namespace, unqualified names stay unqualified
// (nothing to strip) and fully-qualified names keep their dotted form since
// their namespace does not match the (empty) enclosing namespace.
const pps::schema_definition no_ns_unqualified_union_ref{
  R"({"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":["null","Inner"]}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_no_normalize_without_namespace) {
    auto sanitized = pps::sanitize_avro_schema_definition(
                       no_ns_unqualified_union_ref.share())
                       .value();
    pps::schema_definition expected{
      R"({"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"val","type":"string"}]}},{"name":"ref","type":["null","Inner"]}]})",
      pps::schema_type::avro};
    BOOST_REQUIRE_EQUAL(sanitized, expected);
}

const pps::schema_definition top_level_primitive_object_form{
  R"({"type":"string"})", pps::schema_type::avro};

const pps::schema_definition top_level_primitive_simple_form{
  R"("string")", pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_collapse_top_level_primitive_object) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(
        top_level_primitive_object_form.share())
        .value(),
      top_level_primitive_simple_form);
}

// CORE-16247 regression: nested `{"type":"<primitive>"}` collapses to bare
// form. Covers all 8 Avro primitives across record-field, array-items,
// map-values, and union-branch positions.
const pps::schema_definition primitive_object_forms{
  R"({"type":"record","name":"R","fields":[)"
  R"({"name":"f_null","type":{"type":"null"}},)"
  R"({"name":"f_bool","type":{"type":"boolean"}},)"
  R"({"name":"f_int","type":{"type":"int"}},)"
  R"({"name":"f_long","type":{"type":"long"}},)"
  R"({"name":"f_float","type":{"type":"float"}},)"
  R"({"name":"f_double","type":{"type":"double"}},)"
  R"({"name":"f_bytes","type":{"type":"bytes"}},)"
  R"({"name":"f_string","type":{"type":"string"}},)"
  R"({"name":"f_array","type":{"type":"array","items":{"type":"string"}}},)"
  R"({"name":"f_map","type":{"type":"map","values":{"type":"int"}}},)"
  R"({"name":"f_union","type":["null",{"type":"string"}]})"
  R"(]})",
  pps::schema_type::avro};

const pps::schema_definition primitive_object_forms_sanitized{
  R"({"type":"record","name":"R","fields":[)"
  R"({"name":"f_null","type":"null"},)"
  R"({"name":"f_bool","type":"boolean"},)"
  R"({"name":"f_int","type":"int"},)"
  R"({"name":"f_long","type":"long"},)"
  R"({"name":"f_float","type":"float"},)"
  R"({"name":"f_double","type":"double"},)"
  R"({"name":"f_bytes","type":"bytes"},)"
  R"({"name":"f_string","type":"string"},)"
  R"({"name":"f_array","type":{"type":"array","items":"string"}},)"
  R"({"name":"f_map","type":{"type":"map","values":"int"}},)"
  R"({"name":"f_union","type":["null","string"]})"
  R"(]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_collapse_primitive_object_form) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(primitive_object_forms.share())
        .value(),
      primitive_object_forms_sanitized);
}

const pps::schema_definition core_16246_verbose{
  R"({"type":"record","name":"TestRecord","namespace":"test","fields":[)"
  R"({"name":"tags","type":["null",{"type":"array","items":{"type":"string"}}],"default":null})"
  R"(]})",
  pps::schema_type::avro};

const pps::schema_definition core_16246_bare{
  R"({"type":"record","name":"TestRecord","namespace":"test","fields":[)"
  R"({"name":"tags","type":["null",{"type":"array","items":"string"}],"default":null})"
  R"(]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_verbose_and_bare_equivalent) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(core_16246_verbose.share()).value(),
      pps::sanitize_avro_schema_definition(core_16246_bare.share()).value());
}

// A primitive object carrying additional attributes is not equivalent to
// primitive simple form and must be preserved.
const pps::schema_definition logical_type_preserved{
  R"({"type":"record","name":"R","fields":[)"
  R"({"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}})"
  R"(]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_logical_type_preserved) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(logical_type_preserved.share())
        .value(),
      logical_type_preserved);
}

// A `{"type":"<non-primitive>"}` object (here a named-type reference) is
// not equivalent to primitive simple form and must be preserved.
const pps::schema_definition named_ref_object_form{
  R"({"type":"record","name":"Outer","fields":[)"
  R"({"name":"f","type":{"type":"Outer"}})"
  R"(]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_named_ref_object_not_collapsed) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(named_ref_object_form.share())
        .value(),
      named_ref_object_form);
}

// A `{"type":[...]}` (a union in the type position) is single-member but
// its type value is not a string, so the collapser must skip it.
const pps::schema_definition union_in_type_field{
  R"({"type":["null","string"]})", pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_union_in_type_not_collapsed) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(union_in_type_field.share()).value(),
      union_in_type_field);
}

// Custom metadata may contain JSON that incidentally has the same shape as
// primitive schemas. Only real schema values should be collapsed.
const pps::schema_definition primitive_like_metadata_preserved{
  R"({"type":"record","name":"R","fields":[)"
  R"({"name":"a","type":{"type":"array","items":"string",)"
  R"("x-meta":{"type":"int"}}},)"
  R"({"name":"b","type":{"type":"array","items":"string",)"
  R"("x-meta":[{"type":"int"}]}}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_primitive_like_metadata_preserved) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(
        primitive_like_metadata_preserved.share())
        .value(),
      primitive_like_metadata_preserved);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_primitive_collapse_idempotent) {
    auto once = pps::sanitize_avro_schema_definition(
                  primitive_object_forms.share())
                  .value();
    auto twice = pps::sanitize_avro_schema_definition(once.share()).value();
    BOOST_REQUIRE_EQUAL(once, twice);
}
