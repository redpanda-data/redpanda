// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"
#include "pandaproxy/schema_registry/types.h"

#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

class AvroSchemaReferencesTest
  : public ::testing::Test
  , public test_utils::store_fixture {
public:
    void SetUp() override {
        // Enable the named references feature for these tests
        config::shard_local_cfg()
          .schema_registry_avro_use_named_references.set_value(true);
    }

    void TearDown() override {
        // Reset to default
        config::shard_local_cfg()
          .schema_registry_avro_use_named_references.reset();
    }

    // Register a schema in the store and return the avro_schema_definition
    avro_schema_definition register_schema(
      const subject& sub,
      const schema_definition& schema_def,
      schema_version version) {
        auto avro_def = make_avro_schema_definition(
                          _store, {sub, schema_def.share()})
                          .get();

        // Use base class method to register
        store_fixture::insert(sub, schema_def, version);

        return avro_def;
    }
};

TEST_F(AvroSchemaReferencesTest, RegisterWithReferences) {
    // Regression test: verify that schemas with external references are
    // correctly resolved from the schema store

    const auto ref_sub = subject("AddressSubject");
    const auto ref_name = "com.example.types.Address";
    const auto main_sub = subject("PersonSubject");
    const auto main_name = "com.example.types.Person";

    auto referenced_schema = schema_definition{
      R"({
   "type": "record",
   "name": "Address",
   "namespace": "com.example.types",
   "fields": [
      {
         "name": "city",
         "type": ["null", "string"],
         "default": null
      }
   ]
})",
      schema_type::avro};

    auto referencing_schema = schema_definition{
      R"({
   "type": "record",
   "name": "Person",
   "namespace": "com.example.types",
   "fields": [
      {
         "name": "name",
         "type": "string"
      },
      {
         "name": "address",
         "type": ["null", "com.example.types.Address"],
         "default": null
      }
   ]
})",
      schema_type::avro,
      {schema_reference{
        .name = ref_name, .sub = ref_sub, .version = schema_version(1)}}};

    // Register the referenced schema (should not throw)
    auto ref_valid = register_schema(
      ref_sub, referenced_schema, schema_version{1});

    // Create schema with reference (should not throw)
    auto main_valid = register_schema(
      main_sub, referencing_schema, schema_version{1});

    // Sanity check the resulting schemas
    ASSERT_EQ(ref_valid.name(), ref_name);
    ASSERT_EQ(main_valid.name(), main_name);
    ASSERT_NE(main_valid.raw(), ref_valid.raw());
}

TEST_F(AvroSchemaReferencesTest, DiamondDependencies) {
    // Diamond: Top → {Left, Right} → Base

    // Shared base
    register_schema(
      subject("BaseSubject"),
      schema_definition{
        R"({"type": "record", "name": "Base", "namespace": "com.example",
            "fields": [{"name": "id", "type": "string"}]})",
        schema_type::avro},
      schema_version{1});

    // Left references Base
    register_schema(
      subject("LeftSubject"),
      schema_definition{
        R"({"type": "record", "name": "Left", "namespace": "com.example",
            "fields": [{"name": "base", "type": "com.example.Base"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Base",
          .sub = subject("BaseSubject"),
          .version = schema_version(1)}}},
      schema_version{1});

    // Right also references Base
    register_schema(
      subject("RightSubject"),
      schema_definition{
        R"({"type": "record", "name": "Right", "namespace": "com.example",
            "fields": [{"name": "base", "type": "com.example.Base"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Base",
          .sub = subject("BaseSubject"),
          .version = schema_version(1)}}},
      schema_version{1});

    // Top references both
    auto top_valid = register_schema(
      subject("TopSubject"),
      schema_definition{
        R"({"type": "record", "name": "Top", "namespace": "com.example",
            "fields": [
              {"name": "left", "type": "com.example.Left"},
              {"name": "right", "type": "com.example.Right"}
            ]})",
        schema_type::avro,
        {schema_reference{
           .name = "com.example.Left",
           .sub = subject("LeftSubject"),
           .version = schema_version(1)},
         schema_reference{
           .name = "com.example.Right",
           .sub = subject("RightSubject"),
           .version = schema_version(1)}}},
      schema_version{1});

    ASSERT_EQ(top_valid.name(), "com.example.Top");
}

TEST_F(AvroSchemaReferencesTest, TransitiveReferences) {
    // Test that we recursively fetch transitive references (Person → Address →
    // Country)

    // Leaf: Country (no dependencies)
    register_schema(
      subject("CountrySubject"),
      schema_definition{
        R"({"type": "record", "name": "Country", "namespace": "com.example",
            "fields": [{"name": "code", "type": "string"}]})",
        schema_type::avro},
      schema_version{1});

    // Middle: Address references Country
    register_schema(
      subject("AddressSubject"),
      schema_definition{
        R"({"type": "record", "name": "Address", "namespace": "com.example",
            "fields": [{"name": "country", "type": "com.example.Country"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Country",
          .sub = subject("CountrySubject"),
          .version = schema_version(1)}}},
      schema_version{1});

    // Root: Person references Address (should transitively fetch Country)
    auto person_valid = register_schema(
      subject("PersonSubject"),
      schema_definition{
        R"({"type": "record", "name": "Person", "namespace": "com.example",
            "fields": [{"name": "address", "type": "com.example.Address"}, {"name": "country", "type": "com.example.Country"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Address",
          .sub = subject("AddressSubject"),
          .version = schema_version(1)}}},
      schema_version{1});

    ASSERT_EQ(person_valid.name(), "com.example.Person");
}

TEST_F(AvroSchemaReferencesTest, PrimitiveTypeReference) {
    // Test that we allow referencing primitive types (naming them)

    register_schema(
      subject("StringSubject"),
      schema_definition{R"("string")", schema_type::avro},
      schema_version{1});

    // Should work - we're giving a name to a primitive type
    auto person_valid = register_schema(
      subject("PersonSubject"),
      schema_definition{
        R"({"type": "record", "name": "Person", "namespace": "com.example",
            "fields": [{"name": "customString", "type": "com.example.CustomString"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.CustomString",
          .sub = subject("StringSubject"),
          .version = schema_version(1)}}},
      schema_version{1});

    ASSERT_EQ(person_valid.name(), "com.example.Person");
}

namespace {

std::string remove_whitespace(std::string_view str) {
    std::string result;
    result.reserve(str.size());
    for (char c : str) {
        if (!std::isspace(static_cast<unsigned char>(c))) {
            result += c;
        }
    }
    return result;
}

} // namespace

TEST_F(AvroSchemaReferencesTest, SameNameDifferentSubjectsInIsolation) {
    // Regression test: Verify that when two different subjects define schemas
    // with the same Avro name but different content, they can coexist. This is
    // allowed by the reference implementation, and for now we follow that
    // behaviour and log a warning.
    //
    // Structure:
    //   Schema A refs [B, C]
    //   Schema B refs [D with name "com.example.Shared"]
    //   Schema C refs [E with name "com.example.Shared"]
    //   SubjectD and SubjectE have DIFFERENT schemas but same name

    // Register SubjectD - schema named "Shared" with field_d
    register_schema(
      subject("SubjectD"),
      schema_definition{
        R"({"type": "record", "name": "Shared", "namespace": "com.example",
            "fields": [{"name": "field_d", "type": "string"}]})",
        schema_type::avro},
      schema_version{1});

    // Register SubjectE - DIFFERENT schema but also named "Shared"
    register_schema(
      subject("SubjectE"),
      schema_definition{
        R"({"type": "record", "name": "Shared", "namespace": "com.example",
            "fields": [{"name": "field_e", "type": "int"}]})",
        schema_type::avro},
      schema_version{1});

    // Register B - uses Shared from SubjectD
    auto b_schema = register_schema(
      subject("SubjectB"),
      schema_definition{
        R"({"type": "record", "name": "B", "namespace": "com.example",
            "fields": [{"name": "shared", "type": "com.example.Shared"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Shared",
          .sub = subject("SubjectD"),
          .version = schema_version(1)}}},
      schema_version{1});

    // Register C - uses Shared from SubjectE
    auto c_schema = register_schema(
      subject("SubjectC"),
      schema_definition{
        R"({"type": "record", "name": "C", "namespace": "com.example",
            "fields": [{"name": "shared", "type": "com.example.Shared"}]})",
        schema_type::avro,
        {schema_reference{
          .name = "com.example.Shared",
          .sub = subject("SubjectE"),
          .version = schema_version(1)}}},
      schema_version{1});

    // Register A which depends on both B and C
    // This should succeed, with a logged warning about the name conflict
    auto a_schema = register_schema(
      subject("SubjectA"),
      schema_definition{
        R"({"type": "record", "name": "A", "namespace": "com.example",
            "fields": [
              {"name": "b", "type": "com.example.B"},
              {"name": "c", "type": "com.example.C"}
            ]})",
        schema_type::avro,
        {schema_reference{
           .name = "com.example.B",
           .sub = subject("SubjectB"),
           .version = schema_version(1)},
         schema_reference{
           .name = "com.example.C",
           .sub = subject("SubjectC"),
           .version = schema_version(1)}}},
      schema_version{1});

    // Show the structure of the resulting schema A, which has inlined the
    // references, with C now using D instead of E as its Shared definition
    const std::string expected_schema = remove_whitespace(R"({
  "type": "record",
  "namespace": "com.example",
  "name": "A",
  "fields": [
    {
      "name": "b",
      "type": {
        "type": "record",
        "namespace": "com.example",
        "name": "B",
        "fields": [
          {
            "name": "shared",
            "type": {
              "type": "record",
              "namespace": "com.example",
              "name": "Shared",
              "fields": [
                {"name": "field_d", "type": "string"}
              ]
            }
          }
        ]
      }
    },
    {
      "name": "c",
      "type": {
        "type": "record",
        "namespace": "com.example",
        "name": "C",
        "fields": [
          {"name": "shared", "type": "com.example.Shared"}
        ]
      }
    }
  ]
})");
    ASSERT_EQ(
      std::string_view{a_schema.raw()().linearize_to_string()},
      std::string_view{expected_schema});
}

// =============================================================================
// Legacy Behavior Tests (config disabled - default)
// =============================================================================
// These tests document the legacy behavior when
// schema_registry_avro_use_named_references is disabled (the default).

class AvroSchemaReferencesLegacyTest : public AvroSchemaReferencesTest {
public:
    void SetUp() override {
        // Explicitly ensure the legacy behavior is active (default is false)
        config::shard_local_cfg()
          .schema_registry_avro_use_named_references.reset();
    }
};

TEST_F(AvroSchemaReferencesLegacyTest, SchemaNameFromFirstReference) {
    // Documents the legacy behavior: when a schema has references, the compiled
    // schema's name comes from the first schema in the concatenation order (the
    // referenced schema), not from the main schema being registered.

    auto referenced_schema = schema_definition{
      R"({"type": "record", "name": "Address", "namespace": "com.example",
          "fields": [{"name": "city", "type": "string"}]})",
      schema_type::avro};

    auto referencing_schema = schema_definition{
      R"({"type": "record", "name": "Person", "namespace": "com.example",
          "fields": [{"name": "address", "type": "com.example.Address"}]})",
      schema_type::avro,
      {schema_reference{
        .name = "com.example.Address",
        .sub = subject("AddressSubject"),
        .version = schema_version(1)}}};

    register_schema(
      subject("AddressSubject"), referenced_schema, schema_version{1});

    // Legacy behavior: The compiled schema takes its name from the first
    // schema in the concatenation (Address), not from Person.
    auto compiled = register_schema(
      subject("PersonSubject"), referencing_schema, schema_version{1});
    EXPECT_EQ(compiled.name(), "com.example.Address");
    // Note: With schema_registry_avro_use_named_references=true, this would
    // correctly return "com.example.Person" (see AvroSchemaReferencesTest).
}

} // namespace pandaproxy::schema_registry
