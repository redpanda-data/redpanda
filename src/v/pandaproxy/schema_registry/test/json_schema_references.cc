// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"
#include "pandaproxy/schema_registry/types.h"

#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

namespace {

schema_reference
make_ref(std::string_view name, std::string_view subject, int version) {
    return schema_reference{
      .name = ss::sstring{name.data(), name.size()},
      .sub = context_subject_reference::unqualified(subject),
      .version = schema_version(version)};
}

schema_definition
raw_schema(std::string_view body, schema_definition::references refs = {}) {
    return schema_definition{
      body, schema_type::json, std::move(refs), std::nullopt};
}

} // namespace

class JsonSchemaReferencesTest
  : public ::testing::Test
  , public test_utils::store_fixture {
public:
    json_schema_definition register_schema(
      const context_subject& sub,
      const schema_definition& schema_def,
      schema_version version) {
        auto json_def = make_json_schema_definition(
                          _store, {sub, schema_def.share()})
                          .get();

        store_fixture::insert(sub, schema_def, version);

        return json_def;
    }
};

// ---- Parameterized happy-path tests ----

namespace {

struct dep_schema {
    std::string_view subject;
    std::string_view body;
};

struct ref_entry {
    std::string_view name;
    std::string_view subject;
    int version;
};

struct happy_case {
    std::string_view name;
    std::vector<dep_schema> deps;
    std::string_view root_subject;
    std::string_view root_body;
    std::vector<ref_entry> root_refs;
};

} // namespace

class JsonSchemaHappyPathTest
  : public JsonSchemaReferencesTest
  , public ::testing::WithParamInterface<happy_case> {};

TEST_P(JsonSchemaHappyPathTest, RegistersWithoutError) {
    const auto& p = GetParam();

    for (const auto& dep : p.deps) {
        register_schema(
          context_subject::unqualified(dep.subject),
          raw_schema(dep.body),
          schema_version{1});
    }

    schema_definition::references refs;
    for (const auto& r : p.root_refs) {
        refs.push_back(make_ref(r.name, r.subject, r.version));
    }

    ASSERT_NO_THROW(register_schema(
      context_subject::unqualified(p.root_subject),
      raw_schema(p.root_body, std::move(refs)),
      schema_version{1}));
}

INSTANTIATE_TEST_SUITE_P(
  All,
  JsonSchemaHappyPathTest,
  ::testing::ValuesIn(
    std::vector<happy_case>{
      {.name = "basic",
       .deps = {{.subject = "PersonSubject", .body = R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "age": { "type": "integer" }
  }
})"}},
       .root_subject = "TeamSubject",
       .root_body = R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "lead": { "$ref": "person.json" }
  }
})",
       .root_refs
       = {{.name = "person.json", .subject = "PersonSubject", .version = 1}}},

      {.name = "json_pointer_fragment",
       .deps = {{.subject = "TypesSubject", .body = R"({
  "type": "object",
  "$defs": {
    "FullName": {
      "type": "object",
      "properties": {
        "first": { "type": "string" },
        "last": { "type": "string" }
      }
    }
  }
})"}},
       .root_subject = "PersonSubject",
       .root_body = R"({
  "type": "object",
  "properties": {
    "name": { "$ref": "types.json#/$defs/FullName" }
  }
})",
       .root_refs
       = {{.name = "types.json", .subject = "TypesSubject", .version = 1}}},

      {.name = "multiple_refs_one_of",
       .deps
       = {{.subject = "PersonSubject", .body = R"({"type": "object", "properties": {"name": {"type": "string"}}})"}, {.subject = "CompanySubject", .body = R"({"type": "object", "properties": {"company_name": {"type": "string"}}})"}},
       .root_subject = "ContactSubject",
       .root_body = R"({
  "oneOf": [
    { "$ref": "person.json" },
    { "$ref": "company.json" }
  ]
})",
       .root_refs
       = {{.name = "person.json", .subject = "PersonSubject", .version = 1}, {.name = "company.json", .subject = "CompanySubject", .version = 1}}},

      {.name = "dot_slash_prefix",
       .deps
       = {{.subject = "PersonSubject", .body = R"({"type": "object", "properties": {"name": {"type": "string"}}})"}},
       .root_subject = "TeamSubject",
       .root_body = R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "./person.json" }
  }
})",
       .root_refs
       = {{.name = "person.json", .subject = "PersonSubject", .version = 1}}},
    }),
  [](const auto& info) { return std::string{info.param.name}; });

// ---- Multi-step and error tests ----

TEST_F(JsonSchemaReferencesTest, TransitiveReferences) {
    register_schema(
      context_subject::unqualified("CountrySubject"),
      raw_schema(R"({
  "type": "object",
  "properties": {
    "code": { "type": "string" }
  }
})"),
      schema_version{1});

    register_schema(
      context_subject::unqualified("AddressSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "street": { "type": "string" },
    "country": { "$ref": "country.json" }
  }
})",
        {make_ref("country.json", "CountrySubject", 1)}),
      schema_version{1});

    ASSERT_NO_THROW(register_schema(
      context_subject::unqualified("PersonSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "address": { "$ref": "address.json" }
  }
})",
        {make_ref("address.json", "AddressSubject", 1)}),
      schema_version{1}));
}

TEST_F(JsonSchemaReferencesTest, DiamondDependencies) {
    // A → {B, C} → D
    register_schema(
      context_subject::unqualified("BaseSubject"),
      raw_schema(
        R"({"type": "object", "properties": {"id": {"type": "string"}}})"),
      schema_version{1});

    register_schema(
      context_subject::unqualified("LeftSubject"),
      raw_schema(
        R"({"type": "object", "properties": {"base": {"$ref": "base.json"}}})",
        {make_ref("base.json", "BaseSubject", 1)}),
      schema_version{1});

    register_schema(
      context_subject::unqualified("RightSubject"),
      raw_schema(
        R"({"type": "object", "properties": {"base": {"$ref": "base.json"}}})",
        {make_ref("base.json", "BaseSubject", 1)}),
      schema_version{1});

    ASSERT_NO_THROW(register_schema(
      context_subject::unqualified("TopSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "left": { "$ref": "left.json" },
    "right": { "$ref": "right.json" }
  }
})",
        {make_ref("left.json", "LeftSubject", 1),
         make_ref("right.json", "RightSubject", 1)}),
      schema_version{1}));
}

TEST_F(JsonSchemaReferencesTest, MissingReference) {
    auto team_def = raw_schema(
      R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
      {make_ref("person.json", "NonExistentSubject", 1)});

    ASSERT_THROW(
      make_json_schema_definition(
        _store, {context_subject::unqualified("TeamSubject"), team_def.share()})
        .get(),
      exception);
}

TEST_F(JsonSchemaReferencesTest, RefNameMustMatchExternalId) {
    // If the referenced schema declares a $id, the referrer's ref.name
    // must agree. A mismatch is semantically ambiguous — $ref resolution
    // would silently fail later in the translation pipeline — so reject
    // it cleanly at registration time.
    register_schema(
      context_subject::unqualified("PersonSubject"),
      raw_schema(R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://canonical.com/person.json",
  "type": "object",
  "properties": {
    "name": { "type": "string" }
  }
})"),
      schema_version{1});

    // ref.name "person.json" does not match the referenced schema's
    // declared $id "https://canonical.com/person.json".
    auto team_def = raw_schema(
      R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
      {make_ref("person.json", "PersonSubject", 1)});

    ASSERT_THROW(
      make_json_schema_definition(
        _store, {context_subject::unqualified("TeamSubject"), team_def.share()})
        .get(),
      exception);
}

TEST_F(JsonSchemaReferencesTest, ExternalSchemaWithOwnId) {
    // The external schema's $id must be preserved when resolving its
    // internal relative $refs.
    register_schema(
      context_subject::unqualified("PersonSubject"),
      raw_schema(R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://example.com/person.json",
  "type": "object",
  "properties": {
    "name": { "$ref": "#/definitions/FullName" }
  },
  "definitions": {
    "FullName": {
      "type": "object",
      "properties": {
        "first": { "type": "string" },
        "last": { "type": "string" }
      }
    }
  }
})"),
      schema_version{1});

    ASSERT_NO_THROW(register_schema(
      context_subject::unqualified("TeamSubject"),
      raw_schema(
        R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "lead": { "$ref": "https://example.com/person.json" }
  }
})",
        {make_ref("https://example.com/person.json", "PersonSubject", 1)}),
      schema_version{1}));
}

// ---- Compatibility tests ----

TEST_F(JsonSchemaReferencesTest, CompatibleRefChange) {
    auto person_sub = context_subject::unqualified("PersonSubject");
    register_schema(
      person_sub,
      raw_schema(R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" }
  }
})"),
      schema_version{1});

    auto team_v1 = register_schema(
      context_subject::unqualified("TeamSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
        {make_ref("person.json", "PersonSubject", 1)}),
      schema_version{1});

    // Evolve person: add optional "age" property
    register_schema(
      person_sub,
      raw_schema(R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "age": { "type": "integer" }
  }
})"),
      schema_version{2});

    auto team_v2 = register_schema(
      context_subject::unqualified("TeamSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
        {make_ref("person.json", "PersonSubject", 2)}),
      schema_version{2});

    // Backward-compatible: reader=v1 ignores the added optional "age".
    auto result = check_compatible(team_v1, team_v2, verbose::yes);
    ASSERT_TRUE(result.is_compat) << result.messages;
}

TEST_F(JsonSchemaReferencesTest, IncompatibleRefChange) {
    auto person_sub = context_subject::unqualified("PersonSubject");
    register_schema(
      person_sub,
      raw_schema(R"({
  "type": "object",
  "properties": {
    "name": { "type": "string" }
  }
})"),
      schema_version{1});

    auto team_v1 = register_schema(
      context_subject::unqualified("TeamSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
        {make_ref("person.json", "PersonSubject", 1)}),
      schema_version{1});

    // Evolve person: change "name" from string to integer (incompatible)
    register_schema(
      person_sub,
      raw_schema(R"({
  "type": "object",
  "properties": {
    "name": { "type": "integer" }
  }
})"),
      schema_version{2});

    auto team_v2 = register_schema(
      context_subject::unqualified("TeamSubject"),
      raw_schema(
        R"({
  "type": "object",
  "properties": {
    "lead": { "$ref": "person.json" }
  }
})",
        {make_ref("person.json", "PersonSubject", 2)}),
      schema_version{2});

    auto result = check_compatible(team_v1, team_v2, verbose::yes);
    ASSERT_FALSE(result.is_compat);
}

TEST_F(JsonSchemaReferencesTest, ExternalRefWithInternalRefs) {
    register_schema(
      context_subject::unqualified("TypesSubject"),
      raw_schema(R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "name": { "$ref": "#/definitions/Name" },
    "address": { "$ref": "#/definitions/Address" }
  },
  "definitions": {
    "Name": {
      "type": "object",
      "properties": {
        "first": { "type": "string" },
        "last": { "type": "string" }
      }
    },
    "Address": {
      "type": "object",
      "properties": {
        "street": { "type": "string" },
        "city": { "type": "string" }
      }
    }
  }
})"),
      schema_version{1});

    auto root_v1 = register_schema(
      context_subject::unqualified("RootSubject"),
      raw_schema(
        R"({
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "person": { "$ref": "types.json" }
  }
})",
        {make_ref("types.json", "TypesSubject", 1)}),
      schema_version{1});

    // Exercises cross-document $ref resolution during is_superset.
    auto result = check_compatible(root_v1, root_v1, verbose::yes);
    ASSERT_TRUE(result.is_compat) << result.messages;
}

} // namespace pandaproxy::schema_registry
