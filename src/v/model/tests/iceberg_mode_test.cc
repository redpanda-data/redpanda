// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "gmock/gmock.h"
#include "model/metadata.h"

#include <gtest/gtest.h>

#include <sstream>

namespace {

// Parse an iceberg_mode from a string using operator>>.
// Returns std::nullopt if parsing fails (failbit set).
std::optional<model::iceberg_mode> parse(std::string_view s) {
    model::iceberg_mode m;
    std::istringstream ss{std::string(s)};
    ss >> m;
    if (ss.fail()) {
        return std::nullopt;
    }
    return m;
}

// Serialize an iceberg_mode to its string representation.
std::string to_string(const model::iceberg_mode& m) {
    return fmt::format("{}", m);
}

// Wire round-trip: write_nested → read_nested.
model::iceberg_mode wire_roundtrip(const model::iceberg_mode& m) {
    iobuf buf;
    write_nested(buf, m);
    iobuf_parser parser{buf.share(0, buf.size_bytes())};
    model::iceberg_mode result;
    read_nested(parser, result, 0UL);
    return result;
}

using sm = model::iceberg_mode::schema_mode;
using hsm = model::iceberg_mode::header_schema_mode;
using enabled = model::iceberg_mode::enabled_impl;

} // namespace

// --- disabled ---

TEST(IcebergModeParse, Disabled) {
    auto m = parse("disabled");
    ASSERT_TRUE(m.has_value());
    EXPECT_TRUE(m->is_disabled());
    EXPECT_EQ(*m, model::iceberg_mode::disabled);
}

// --- legacy strings ---

TEST(IcebergModeParse, LegacyKeyValue) {
    auto m = parse("key_value");
    ASSERT_TRUE(m.has_value());
    ASSERT_FALSE(m->is_disabled());
    EXPECT_EQ(m->key().mode, sm::binary);
    EXPECT_EQ(m->value().mode, sm::binary);
    EXPECT_EQ(m->headers().value_type, hsm::binary);
}

TEST(IcebergModeParse, LegacyValueSchemaIdPrefix) {
    auto m = parse("value_schema_id_prefix");
    ASSERT_TRUE(m.has_value());
    ASSERT_FALSE(m->is_disabled());
    EXPECT_EQ(m->key().mode, sm::binary);
    EXPECT_EQ(m->value().mode, sm::schema_id_prefix);
}

TEST(IcebergModeParse, LegacyValueSchemaLatestBare) {
    auto m = parse("value_schema_latest");
    ASSERT_TRUE(m.has_value());
    ASSERT_FALSE(m->is_disabled());
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_TRUE(m->value().subject.empty());
    EXPECT_TRUE(m->value().protobuf_name.empty());
}

TEST(IcebergModeParse, LegacyValueSchemaLatestWithSubject) {
    auto m = parse("value_schema_latest:subject=my-topic-value");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().subject, "my-topic-value");
    EXPECT_TRUE(m->value().protobuf_name.empty());
}

TEST(IcebergModeParse, LegacyValueSchemaLatestWithProtobufName) {
    auto m = parse("value_schema_latest:protobuf_name=com.example.Msg");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().protobuf_name, "com.example.Msg");
    EXPECT_TRUE(m->value().subject.empty());
}

TEST(IcebergModeParse, LegacyValueSchemaLatestBoth) {
    auto m = parse(
      "value_schema_latest:subject=my-topic-value,protobuf_name=com.example."
      "Msg");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().subject, "my-topic-value");
    EXPECT_EQ(m->value().protobuf_name, "com.example.Msg");
}

// --- new section-based format ---

TEST(IcebergModeParse, NewValueSchemaIdPrefix) {
    auto m = parse("value:mode=schema_id_prefix");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->key().mode, sm::binary);
    EXPECT_EQ(m->value().mode, sm::schema_id_prefix);
    // Equivalent to the legacy string
    EXPECT_EQ(*m, model::iceberg_mode::value_schema_id_prefix);
}

TEST(IcebergModeParse, NewValueSchemaLatestWithSubject) {
    auto m = parse("value:mode=schema_latest,subject=my-topic-value");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().subject, "my-topic-value");
}

TEST(IcebergModeParse, NewKeyAndValue) {
    auto m = parse(
      "key:mode=schema_id_prefix;value:mode=schema_latest,subject=my-topic-"
      "value");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->key().mode, sm::schema_id_prefix);
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().subject, "my-topic-value");
}

TEST(IcebergModeParse, NewHeadersString) {
    auto m = parse("headers:value_type=string");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->key().mode, sm::binary);
    EXPECT_EQ(m->value().mode, sm::binary);
    EXPECT_EQ(m->headers().value_type, hsm::string);
}

TEST(IcebergModeParse, NewFullCombination) {
    auto m = parse(
      "key:mode=schema_id_prefix;value:mode=schema_latest,subject=my-topic-"
      "value;headers:value_type=string");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->key().mode, sm::schema_id_prefix);
    EXPECT_EQ(m->value().mode, sm::schema_latest);
    EXPECT_EQ(m->value().subject, "my-topic-value");
    EXPECT_EQ(m->headers().value_type, hsm::string);
}

TEST(IcebergModeParse, NewSectionOrderIrrelevant) {
    // headers before key before value
    auto m = parse(
      "headers:value_type=string;key:mode=schema_id_prefix;value:mode=schema_"
      "id_prefix");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->key().mode, sm::schema_id_prefix);
    EXPECT_EQ(m->value().mode, sm::schema_id_prefix);
    EXPECT_EQ(m->headers().value_type, hsm::string);
}

// --- backward compat: new-format strings equivalent to old-format ---

TEST(IcebergModeParse, NewFormatEquivalentToOld) {
    // New format "value:mode=schema_id_prefix" must equal old-format
    // "value_schema_id_prefix" even though they were written differently.
    auto old_m = parse("value_schema_id_prefix");
    auto new_m = parse("value:mode=schema_id_prefix");
    ASSERT_TRUE(old_m.has_value());
    ASSERT_TRUE(new_m.has_value());
    EXPECT_EQ(*old_m, *new_m);
}

// --- parse errors ---

TEST(IcebergModeParseError, UnknownTopLevelGarbage) {
    EXPECT_FALSE(parse("totally_invalid").has_value());
}

TEST(IcebergModeParseError, AllUnknownSections) {
    // All-unknown sections must not silently produce a default enabled config.
    EXPECT_FALSE(parse("hederas:value_type=string").has_value());
    EXPECT_FALSE(parse("future_section:opt=val").has_value());
}

TEST(IcebergModeParseError, UnknownSection) {
    EXPECT_FALSE(parse("value:mode=schema_id_prefix;future_section:new_opt=foo")
                   .has_value());
}

TEST(IcebergModeParseError, UnknownOptionKey) {
    EXPECT_FALSE(
      parse("value:mode=schema_id_prefix,future_opt=bar").has_value());
    EXPECT_FALSE(parse("headers:value_type=string,future_opt=bar").has_value());
}

TEST(IcebergModeParseError, DuplicateSection) {
    EXPECT_FALSE(
      parse("value:mode=binary;value:mode=schema_id_prefix").has_value());
}

TEST(IcebergModeParseError, DuplicateOption) {
    EXPECT_FALSE(parse("value:mode=binary,mode=schema_id_prefix").has_value());
}

TEST(IcebergModeParseError, BadModeValue) {
    EXPECT_FALSE(parse("value:mode=banana").has_value());
}

TEST(IcebergModeParseError, BadHeaderValueType) {
    EXPECT_FALSE(parse("headers:value_type=blob").has_value());
}

TEST(IcebergModeParseError, EmptyOptValue) {
    EXPECT_FALSE(parse("value:mode=").has_value());
}

TEST(IcebergModeParseError, LegacyUnknownOpt) {
    // In the legacy format, unknown option keys are a hard error.
    EXPECT_FALSE(parse("value_schema_latest:unknown_opt=foo").has_value());
}

TEST(IcebergModeParseError, SubjectWithBinaryMode) {
    // subject is only valid for schema_latest.
    EXPECT_FALSE(parse("value:mode=binary,subject=foo").has_value());
    EXPECT_FALSE(parse("key:mode=binary,subject=foo").has_value());
}

TEST(IcebergModeParseError, SubjectWithSchemaIdPrefixMode) {
    // subject is only valid for schema_latest.
    EXPECT_FALSE(parse("value:mode=schema_id_prefix,subject=foo").has_value());
}

TEST(IcebergModeParseError, ProtobufNameWithBinaryMode) {
    EXPECT_FALSE(parse("value:mode=binary,protobuf_name=foo.Bar").has_value());
}

// --- string serialization (format_to / operator<<) ---

TEST(IcebergModeFormat, Disabled) {
    EXPECT_EQ(to_string(model::iceberg_mode::disabled), "disabled");
}

TEST(IcebergModeFormat, KeyValue) {
    EXPECT_EQ(to_string(model::iceberg_mode::key_value), "key_value");
}

TEST(IcebergModeFormat, ValueSchemaIdPrefix) {
    EXPECT_EQ(
      to_string(model::iceberg_mode::value_schema_id_prefix),
      "value_schema_id_prefix");
}

TEST(IcebergModeFormat, ValueSchemaLatestBare) {
    auto m = model::iceberg_mode::value_schema_latest("", "");
    EXPECT_EQ(to_string(m), "value_schema_latest");
}

TEST(IcebergModeFormat, ValueSchemaLatestWithSubject) {
    auto m = model::iceberg_mode::value_schema_latest("", "my-topic-value");
    EXPECT_EQ(to_string(m), "value_schema_latest:subject=my-topic-value");
}

TEST(IcebergModeFormat, ValueSchemaLatestWithProtobuf) {
    auto m = model::iceberg_mode::value_schema_latest(
      "com.example.Msg", "my-topic-value");
    EXPECT_EQ(
      to_string(m),
      "value_schema_latest:protobuf_name=com.example.Msg,subject=my-topic-"
      "value");
}

TEST(IcebergModeFormat, NewKeySchema) {
    // key:mode=schema_id_prefix should NOT be expressible as legacy format
    enabled e{};
    e.key.mode = sm::schema_id_prefix;
    model::iceberg_mode m{std::move(e)};
    EXPECT_EQ(to_string(m), "key:mode=schema_id_prefix");
}

TEST(IcebergModeFormat, NewHeadersString) {
    enabled e{};
    e.headers.value_type = hsm::string;
    model::iceberg_mode m{std::move(e)};
    EXPECT_EQ(to_string(m), "headers:value_type=string");
}

// New-format string that is equivalent to an old-format string serializes
// as the old-format string (not the new-format section string).
TEST(IcebergModeFormat, NewFormatEquivalentSerializesAsOld) {
    auto m = parse("value:mode=schema_id_prefix");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(to_string(*m), "value_schema_id_prefix");
}

// --- string round-trip ---

// parse → format → parse gives the same object for stable representations.
// Note: parse → format is not necessarily identity (new-format equivalent to
// old-format normalizes to old-format string).
void check_stable(const model::iceberg_mode& m) {
    auto s1 = to_string(m);
    auto m2 = parse(s1);
    ASSERT_TRUE(m2.has_value());
    EXPECT_EQ(m, *m2);
    // Second round-trip produces the same string.
    EXPECT_EQ(to_string(*m2), s1);
}

TEST(IcebergModeStringRoundtrip, Disabled) {
    check_stable(model::iceberg_mode::disabled);
}

TEST(IcebergModeStringRoundtrip, KeyValue) {
    check_stable(model::iceberg_mode::key_value);
}

TEST(IcebergModeStringRoundtrip, ValueSchemaIdPrefix) {
    check_stable(model::iceberg_mode::value_schema_id_prefix);
}

TEST(IcebergModeStringRoundtrip, ValueSchemaLatest) {
    check_stable(
      model::iceberg_mode::value_schema_latest(
        "com.example.Msg", "my-topic-value"));
}

TEST(IcebergModeStringRoundtrip, KeySchema) {
    enabled e{};
    e.key.mode = sm::schema_id_prefix;
    check_stable(model::iceberg_mode{std::move(e)});
}

TEST(IcebergModeStringRoundtrip, HeadersString) {
    enabled e{};
    e.headers.value_type = hsm::string;
    check_stable(model::iceberg_mode{std::move(e)});
}

TEST(IcebergModeStringRoundtrip, FullCombination) {
    enabled e{};
    e.key.mode = sm::schema_id_prefix;
    e.value.mode = sm::schema_latest;
    e.value.subject = "my-topic-value";
    e.headers.value_type = hsm::string;
    check_stable(model::iceberg_mode{std::move(e)});
}

// --- wire format round-trip ---

TEST(IcebergModeWireRoundtrip, Disabled) {
    EXPECT_EQ(
      wire_roundtrip(model::iceberg_mode::disabled),
      model::iceberg_mode::disabled);
}

TEST(IcebergModeWireRoundtrip, KeyValue) {
    EXPECT_EQ(
      wire_roundtrip(model::iceberg_mode::key_value),
      model::iceberg_mode::key_value);
}

TEST(IcebergModeWireRoundtrip, ValueSchemaIdPrefix) {
    EXPECT_EQ(
      wire_roundtrip(model::iceberg_mode::value_schema_id_prefix),
      model::iceberg_mode::value_schema_id_prefix);
}

TEST(IcebergModeWireRoundtrip, ValueSchemaLatest) {
    auto m = model::iceberg_mode::value_schema_latest(
      "com.example.Msg", "my-topic-value");
    EXPECT_EQ(wire_roundtrip(m), m);
}

TEST(IcebergModeWireRoundtrip, KeySchemaUsesDiscriminant4) {
    // key.mode != binary → must use discriminant 4
    enabled e{};
    e.key.mode = sm::schema_id_prefix;
    model::iceberg_mode m{std::move(e)};
    EXPECT_EQ(wire_roundtrip(m), m);
}

TEST(IcebergModeWireRoundtrip, HeadersStringUsesDiscriminant4) {
    // headers.value_type != binary → must use discriminant 4
    enabled e{};
    e.headers.value_type = hsm::string;
    model::iceberg_mode m{std::move(e)};
    EXPECT_EQ(wire_roundtrip(m), m);
}

TEST(IcebergModeWireRoundtrip, FullCombination) {
    enabled e{};
    e.key.mode = sm::schema_latest;
    e.key.subject = "my-topic-key";
    e.value.mode = sm::schema_latest;
    e.value.subject = "my-topic-value";
    e.value.protobuf_name = "com.example.Msg";
    e.headers.value_type = hsm::string;
    model::iceberg_mode m{std::move(e)};
    EXPECT_EQ(wire_roundtrip(m), m);
}

// Old discriminants (0-3) must still be readable by new code.
// Verified implicitly: key_value, value_schema_id_prefix, value_schema_latest
// all use old discriminants 1, 2, 3. Confirmed above.

// --- parse error messages ---

// Helper: call parse_iceberg_mode and return the error string.
// Asserts that parsing fails.
std::string parse_err(std::string_view s) {
    auto r = model::parse_iceberg_mode(s);
    EXPECT_FALSE(r.has_value()) << "expected parse failure for: " << s;
    return std::string(r.error());
}

TEST(IcebergModeParseErrorMsg, UnknownSection) {
    auto e = parse_err("hederas:value_type=string");
    EXPECT_THAT(e, testing::HasSubstr("hederas"));
}

TEST(IcebergModeParseErrorMsg, UnknownOptionInValueSection) {
    auto e = parse_err("value:mode=schema_id_prefix,future_opt=bar");
    EXPECT_THAT(e, testing::HasSubstr("future_opt"));
    EXPECT_THAT(e, testing::HasSubstr("value"));
}

TEST(IcebergModeParseErrorMsg, UnknownOptionInHeadersSection) {
    auto e = parse_err("headers:value_type=string,future_opt=bar");
    EXPECT_THAT(e, testing::HasSubstr("future_opt"));
    EXPECT_THAT(e, testing::HasSubstr("headers"));
}

TEST(IcebergModeParseErrorMsg, UnknownModeValue) {
    auto e = parse_err("value:mode=banana");
    EXPECT_THAT(e, testing::HasSubstr("banana"));
    EXPECT_THAT(e, testing::HasSubstr("value"));
}

TEST(IcebergModeParseErrorMsg, UnknownHeaderValueType) {
    auto e = parse_err("headers:value_type=blob");
    EXPECT_THAT(e, testing::HasSubstr("blob"));
}

TEST(IcebergModeParseErrorMsg, DuplicateSection) {
    auto e = parse_err("value:mode=binary;value:mode=schema_id_prefix");
    EXPECT_THAT(e, testing::HasSubstr("value"));
}

TEST(IcebergModeParseErrorMsg, SubjectWithBinaryMode) {
    auto e = parse_err("value:mode=binary,subject=foo");
    EXPECT_EQ(
      e,
      "subject and protobuf_name require mode=schema_latest in section "
      "'value'");
}

TEST(IcebergModeParseErrorMsg, LegacyUnknownOpt) {
    auto e = parse_err("value_schema_latest:unknown_opt=foo");
    EXPECT_THAT(e, testing::HasSubstr("unknown_opt"));
}

// Degenerate section configs that contain no meaningful options all reduce to
// the same all-defaults enabled_impl, which serializes as "key_value".
TEST(IcebergModeNormalize, EmptyKeySection) {
    auto m = parse("key:");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(to_string(*m), "key_value");
}

TEST(IcebergModeNormalize, HeadersBinaryExplicit) {
    auto m = parse("headers:value_type=binary");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(to_string(*m), "key_value");
}

TEST(IcebergModeNormalize, AllSectionsAllDefaults) {
    auto m = parse("key:;value:;headers:");
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(to_string(*m), "key_value");
}
