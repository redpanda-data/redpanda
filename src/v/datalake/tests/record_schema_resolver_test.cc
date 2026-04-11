/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "absl/container/flat_hash_map.h"
#include "base/vassert.h"
#include "bytes/bytes.h"
#include "config/property.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "gmock/gmock.h"
#include "iceberg/datatypes.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <exception>
#include <variant>

using namespace pandaproxy::schema_registry;
using namespace datalake;
using namespace iceberg;

namespace {
constexpr std::string_view avro_record_schema = R"({
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "value", "type": "long"},
    {"type": "int", "name": "next", "metadata" : "two"}
  ]
})";
constexpr std::string_view pb_record_schema = R"(
syntax = "proto2";
package datalake.proto;
message empty_message {}
message simple_message {
  optional string label = 1;
  optional int32 number = 3;
  optional int64 big_number = 4;
  optional float float_number = 5;
  optional double double_number = 6;
  optional bool true_or_false = 7;
}
message nested_message {
  message inner_message_t1 {
    optional string inner_label_1 = 1;
    optional int32 inner_number_1 = 2;
  }
  message inner_message_t2 {
    optional string inner_label_2 = 1;
    optional int32 inner_number_2 = 2;
  }
  optional string label = 1;
  optional int32 number = 2;
  optional inner_message_t1 inner = 3;
}
)";
constexpr std::string_view json_record_schema = R"(
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "json_value": {"type": "integer"},
    "json_next": {"type": "integer"}
  }
}
)";
iobuf generate_dummy_body() { return iobuf::from("blob"); }
iobuf encode_pb_offsets(const std::vector<int32_t>& offsets) {
    auto cnt_bytes = vint::to_bytes(offsets.size());
    iobuf buf;
    buf.append(cnt_bytes.data(), cnt_bytes.size());
    for (auto o : offsets) {
        auto bytes = vint::to_bytes(o);
        buf.append(bytes.data(), bytes.size());
    }
    return buf;
}
} // namespace

class RecordSchemaResolverTest : public ::testing::Test {
public:
    RecordSchemaResolverTest()
      : sr(std::make_unique<schema::fake_registry>()) {}

    void SetUp() override {
        auto avro_schema_id = sr->create_schema(
                                  subject_schema{
                                    context_subject::unqualified("foo-value"),
                                    schema_definition{
                                      avro_record_schema, schema_type::avro}})
                                .get();
        ASSERT_EQ(1, avro_schema_id.id());
        auto pb_schema_id = sr->create_schema(
                                subject_schema{
                                  context_subject::unqualified("foo-value"),
                                  schema_definition{
                                    pb_record_schema, schema_type::protobuf}})
                              .get();
        ASSERT_EQ(2, pb_schema_id.id());
        auto json_schema_id = sr->create_schema(
                                  subject_schema{
                                    context_subject::unqualified("foo-value"),
                                    schema_definition{
                                      json_record_schema, schema_type::json}})
                                .get();
        ASSERT_EQ(3, json_schema_id.id());
        avro_schema_id = sr->create_schema(
                             subject_schema{
                               context_subject::unqualified("latest-avro"),
                               schema_definition{
                                 avro_record_schema, schema_type::avro}})
                           .get();
        ASSERT_EQ(1, avro_schema_id.id());
        pb_schema_id = sr->create_schema(
                           subject_schema{
                             context_subject::unqualified("latest-proto"),
                             schema_definition{
                               pb_record_schema, schema_type::protobuf}})
                         .get();
        ASSERT_EQ(2, pb_schema_id.id());
        json_schema_id = sr->create_schema(
                             subject_schema{
                               context_subject::unqualified("latest-json"),
                               schema_definition{
                                 json_record_schema, schema_type::json}})
                           .get();
        ASSERT_EQ(3, json_schema_id.id());
    }
    std::unique_ptr<schema::fake_registry> sr;
};

TEST_F(RecordSchemaResolverTest, TestAvroSchemaHappyPath) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(1, (*resolved_buf.type)->id.schema_id());
    EXPECT_FALSE((*resolved_buf.type)->id.protobuf_offsets.has_value());

    // Check that the resolved schema looks correct. Note, the field IDs are
    // unimportant since they are assigned outside of the resolver -- it's just
    // important the data's structure looks good.
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(0, "value", field_required::yes, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(0, "next", field_required::yes, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPath) {
    // Kakfa magic byte + schema ID + pb offsets.
    std::vector<int32_t> pb_offsets{2, 0};
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
    EXPECT_TRUE((*resolved_buf.type)->id.protobuf_offsets.has_value());
    EXPECT_EQ((*resolved_buf.type)->id.protobuf_offsets.value(), pb_offsets);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(
            1, "inner_label_1", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            2, "inner_number_1", field_required::no, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPathNested) {
    // Kakfa magic byte + schema ID + pb offsets.
    // Point at a nested field.
    std::vector<int32_t> pb_offsets{2};
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
    EXPECT_TRUE((*resolved_buf.type)->id.protobuf_offsets.has_value());
    EXPECT_EQ((*resolved_buf.type)->id.protobuf_offsets.value(), pb_offsets);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(1, "label", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(2, "number", field_required::no, int_type{}));

        struct_type inner_struct;
        inner_struct.fields.emplace_back(
          nested_field::create(
            1, "inner_label_1", field_required::no, string_type{}));
        inner_struct.fields.emplace_back(
          nested_field::create(
            2, "inner_number_1", field_required::no, int_type{}));

        expected_struct.fields.emplace_back(
          nested_field::create(
            3, "inner", field_required::no, std::move(inner_struct)));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaReferences) {
    constexpr std::string_view pb_simple_schema = R"(
syntax = "proto2";
message SimpleMessage {
  optional string label = 1;
  optional int32 number = 2;
  optional int64 big_number = 3;
}
)";
    constexpr std::string_view pb_references_schema = R"(
syntax = "proto2";
import "simple.proto";

message NestedMessage {
  optional string label = 1;
  optional SimpleMessage simple = 2;
}
)";
    auto pb_schema_id = sr->create_schema(
                            subject_schema{
                              context_subject::unqualified("simple_schema"),
                              schema_definition{
                                pb_simple_schema, schema_type::protobuf}})
                          .get();
    ASSERT_EQ(7, pb_schema_id.id());
    pb_schema_id = sr->create_schema(
                       subject_schema{
                         context_subject::unqualified("references_schema"),
                         schema_definition{
                           pb_references_schema,
                           schema_type::protobuf,
                           {schema_reference{
                             .name = "simple.proto",
                             .sub = context_subject_reference::unqualified(
                               "simple_schema"),
                             .version = schema_version{0}}},
                           std::optional<schema_metadata>{}}})
                     .get();
    ASSERT_EQ(8, pb_schema_id.id());
    std::vector<int32_t> pb_offsets{};
    iobuf buf;
    buf.append("\0\0\0\0\10", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(1, "label", field_required::no, string_type{}));
        auto simple_struct = struct_type{};
        simple_struct.fields.emplace_back(
          nested_field::create(1, "label", field_required::no, string_type{}));
        simple_struct.fields.emplace_back(
          nested_field::create(2, "number", field_required::no, int_type{}));
        simple_struct.fields.emplace_back(
          nested_field::create(
            3, "big_number", field_required::no, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            2, "simple", field_required::no, std::move(simple_struct)));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPathNoOffsets) {
    // Kakfa magic byte + schema ID + _empty_ pb offsets.
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets({}));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
    EXPECT_TRUE((*resolved_buf.type)->id.protobuf_offsets.has_value());
    EXPECT_EQ(
      (*resolved_buf.type)->id.protobuf_offsets.value(),
      std::vector<int32_t>{0});

    // When there are no protobuf offsets, we return the first descriptor,
    // which in this case translates to an empty struct.
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaBadOffsets) {
    // Kakfa magic byte + schema ID + bogus pb offsets.
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets({100}));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::bad_input);
}

TEST_F(RecordSchemaResolverTest, TestJsonSchemaHappyPath) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\3", 5);
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(3, (*resolved_buf.type)->id.schema_id());
    EXPECT_FALSE((*resolved_buf.type)->id.protobuf_offsets.has_value());

    // Check that the resolved schema looks correct. Note, the field IDs are
    // unimportant since they are assigned outside of the resolver -- it's just
    // important the data's structure looks good.
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(
            0, "json_next", field_required::no, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            0, "json_value", field_required::no, long_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestMissingMagic) {
    iobuf buf;
    // Write body but no magic.
    buf.append(generate_dummy_body());
    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::bad_input);
}

TEST_F(RecordSchemaResolverTest, TestSchemaRegistryError) {
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_dummy_body());
    sr->set_inject_failures(
      std::make_exception_ptr(std::runtime_error("injected")));

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::registry_error);

    // We can try again when there are no injected errors and there should be
    // no issue.
    sr->set_inject_failures(nullptr);
    res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(1, (*resolved_buf.type)->id.schema_id());
    EXPECT_FALSE((*resolved_buf.type)->id.protobuf_offsets.has_value());
}

TEST_F(RecordSchemaResolverTest, TestLatestSubjectSchema_Protobuf) {
    using namespace std::chrono_literals;
    iobuf buf;
    buf.append(generate_dummy_body());

    auto resolver = latest_subject_schema_resolver(
      *sr,
      subject("latest-proto"),
      std::nullopt,
      config::mock_binding(std::chrono::milliseconds(0s)),
      std::nullopt,
      std::nullopt);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
    EXPECT_THAT(
      (*resolved_buf.type)->id.protobuf_offsets,
      testing::Optional(testing::ElementsAre(0)));

    const auto expected_type = field_type{struct_type{}};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    EXPECT_THAT(resolved_buf.parsable_buf, testing::Optional(std::ref(buf)));
}

TEST_F(RecordSchemaResolverTest, TestLatestSubjectSchema_Protobuf_MessageName) {
    using namespace std::chrono_literals;
    iobuf buf;
    buf.append(generate_dummy_body());

    auto resolver = latest_subject_schema_resolver(
      *sr,
      subject("latest-proto"),
      "datalake.proto.nested_message.inner_message_t1",
      config::mock_binding(std::chrono::milliseconds(0s)),
      std::nullopt,
      std::nullopt);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
    EXPECT_THAT(
      (*resolved_buf.type)->id.protobuf_offsets,
      testing::Optional(testing::ElementsAre(2, 0)));

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(
            1, "inner_label_1", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            2, "inner_number_1", field_required::no, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    EXPECT_THAT(resolved_buf.parsable_buf, testing::Optional(std::ref(buf)));
}

TEST_F(RecordSchemaResolverTest, TestLatestSubjectSchema_Avro) {
    // NOTE: we strongly should discourage avro users from using this mode, it's
    // impossible to correctly evolve the schema this way without a stop the
    // world pause.
    using namespace std::chrono_literals;
    iobuf buf;
    buf.append(generate_dummy_body());

    auto resolver = latest_subject_schema_resolver(
      *sr,
      subject("latest-avro"),
      std::nullopt,
      config::mock_binding(std::chrono::milliseconds(0s)),
      std::nullopt,
      std::nullopt);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(1, (*resolved_buf.type)->id.schema_id());
    EXPECT_EQ((*resolved_buf.type)->id.protobuf_offsets, std::nullopt);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(0, "value", field_required::yes, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(0, "next", field_required::yes, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    EXPECT_THAT(resolved_buf.parsable_buf, testing::Optional(std::ref(buf)));
}

TEST_F(RecordSchemaResolverTest, TestLatestSubjectSchema_Json) {
    using namespace std::chrono_literals;
    iobuf buf;
    buf.append(generate_dummy_body());

    auto resolver = latest_subject_schema_resolver(
      *sr,
      subject("latest-json"),
      std::nullopt,
      config::mock_binding(std::chrono::milliseconds(0s)),
      std::nullopt,
      std::nullopt);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(3, (*resolved_buf.type)->id.schema_id());
    EXPECT_EQ((*resolved_buf.type)->id.protobuf_offsets, std::nullopt);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(
            0, "json_next", field_required::no, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            0, "json_value", field_required::no, long_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    EXPECT_THAT(resolved_buf.parsable_buf, testing::Optional(std::ref(buf)));
}

namespace {
struct counting_store : public pandaproxy::schema_registry::schema_getter {
    counting_store(
      schema::fake_registry& registry,
      absl::flat_hash_map<pandaproxy::schema_registry::schema_id, size_t>&
        counts)
      : registry(registry)
      , counts(counts) {}

    ss::future<pandaproxy::schema_registry::stored_schema> get_subject_schema(
      pandaproxy::schema_registry::context_subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version,
      pandaproxy::schema_registry::include_deleted inc_dec) final {
        auto* getter = co_await registry.getter();
        co_return co_await getter->get_subject_schema(sub, version, inc_dec);
    }

    ss::future<pandaproxy::schema_registry::schema_definition>
    get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) final {
        vassert(id.ctx == default_context, "unexpected context {}", id.ctx);
        counts[id.id] += 1;
        auto* getter = co_await registry.getter();
        co_return co_await getter->get_schema_definition(id);
    }

    ss::future<std::optional<pandaproxy::schema_registry::schema_definition>>
    maybe_get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) final {
        vassert(id.ctx == default_context, "unexpected context {}", id.ctx);
        counts[id.id] += 1;
        auto* getter = co_await registry.getter();
        co_return co_await getter->maybe_get_schema_definition(id);
    }

    schema::fake_registry& registry;
    absl::flat_hash_map<pandaproxy::schema_registry::schema_id, size_t>& counts;
};

class counting_registry : public schema::registry {
public:
    bool is_enabled() const override { return true; };

    ss::future<pandaproxy::schema_registry::schema_getter*>
    getter() const override {
        co_return &_store;
    }
    ss::future<pandaproxy::schema_registry::schema_getter*>
    synced_getter() const override {
        co_return &_store;
    }
    ss::future<ss::lowres_clock::time_point>
    sync(ss::lowres_clock::duration) override {
        co_return ss::lowres_clock::now();
    }
    ss::future<pandaproxy::schema_registry::schema_definition>
    get_schema_definition(
      pandaproxy::schema_registry::context_schema_id id) const override {
        return _store.get_schema_definition(id);
    }

    ss::future<pandaproxy::schema_registry::stored_schema> get_subject_schema(
      pandaproxy::schema_registry::context_subject sub,
      std::optional<pandaproxy::schema_registry::schema_version> version)
      const override {
        return _registry.get_subject_schema(sub, version);
    }

    ss::future<pandaproxy::schema_registry::context_schema_id> create_schema(
      pandaproxy::schema_registry::subject_schema unparsed) override {
        return _registry.create_schema(std::move(unparsed));
    }

    const std::vector<pandaproxy::schema_registry::stored_schema>& get_all() {
        return _registry.get_all();
    }

    size_t get_count(pandaproxy::schema_registry::schema_id id) {
        return _counts[id];
    }

    void reset_counts() { _counts.clear(); }

private:
    schema::fake_registry _registry{};
    absl::flat_hash_map<pandaproxy::schema_registry::schema_id, size_t>
      _counts{};
    mutable counting_store _store{_registry, _counts};
};

chunked_schema_cache make_schema_cache() {
    return chunked_schema_cache{chunked_schema_cache::cache_t::config{2, 1}};
}

std::unique_ptr<counting_registry> make_counting_sr() {
    auto sr = std::make_unique<counting_registry>();

    auto avro_schema_id = sr->create_schema(
                              subject_schema{
                                context_subject::unqualified("foo-value"),
                                schema_definition{
                                  avro_record_schema, schema_type::avro}})
                            .get();
    vassert(1 == avro_schema_id.id(), "failed to registry avro schema");
    auto pb_schema_id = sr->create_schema(
                            subject_schema{
                              context_subject::unqualified("foo-value"),
                              schema_definition{
                                pb_record_schema, schema_type::protobuf}})
                          .get();
    vassert(2 == pb_schema_id.id(), "failed to register protobuf schema");

    auto get_simple_schema = [](int i) {
        constexpr std::string_view schema_temp = R"(
        syntax = "proto2";
        message empty_message_{} {{}}
        )";

        return fmt::format(schema_temp, i);
    };
    for (auto i = 3; i < 10; i++) {
        auto pb_schema_id = sr->create_schema(
                                subject_schema{
                                  context_subject::unqualified("foo-value"),
                                  schema_definition{
                                    get_simple_schema(i),
                                    schema_type::protobuf}})
                              .get();
        vassert(i == pb_schema_id.id(), "failed to register protobuf schema");
    }
    auto json_schema_id = sr->create_schema(
                              subject_schema{
                                context_subject::unqualified("foo-value"),
                                schema_definition{
                                  json_record_schema, schema_type::json}})
                            .get();
    vassert(10 == json_schema_id.id(), "failed to register json schema");
    return sr;
}
} // namespace

TEST(CachedRecordSchemaResolverTest, TestProtobufSchemaCache) {
    // Kakfa magic byte + schema ID + pb offsets.
    std::vector<int32_t> pb_offsets{2, 0};
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto schema_cache = make_schema_cache();
    auto sr = make_counting_sr();
    auto resolver = record_schema_resolver(*sr, schema_cache);

    auto resolve_buffer_fn = [&](bool expect_sr_access) {
        sr->reset_counts();
        size_t expected_sr_count = expect_sr_access ? 1 : 0;

        auto res = resolver.resolve_buf_type(buf.copy()).get();
        ASSERT_FALSE(res.has_error());
        auto& resolved_buf = res.value();
        ASSERT_TRUE(resolved_buf.type.has_value());
        EXPECT_EQ(2, (*resolved_buf.type)->id.schema_id());
        ASSERT_EQ(
          sr->get_count((*resolved_buf.type)->id.schema_id), expected_sr_count);
        EXPECT_TRUE((*resolved_buf.type)->id.protobuf_offsets.has_value());
        EXPECT_EQ(
          (*resolved_buf.type)->id.protobuf_offsets.value(), pb_offsets);

        const auto expected_type = field_type{[] {
            auto expected_struct = struct_type{};
            expected_struct.fields.emplace_back(
              nested_field::create(
                1, "inner_label_1", field_required::no, string_type{}));
            expected_struct.fields.emplace_back(
              nested_field::create(
                2, "inner_number_1", field_required::no, int_type{}));
            return expected_struct;
        }()};
        EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    };

    // First access to a schema, should hit the schema registry.
    resolve_buffer_fn(true);
    // All accesses afterwards should be cache hits.
    resolve_buffer_fn(false);
}

TEST(CachedRecordSchemaResolverTest, TestAvroSchemaCache) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_dummy_body());

    auto schema_cache = make_schema_cache();
    auto sr = make_counting_sr();
    auto resolver = record_schema_resolver(*sr, schema_cache);

    auto resolve_buffer_fn = [&](bool expect_sr_access) {
        sr->reset_counts();
        size_t expected_sr_count = expect_sr_access ? 1 : 0;

        auto res = resolver.resolve_buf_type(buf.copy()).get();
        ASSERT_FALSE(res.has_error());
        auto& resolved_buf = res.value();
        ASSERT_TRUE(resolved_buf.type.has_value());
        EXPECT_EQ(1, (*resolved_buf.type)->id.schema_id());
        ASSERT_EQ(
          sr->get_count((*resolved_buf.type)->id.schema_id), expected_sr_count);
        EXPECT_FALSE((*resolved_buf.type)->id.protobuf_offsets.has_value());

        const auto expected_type = field_type{[] {
            auto expected_struct = struct_type{};
            expected_struct.fields.emplace_back(
              nested_field::create(
                0, "value", field_required::yes, long_type{}));
            expected_struct.fields.emplace_back(
              nested_field::create(0, "next", field_required::yes, int_type{}));
            return expected_struct;
        }()};
        EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    };

    // First access to a schema, should hit the schema registry.
    resolve_buffer_fn(true);
    // All accesses afterwards should be cache hits.
    resolve_buffer_fn(false);
}

TEST(CachedRecordSchemaResolverTest, TestJsonSchemaCache) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\12", 5);
    buf.append(generate_dummy_body());

    auto schema_cache = make_schema_cache();
    auto sr = make_counting_sr();
    auto resolver = record_schema_resolver(*sr, schema_cache);

    auto resolve_buffer_fn = [&](bool expect_sr_access) {
        sr->reset_counts();
        size_t expected_sr_count = expect_sr_access ? 1 : 0;

        auto res = resolver.resolve_buf_type(buf.copy()).get();
        ASSERT_FALSE(res.has_error());
        auto& resolved_buf = res.value();
        ASSERT_TRUE(resolved_buf.type.has_value());
        EXPECT_EQ(10, (*resolved_buf.type)->id.schema_id());
        ASSERT_EQ(
          sr->get_count((*resolved_buf.type)->id.schema_id), expected_sr_count);
        EXPECT_FALSE((*resolved_buf.type)->id.protobuf_offsets.has_value());

        const auto expected_type = field_type{[] {
            auto expected_struct = struct_type{};
            expected_struct.fields.emplace_back(
              nested_field::create(
                0, "json_next", field_required::no, long_type{}));
            expected_struct.fields.emplace_back(
              nested_field::create(
                0, "json_value", field_required::no, long_type{}));
            return expected_struct;
        }()};
        EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    };

    // First access to a schema, should hit the schema registry.
    resolve_buffer_fn(true);
    // All accesses afterwards should be cache hits.
    resolve_buffer_fn(false);
}

TEST(CachedRecordSchemaResolverTest, TestSchemaCacheEviction) {
    auto schema_cache = make_schema_cache();
    auto sr = make_counting_sr();
    auto resolver = record_schema_resolver(*sr, schema_cache);

    auto resolve_buffer_fn = [&](
                               bool expect_sr_access,
                               uint8_t schema_id,
                               const std::vector<int32_t>& pb_offsets,
                               const field_type& expected_type) {
        // Kakfa magic byte + schema ID + pb offsets.
        iobuf buf;
        buf.append("\0\0\0\0", 4);
        buf.append(&schema_id, 1);
        buf.append(encode_pb_offsets(pb_offsets));
        buf.append(generate_dummy_body());

        sr->reset_counts();
        size_t expected_sr_count = expect_sr_access ? 1 : 0;

        auto res = resolver.resolve_buf_type(buf.copy()).get();
        ASSERT_FALSE(res.has_error());
        auto& resolved_buf = res.value();
        ASSERT_TRUE(resolved_buf.type.has_value());
        EXPECT_EQ(schema_id, (*resolved_buf.type)->id.schema_id());
        ASSERT_EQ(
          sr->get_count((*resolved_buf.type)->id.schema_id), expected_sr_count);
        EXPECT_TRUE((*resolved_buf.type)->id.protobuf_offsets.has_value());
        EXPECT_EQ(
          (*resolved_buf.type)->id.protobuf_offsets.value(), pb_offsets);
        EXPECT_EQ((*resolved_buf.type)->type, expected_type);
    };

    const auto schema_2_expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(
            1, "inner_label_1", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(
            2, "inner_number_1", field_required::no, int_type{}));
        return expected_struct;
    }()};

    const auto schema_3_expected_type = field_type{struct_type{}};

    // First access to schema 2 should hit the schema registry.
    resolve_buffer_fn(true, 2, {2, 0}, schema_2_expected_type);
    // All accesses afterwards should be cache hits.
    resolve_buffer_fn(false, 2, {2, 0}, schema_2_expected_type);

    // Try to get schema from cache.
    auto cached_schema_opt = schema_cache.get_value(
      pandaproxy::schema_registry::schema_id{2});
    EXPECT_TRUE(cached_schema_opt);
    auto schema_buf = cached_schema_opt->get()->raw()().copy();

    // Fill the cache with other schemas. Note that each is accessed 3 times to
    // ensure that the main cache is filled.
    resolve_buffer_fn(true, 3, {0}, schema_3_expected_type);
    resolve_buffer_fn(false, 3, {0}, schema_3_expected_type);
    resolve_buffer_fn(false, 3, {0}, schema_3_expected_type);
    resolve_buffer_fn(true, 4, {0}, schema_3_expected_type);
    resolve_buffer_fn(false, 4, {0}, schema_3_expected_type);
    resolve_buffer_fn(false, 4, {0}, schema_3_expected_type);
    resolve_buffer_fn(true, 5, {0}, schema_3_expected_type);

    // Ensure schema 2 was evicted.
    EXPECT_FALSE(
      schema_cache.get_value(pandaproxy::schema_registry::schema_id{2}));
    // Ensure that the shared pointer to the evicted schema is still valid.
    EXPECT_EQ(cached_schema_opt->get()->raw()(), schema_buf);

    // Try to resolve a schema 2 formatted buffer again.
    resolve_buffer_fn(true, 2, {2, 0}, schema_2_expected_type);
    // Ensure that schema 2 is once again in the cache.
    resolve_buffer_fn(false, 2, {2, 0}, schema_2_expected_type);
}
