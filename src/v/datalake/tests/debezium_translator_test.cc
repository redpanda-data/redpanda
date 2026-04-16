/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/debezium_translator.h"
#include "datalake/record_schema_resolver.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "schema/tests/fake_registry.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;
using namespace pandaproxy::schema_registry;

namespace {

// Debezium envelope Avro schema with before/after/op/source/ts_ms.
// The inner table has two fields: "id" (key) and "name".
constexpr std::string_view debezium_envelope_schema = R"({
  "type": "record",
  "name": "Envelope",
  "namespace": "test.debezium",
  "fields": [
    {
      "name": "before",
      "type": ["null", {
        "type": "record",
        "name": "Value",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "name", "type": "string"}
        ]
      }],
      "default": null
    },
    {
      "name": "after",
      "type": ["null", "Value"],
      "default": null
    },
    {
      "name": "source",
      "type": {
        "type": "record",
        "name": "Source",
        "fields": [
          {"name": "connector", "type": "string"}
        ]
      }
    },
    {
      "name": "op",
      "type": "string"
    },
    {
      "name": "ts_ms",
      "type": ["null", "long"],
      "default": null
    }
  ]
})";

// Key schema matching the "id" field of the inner table.
constexpr std::string_view key_schema = R"({
  "type": "record",
  "name": "Key",
  "namespace": "test.debezium",
  "fields": [
    {"name": "id", "type": "int"}
  ]
})";

// Encode an Avro GenericDatum to iobuf with schema registry wire format
// (magic byte 0x00 + 4 byte big-endian schema id).
iobuf encode_avro_with_schema_id(
  const avro::GenericDatum& datum, int32_t schema_id) {
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, datum);
    encoder->flush();
    auto snap = avro::snapshot(*out);

    iobuf buf;
    buf.append("\0", 1);
    int32_t encoded_id = ss::cpu_to_be(schema_id);
    buf.append(reinterpret_cast<const uint8_t*>(&encoded_id), 4);
    buf.append(snap->data(), snap->size());
    return buf;
}

// Build a Debezium envelope datum with the given op, before, and after values.
avro::GenericDatum make_envelope_datum(
  const avro::ValidSchema& envelope_schema,
  std::string_view op,
  std::optional<std::pair<int32_t, std::string>> before,
  std::optional<std::pair<int32_t, std::string>> after) {
    avro::GenericDatum datum(envelope_schema);
    auto& envelope = datum.value<avro::GenericRecord>();

    // before: union [null, Value]
    auto& before_datum = envelope.field("before");
    if (before.has_value()) {
        before_datum.selectBranch(1);
        auto& before_rec = before_datum.value<avro::GenericRecord>();
        before_rec.field("id").value<int32_t>() = before->first;
        before_rec.field("name").value<std::string>() = before->second;
    } else {
        before_datum.selectBranch(0);
    }

    // after: union [null, Value]
    auto& after_datum = envelope.field("after");
    if (after.has_value()) {
        after_datum.selectBranch(1);
        auto& after_rec = after_datum.value<avro::GenericRecord>();
        after_rec.field("id").value<int32_t>() = after->first;
        after_rec.field("name").value<std::string>() = after->second;
    } else {
        after_datum.selectBranch(0);
    }

    // source
    auto& source = envelope.field("source").value<avro::GenericRecord>();
    source.field("connector").value<std::string>() = "mysql";

    // op
    envelope.field("op").value<std::string>() = std::string(op);

    // ts_ms: union [null, long]
    auto& ts_ms = envelope.field("ts_ms");
    ts_ms.selectBranch(1);
    ts_ms.value<int64_t>() = 1234567890;

    return datum;
}

avro::GenericDatum
make_key_datum(const avro::ValidSchema& key_avro_schema, int32_t id) {
    avro::GenericDatum datum(key_avro_schema);
    auto& rec = datum.value<avro::GenericRecord>();
    rec.field("id").value<int32_t>() = id;
    return datum;
}

} // namespace

class DebeziumTranslatorTest : public ::testing::Test {
public:
    DebeziumTranslatorTest()
      : sr(std::make_unique<schema::fake_registry>())
      , key_resolver(*sr, std::nullopt, std::nullopt)
      , val_resolver(*sr, std::nullopt, std::nullopt) {}

    void SetUp() override {
        // Register envelope schema (id=1).
        auto envelope_id = sr->create_schema(
                               subject_schema{
                                 context_subject::unqualified("test-value"),
                                 schema_definition{
                                   debezium_envelope_schema,
                                   schema_type::avro}})
                             .get();
        ASSERT_EQ(1, envelope_id.id());

        // Register key schema (id=2).
        auto k_id = sr->create_schema(
                        subject_schema{
                          context_subject::unqualified("test-key"),
                          schema_definition{key_schema, schema_type::avro}})
                      .get();
        ASSERT_EQ(2, k_id.id());

        envelope_avro = avro::compileJsonSchemaFromString(
          std::string(debezium_envelope_schema));
        key_avro = avro::compileJsonSchemaFromString(std::string(key_schema));
    }

    // Resolve the envelope type from a raw iobuf (with schema ID prefix).
    shared_resolved_type_t resolve_envelope(iobuf buf) {
        auto res = val_resolver.resolve_buf_type(std::move(buf)).get();
        EXPECT_TRUE(res.has_value());
        auto& tb = res.value();
        EXPECT_TRUE(tb.type.has_value());
        return tb.type.value();
    }

    iobuf encode_envelope(
      std::string_view op,
      std::optional<std::pair<int32_t, std::string>> before,
      std::optional<std::pair<int32_t, std::string>> after) {
        auto datum = make_envelope_datum(envelope_avro, op, before, after);
        return encode_avro_with_schema_id(datum, 1);
    }

    iobuf encode_key(int32_t id) {
        auto datum = make_key_datum(key_avro, id);
        return encode_avro_with_schema_id(datum, 2);
    }

    // Strip the schema ID prefix to get the parsable buffer, mimicking what
    // resolve_buf_type returns.
    std::pair<shared_resolved_type_t, iobuf>
    resolve_and_strip(iobuf envelope_buf) {
        auto res = val_resolver.resolve_buf_type(std::move(envelope_buf)).get();
        EXPECT_TRUE(res.has_value());
        auto& tb = res.value();
        EXPECT_TRUE(tb.type.has_value());
        return {tb.type.value(), std::move(tb.parsable_buf.value())};
    }

    std::unique_ptr<schema::fake_registry> sr;
    record_schema_resolver key_resolver;
    record_schema_resolver val_resolver;
    avro::ValidSchema envelope_avro;
    avro::ValidSchema key_avro;

    static constexpr model::partition_id pid{0};
    static constexpr kafka::offset offset{42};
    static constexpr model::timestamp ts{1000};
    static constexpr model::timestamp_type ts_t{
      model::timestamp_type::create_time};
    chunked_vector<model::record_header> headers;
};

TEST_F(DebeziumTranslatorTest, CreateOp) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("c", std::nullopt, {{1, "Alice"}});
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));
    auto key_buf = encode_key(1);

    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::move(key_buf),
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_value());
    auto& rec = result.value();
    EXPECT_TRUE(rec.data_row.has_value());
    EXPECT_FALSE(rec.delete_key.has_value());
}

TEST_F(DebeziumTranslatorTest, ReadOp) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("r", std::nullopt, {{2, "Bob"}});
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));
    auto key_buf = encode_key(2);

    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::move(key_buf),
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_value());
    auto& rec = result.value();
    EXPECT_TRUE(rec.data_row.has_value());
    EXPECT_FALSE(rec.delete_key.has_value());
}

TEST_F(DebeziumTranslatorTest, UpdateOp) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope(
      "u", {{1, "Alice"}}, {{1, "Alice Updated"}});
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));
    auto key_buf = encode_key(1);

    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::move(key_buf),
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_value());
    auto& rec = result.value();
    EXPECT_TRUE(rec.data_row.has_value());
    EXPECT_TRUE(rec.delete_key.has_value());
}

TEST_F(DebeziumTranslatorTest, DeleteOp) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("d", {{1, "Alice"}}, std::nullopt);
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));
    auto key_buf = encode_key(1);

    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::move(key_buf),
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_value());
    auto& rec = result.value();
    EXPECT_FALSE(rec.data_row.has_value());
    EXPECT_TRUE(rec.delete_key.has_value());
}

TEST_F(DebeziumTranslatorTest, TombstoneReturnsNullopt) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("c", std::nullopt, {{1, "Alice"}});
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));

    // Tombstone: parsable_val is nullopt.
    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::nullopt,
                      val_type,
                      std::nullopt,
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_value());
    auto& rec = result.value();
    EXPECT_FALSE(rec.data_row.has_value());
    EXPECT_FALSE(rec.delete_key.has_value());
}

TEST_F(DebeziumTranslatorTest, NullKeyReturnsError) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("c", std::nullopt, {{1, "Alice"}});
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));

    // Null key with non-null value should error.
    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::nullopt,
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), record_translator::errc::translation_error);
}

TEST_F(DebeziumTranslatorTest, UnknownOpReturnsError) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("t", std::nullopt, std::nullopt);
    auto [val_type, parsable] = resolve_and_strip(std::move(envelope_buf));
    auto key_buf = encode_key(1);

    auto result = translator
                    .translate_data(
                      pid,
                      offset,
                      std::move(key_buf),
                      val_type,
                      std::move(parsable),
                      ts,
                      ts_t,
                      headers)
                    .get();

    ASSERT_TRUE(result.has_error());
    EXPECT_EQ(result.error(), record_translator::errc::translation_error);
}

TEST_F(DebeziumTranslatorTest, BuildTypeExtractsAfterFields) {
    debezium_translator translator(key_resolver);

    auto envelope_buf = encode_envelope("c", std::nullopt, {{1, "Alice"}});
    auto val_type = resolve_envelope(std::move(envelope_buf));

    auto rt = translator.build_type(val_type);

    // The result should have the redpanda system struct plus the inner table
    // fields (id, name).
    EXPECT_EQ(rt.comps.val_identifier.has_value(), true);
    // redpanda struct + id + name = 3 top-level fields
    EXPECT_EQ(rt.type.fields.size(), 3);
    EXPECT_EQ(rt.type.fields[0]->name, "redpanda");
    EXPECT_EQ(rt.type.fields[1]->name, "id");
    EXPECT_EQ(rt.type.fields[2]->name, "name");
}
