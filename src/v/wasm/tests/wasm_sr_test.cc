/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/bytes.h"
#include "bytes/streambuf.h"
#include "pandaproxy/schema_registry/types.h"
#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/reactor.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace {
std::string generate_example_avro_record(
  const pandaproxy::schema_registry::canonical_schema_definition& def) {
    // Generate a simple avro record that looks like this (as json):
    // {"a":5,"b":"foo"}
    iobuf_istream bis{def.shared_raw()};
    auto is = avro::istreamInputStream(bis.istream());
    auto schema = avro::compileJsonSchemaFromStream(*is);
    avro::GenericRecord r(schema.root());
    r.setFieldAt(r.fieldIndex("a"), int64_t(4));
    r.setFieldAt(r.fieldIndex("b"), std::string("foo"));

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, avro::GenericDatum(schema.root(), r));
    e->flush();
    auto snap = avro::snapshot(*out);
    return {snap->begin(), snap->end()};
}
} // namespace

TEST_F(WasmTestFixture, SchemaRegistry) {
    // Test an example schema registry encoded avro value -> JSON transform
    load_wasm("schema-registry");
    const auto& schemas = registered_schemas();
    ASSERT_EQ(schemas.size(), 1);
    ASSERT_EQ(schemas[0].id, 1);
    iobuf record_value;
    // Prepend the "magic" nul byte then the schema id in big endian
    record_value.append("\0\0\0\0\1", 5);
    auto avro = generate_example_avro_record(schemas[0].schema.def());
    record_value.append(avro.data(), avro.size());
    auto batch = make_tiny_batch(std::move(record_value));
    auto transformed = transform(batch);
    auto records = transformed.copy_records();
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(
      iobuf_to_bytes(records[0].value()),
      bytes::from_string(R"JSON({"a":4,"b":"foo"})JSON"));
}
