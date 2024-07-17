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

#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/reactor.hh>

#include <absl/strings/str_cat.h>
#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST_F(WasmTestFixture, IdentityFunction) {
    load_wasm("identity.wasm");
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
}

TEST_F(WasmTestFixture, CanRestartEngine) {
    load_wasm("identity.wasm");
    engine()->stop().get();
    // It still works after being restarted
    engine()->start().get();
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
}

TEST_F(WasmTestFixture, HandlesSetupPanic) {
    EXPECT_THROW(load_wasm("setup-panic.wasm"), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, HandlesTransformPanic) {
    load_wasm("transform-panic.wasm");
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, HandlesTransformErrors) {
    load_wasm("transform-error.wasm");
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
    engine()->stop().get();
    engine()->start().get();
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
}

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
    load_wasm("schema-registry.wasm");
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
      iobuf_to_bytes(records[0].value()), R"JSON({"a":4,"b":"foo"})JSON");
}

TEST_F(WasmTestFixture, MemoryIsLimited) {
    load_wasm("dynamic.wasm");
    EXPECT_NO_THROW(execute_command("allocate", 5_KiB));
    EXPECT_THROW(execute_command("allocate", MAX_MEMORY), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, CpuIsLimited) {
    load_wasm("dynamic.wasm");
    // In reality, we don't want this to be over a few milliseconds, but in an
    // effort to prevent test flakiness on hosts with lots of concurrent tests
    // on shared vCPUs, we make this much higher.
    ss::engine().update_blocked_reactor_notify_ms(50ms);
    bool stalled = false;
    ss::engine().set_stall_detector_report_function(
      [&stalled] { stalled = true; });
    EXPECT_THROW(execute_command("loop", 0), wasm::wasm_exception);
    EXPECT_FALSE(stalled);
}

using ::testing::ElementsAre;

TEST_F(WasmTestFixture, LogsAreEmitted) {
    load_wasm("dynamic.wasm");
    ss::sstring msg = "foobar";
    auto value = execute_command("print", msg);
    auto bytes = iobuf_to_bytes(value);
    ss::sstring expected;
    // NOLINTNEXTLINE(*-reinterpret-cast)
    expected.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    EXPECT_THAT(log_lines(), ElementsAre(expected));
}

TEST_F(WasmTestFixture, WorksWithCpuProfiler) {
    bool original_enabled = ss::engine().get_cpu_profiler_enabled();
    std::chrono::nanoseconds original_period
      = ss::engine().get_cpu_profiler_period();
    ss::engine().set_cpu_profiler_enabled(true);
    ss::engine().set_cpu_profiler_period(100us);
    load_wasm("dynamic.wasm");
    EXPECT_THROW(execute_command("loop", 0), wasm::wasm_exception);
    ss::engine().set_cpu_profiler_enabled(original_enabled);
    ss::engine().set_cpu_profiler_period(original_period);
    std::vector<ss::cpu_profiler_trace> traces;
    ss::engine().profiler_results(traces);
    for (const auto& t : traces) {
        std::cout << t.user_backtrace << "\n";
    }
}
