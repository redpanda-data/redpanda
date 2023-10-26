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

#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "test_utils/randoms.h"
#include "units.h"
#include "wasm/api.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/schema_registry.h"
#include "wasm/wasmtime.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/file.hh>

template<size_t BatchSize, size_t RecordSize>
class WasmBenchTest {
public:
    WasmBenchTest() { load("identity.wasm").get(); }
    WasmBenchTest(const WasmBenchTest&) = delete;
    WasmBenchTest(WasmBenchTest&&) = delete;
    WasmBenchTest& operator=(const WasmBenchTest&) = delete;
    WasmBenchTest& operator=(WasmBenchTest&&) = delete;
    ~WasmBenchTest() { cleanup().get(); }

    ss::future<> load(std::string_view filename) {
        if (_engine) {
            co_await _engine->stop();
        }
        _engine = nullptr;
        if (!_runtime) {
            _runtime = wasm::wasmtime::create_runtime(nullptr);
            constexpr wasm::runtime::config wasm_runtime_config {
                .heap_memory = {
                  .per_core_pool_size_bytes = 20_MiB,
                  .per_engine_memory_limit = 20_MiB,
                },
                .stack_memory = {
                  .debug_host_stack_usage = false,
                },
            };
            co_await _runtime->start(wasm_runtime_config);
        }
        const model::transform_metadata meta = {
          .name = tests::random_named_string<model::transform_name>(),
          .input_topic = model::random_topic_namespace(),
          .output_topics = {model::random_topic_namespace()},
          .environment = {},
          .source_ptr = model::offset(0),
        };
        iobuf wasm_binary;
        {
            auto data = co_await ss::util::read_entire_file_contiguous(
              filename);
            wasm_binary.append(data.data(), data.size());
        }
        auto factory = co_await _runtime->make_factory(
          meta, std::move(wasm_binary), &wasm::wasm_log);
        _engine = co_await factory->make_engine();
        co_await _engine->start();
    }

    ss::future<> cleanup() {
        if (_engine) {
            co_await _engine->stop();
        }
        _engine = nullptr;
        co_await _runtime->stop();
        _runtime = nullptr;
    }

    ss::future<> run_test() {
        model::record_batch batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .allow_compression = false,
            .count = BatchSize,
            .record_sizes = std::vector<size_t>(BatchSize, RecordSize),
          });
        perf_tests::start_measuring_time();
        return _engine->transform(std::move(batch), &_probe).then([](auto) {
            perf_tests::stop_measuring_time();
        });
    }

private:
    ss::shared_ptr<wasm::engine> _engine;
    std::unique_ptr<wasm::runtime> _runtime;
    wasm::transform_probe _probe;
};

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define WASM_IDENTITY_PERF_TEST(batch_size, record_size)                       \
    class WasmBenchTest_BatchSize##batch_size##_RecordSize##record_size        \
      : public WasmBenchTest<batch_size, record_size> {};                      \
    PERF_TEST_F(                                                               \
      WasmBenchTest_BatchSize##batch_size##_RecordSize##record_size,           \
      IdentityTransform) {                                                     \
        return run_test();                                                     \
    }

WASM_IDENTITY_PERF_TEST(1, 1_KiB);
WASM_IDENTITY_PERF_TEST(10, 1_KiB);
WASM_IDENTITY_PERF_TEST(10, 512);
