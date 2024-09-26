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

#include "base/units.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "schema/registry.h"
#include "test_utils/randoms.h"
#include "wasm/engine.h"
#include "wasm/logger.h"
#include "wasm/tests/wasm_logger.h"
#include "wasm/transform_probe.h"
#include "wasm/wasmtime.h"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/file.hh>

#include <absl/strings/ascii.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <unistd.h>

using namespace std::chrono_literals;

template<size_t BatchSize, size_t RecordSize>
class WasmBenchTest {
public:
    WasmBenchTest() { load("identity").get(); }
    WasmBenchTest(const WasmBenchTest&) = delete;
    WasmBenchTest(WasmBenchTest&&) = delete;
    WasmBenchTest& operator=(const WasmBenchTest&) = delete;
    WasmBenchTest& operator=(WasmBenchTest&&) = delete;
    ~WasmBenchTest() { cleanup().get(); }

    ss::future<> load(std::string_view file) {
        if (_engine) {
            co_await _engine->stop();
        }
        _engine = nullptr;
        if (!_runtime) {
            _runtime = wasm::wasmtime::create_runtime(nullptr);
            constexpr wasm::runtime::config wasm_runtime_config {
                .heap_memory = {
                  .per_core_pool_size_bytes = 64_MiB,
                  .per_engine_memory_limit = 64_MiB,
                },
                .stack_memory = {
                  .debug_host_stack_usage = false,
                },
                .cpu = {
                  .per_invocation_timeout = 30s,
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
        auto wasm_binary = model::wasm_binary_iobuf(std::make_unique<iobuf>());
        {
            auto path = fmt::format("{}.wasm", file);
            if (!(co_await ss::file_exists(path))) {
                auto bazel_env_var = fmt::format(
                  "{}_WASM_BINARY", absl::AsciiStrToUpper(file));
                path = std::getenv(bazel_env_var.c_str());
                vassert(!path.empty(), "expected {} to exist", bazel_env_var);
            }
            auto data = co_await ss::util::read_entire_file_contiguous(path);
            wasm_binary()->append(data.data(), data.size());
        }
        auto factory = co_await _runtime->make_factory(
          meta, std::move(wasm_binary));
        _engine = co_await factory->make_engine(
          std::make_unique<wasm_logger>(meta.name(), &wasm::wasm_log));
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
        auto output
          = ss::make_lw_shared<ss::chunked_fifo<model::transformed_data>>();
        return _engine
          ->transform(
            std::move(batch),
            &_probe,
            [output](auto, auto data) {
                output->push_back(std::move(data));
                return ssx::now(wasm::write_success::yes);
            })
          .then([output]() {
              perf_tests::do_not_optimize(model::transformed_data::make_batch(
                model::timestamp::now(), std::move(*output)));
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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define MEMSET_PERF_TEST(buf_size)                                             \
    PERF_TEST(Memset, Speed_##buf_size) {                                      \
        constexpr size_t size = buf_size;                                      \
        size_t pgsz = ::getpagesize();                                         \
        auto mem = ss::allocate_aligned_buffer<uint8_t>(                       \
          ss::align_up(size, pgsz), pgsz);                                     \
        std::memset(mem.get(), 0, size);                                       \
        perf_tests::do_not_optimize(mem);                                      \
    }

MEMSET_PERF_TEST(2_MiB);
MEMSET_PERF_TEST(10_MiB);
MEMSET_PERF_TEST(20_MiB);
MEMSET_PERF_TEST(30_MiB);
MEMSET_PERF_TEST(50_MiB);
MEMSET_PERF_TEST(80_MiB);
MEMSET_PERF_TEST(100_MiB);
