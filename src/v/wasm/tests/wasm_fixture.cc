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

#include "wasm/tests/wasm_fixture.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/registry.h"
#include "schema/tests/fake_registry.h"
#include "storage/record_batch_builder.h"
#include "wasm/engine.h"
#include "wasm/tests/wasm_fixture.h"
#include "wasm/tests/wasm_logger.h"
#include "wasm/wasmtime.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/util/file.hh>

#include <absl/strings/ascii.h>
#include <fmt/chrono.h>

#include <chrono>
#include <memory>
#include <stdexcept>

using namespace std::chrono_literals;

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger dummy_logger("wasm_test_logger");
} // namespace

void WasmTestFixture::SetUpTestSuite() {
    // This is a bit of a hack to set the signal set for the ss::async thread
    // that runs the tests.
    // In debug mode (only) ss::thread uses swapcontext to switch stacks,
    // and swapcontext *also* keeps track of the signalset and swaps those.
    // so depending on the running context we can get the signals that wasmtime
    // needs can be blocked, as these signals are blocked by default and we only
    // unblock the signals on the reactor threads in wasm::runtime::start
    //
    // In release mode, longjmp and setjmp is used instead of swapcontext, so
    // there is nothing that resets the signalset.
    auto mask = ss::make_empty_sigset_mask();
    sigaddset(&mask, SIGSEGV);
    sigaddset(&mask, SIGILL);
    sigaddset(&mask, SIGFPE);
    ss::throw_pthread_error(::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
}

void WasmTestFixture::SetUp() {
    _probe = std::make_unique<wasm::transform_probe>();
    auto sr = std::make_unique<schema::fake_registry>();
    _sr = sr.get();
    _runtime = wasm::wasmtime::create_runtime(std::move(sr));
    // Support creating up to 4 instances in a test
    constexpr wasm::runtime::config wasm_runtime_config {
        .heap_memory = {
          .per_core_pool_size_bytes = MAX_MEMORY,
          .per_engine_memory_limit = MAX_MEMORY,
        },
        .stack_memory = {
#ifdef NDEBUG
          // Only turn this on if ASAN is off.
          // With ASAN on, we get issues because we haven't told
          // ASAN that the stack has switched (as this happens within
          // wasmtime and we don't have the ability to instrument rust 
          // with ASAN checks).
          .debug_host_stack_usage = true,
#else
          .debug_host_stack_usage = false,
#endif
        },
        .cpu = {
          .per_invocation_timeout = 3s,
        },
    };
    _runtime->start(wasm_runtime_config).get();
    _meta = {
      .name = model::transform_name(ss::sstring("test_wasm_transform")),
      .input_topic = model::random_topic_namespace(),
      .output_topics = {model::random_topic_namespace()},
      .environment = {},
      .source_ptr = model::offset(0),
    };
}
void WasmTestFixture::TearDown() {
    if (_engine) {
        _engine->stop().get();
        _log_lines.clear();
    }
    _engine = nullptr;
    _factory = nullptr;
    _runtime->stop().get();
    _runtime = nullptr;
    _probe = nullptr;
}

void WasmTestFixture::load_wasm(std::string file) {
    std::string path = fmt::format("{}.wasm", file);
    if (!ss::file_exists(path).get()) {
        auto bazel_env_var = fmt::format(
          "{}_WASM_BINARY", absl::AsciiStrToUpper(file));
        const char* path_envvar = std::getenv(bazel_env_var.c_str());
        vassert(path_envvar != nullptr, "expected {} to exist", bazel_env_var);
        path = path_envvar;
    }
    auto wasm_file = ss::util::read_entire_file(path).get();
    auto buf = model::wasm_binary_iobuf(std::make_unique<iobuf>());
    for (auto& chunk : wasm_file) {
        buf()->append(std::move(chunk));
    }

    _runtime->validate(model::share_wasm_binary(buf)).get();
    _factory = _runtime->make_factory(_meta, std::move(buf)).get();
    if (_engine) {
        _engine->stop().get();
        _log_lines.clear();
    }
    auto logger = std::make_unique<capturing_logger>(
      [this](ss::log_level, std::string_view log) {
          _log_lines.emplace_back(log);
      });
    _engine = _factory->make_engine(std::move(logger)).get();
    _engine->start().get();
}

model::record_batch WasmTestFixture::transform(const model::record_batch& b) {
    ss::chunked_fifo<model::transformed_data> transformed;
    _engine
      ->transform(
        b.copy(),
        _probe.get(),
        [&transformed](auto, model::transformed_data data) {
            transformed.push_back(std::move(data));
            return ss::make_ready_future<wasm::write_success>(
              wasm::write_success::yes);
        })
      .get();
    return model::transformed_data::make_batch(
      model::timestamp::now(), std::move(transformed));
}
model::record_batch WasmTestFixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .timestamp = NOW,
    });
}
model::record_batch WasmTestFixture::make_tiny_batch(iobuf record_value) {
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset(1));
    b.add_raw_kv(model::test::make_iobuf(), std::move(record_value));
    return std::move(b).build();
}
const std::vector<pandaproxy::schema_registry::subject_schema>&
WasmTestFixture::registered_schemas() const {
    return _sr->get_all();
}
