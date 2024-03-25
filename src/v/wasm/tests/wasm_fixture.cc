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
#include "storage/record_batch_builder.h"
#include "wasm/api.h"
#include "wasm/schema_registry.h"
#include "wasm/tests/wasm_fixture.h"
#include "wasm/tests/wasm_logger.h"
#include "wasm/wasmtime.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/util/file.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <memory>
#include <stdexcept>

using namespace std::chrono_literals;

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger dummy_logger("wasm_test_logger");

namespace ppsr = pandaproxy::schema_registry;

} // namespace

// this is a fake schema registry that works enough for the tests we need to do
// with wasm.
class fake_schema_registry : public wasm::schema_registry {
public:
    bool is_enabled() const override { return true; };

    ss::future<ppsr::canonical_schema_definition>
    get_schema_definition(ppsr::schema_id id) const override {
        for (const auto& s : _schemas) {
            if (s.id == id) {
                co_return s.schema.def();
            }
        }
        throw std::runtime_error("unknown schema id");
    }
    ss::future<ppsr::subject_schema> get_subject_schema(
      ppsr::subject sub,
      std::optional<ppsr::schema_version> version) const override {
        std::optional<ppsr::subject_schema> found;
        for (const auto& s : _schemas) {
            if (s.schema.sub() != sub) {
                continue;
            }
            if (version && *version != s.version) {
                continue;
            }
            if (found && found->version > s.version) {
                continue;
            }
            found = s;
        }
        co_return found.value();
    }

    ss::future<ppsr::schema_id>
    create_schema(ppsr::unparsed_schema unparsed) override {
        // This is wrong, but simple for our testing.
        for (const auto& s : _schemas) {
            if (s.schema.def().raw()() == unparsed.def().raw()()) {
                co_return s.id;
            }
        }
        auto version = ppsr::schema_version(0);
        for (const auto& s : _schemas) {
            if (s.schema.sub() == unparsed.sub()) {
                version = std::max(version, s.version);
            }
        }
        // TODO: validate references too
        _schemas.push_back({
          .schema = ppsr::canonical_schema(
            unparsed.sub(),
            ppsr::canonical_schema_definition(
              unparsed.def().raw(),
              unparsed.def().type(),
              unparsed.def().refs())),
          .version = version + 1,
          .id = ppsr::schema_id(int32_t(_schemas.size() + 1)),
          .deleted = ppsr::is_deleted::no,
        });
        co_return _schemas.back().id;
    }

    std::vector<ppsr::subject_schema> get_all() { return _schemas; }

private:
    std::vector<ppsr::subject_schema> _schemas;
};

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
    auto sr = std::make_unique<fake_schema_registry>();
    _sr = sr.get();
    _runtime = wasm::wasmtime::create_runtime(std::move(sr));
    // Support creating up to 4 instances in a test
    constexpr wasm::runtime::config wasm_runtime_config {
        .heap_memory = {
          .per_core_pool_size_bytes = MAX_MEMORY * 4,
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

void WasmTestFixture::load_wasm(const std::string& path) {
    auto wasm_file = ss::util::read_entire_file(path).get0();
    iobuf buf;
    for (auto& chunk : wasm_file) {
        buf.append(std::move(chunk));
    }
    _runtime->validate(buf.share(0, buf.size_bytes())).get();
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
std::vector<pandaproxy::schema_registry::subject_schema>
WasmTestFixture::registered_schemas() const {
    return _sr->get_all();
}
