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
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"
#include "wasm/api.h"
#include "wasm/probe.h"

#include <seastar/util/file.hh>

#include <fmt/chrono.h>

#include <memory>

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger dummy_logger("wasm_test_logger");
} // namespace

void WasmTestFixture::SetUp() {
    _probe = std::make_unique<wasm::transform_probe>();
    // TODO: Create a custom runtime so that we can test with schema registry
    _runtime = wasm::runtime::create_default(nullptr);
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
    }
    _factory = nullptr;
    _runtime = nullptr;
    _probe = nullptr;
}

void WasmTestFixture::load_wasm(const std::string& path) {
    auto wasm_file = ss::util::read_entire_file(path).get0();
    iobuf buf;
    for (auto& chunk : wasm_file) {
        buf.append(std::move(chunk));
    }
    _factory
      = _runtime->make_factory(_meta, std::move(buf), &dummy_logger).get();
    if (_engine) {
        _engine->stop().get();
    }
    _engine = _factory->make_engine().get();
    _engine->start().get();
    _engine->initialize().get();
}

model::record_batch WasmTestFixture::transform(const model::record_batch& b) {
    return _engine->transform(b.copy(), _probe.get()).get();
}
model::record_batch WasmTestFixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .timestamp = NOW,
    });
}
