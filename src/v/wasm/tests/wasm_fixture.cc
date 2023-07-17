/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "wasm/tests/wasm_fixture.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/randoms.h"
#include "model/timeout_clock.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/file.hh>

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,cert-err58-cpp)
static ss::logger dummy_logger("wasm_test_logger");
} // namespace

wasm_test_fixture::wasm_test_fixture()
  // TODO: Use a non-default runtime so we can fake schema registry
  : _runtime(wasm::runtime::create_default(&_worker, nullptr))
  , _engine(nullptr)
  , _meta(cluster::transform_metadata{
      .name = cluster::transform_name(ss::sstring("test_wasm_transform")),
      .input_topic = model::random_topic_namespace(),
      .output_topics = {model::random_topic_namespace()},
      .environment = {},
      .source_ptr = model::offset(0)}) {
    _worker.start().get();
    _probe.setup_metrics(_meta.name());
}
wasm_test_fixture::~wasm_test_fixture() {
    if (_engine) {
        _engine->stop().get();
    }
    _probe.clear_metrics();
    _worker.stop().get();
}

void wasm_test_fixture::load_wasm(const std::string& path) {
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

model::record_batch wasm_test_fixture::transform(const model::record_batch& b) {
    return _engine->transform(&b, &_probe).get();
}
model::record_batch wasm_test_fixture::make_tiny_batch() {
    return model::test::make_random_batch(model::test::record_batch_spec{
      .allow_compression = false,
      .count = 1,
      .timestamp = NOW,
    });
}
