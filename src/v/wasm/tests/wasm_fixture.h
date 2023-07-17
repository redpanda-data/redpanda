/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/types.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "ssx/thread_worker.h"
#include "wasm/api.h"
#include "wasm/probe.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/file.hh>

#include <memory>

class wasm_test_fixture {
public:
    static constexpr model::timestamp NOW = model::timestamp(1687201340524ULL);

    wasm_test_fixture();
    wasm_test_fixture(const wasm_test_fixture&) = delete;
    wasm_test_fixture& operator=(const wasm_test_fixture&) = delete;
    wasm_test_fixture(wasm_test_fixture&&) = delete;
    wasm_test_fixture& operator=(wasm_test_fixture&&) = delete;
    ~wasm_test_fixture();

    void load_wasm(const std::string& path);
    model::record_batch make_tiny_batch();
    model::record_batch transform(const model::record_batch&);

    cluster::transform_metadata meta() const { return _meta; };

    wasm::engine* engine() { return _engine.get(); }

private:
    ssx::thread_worker _worker;
    std::unique_ptr<wasm::runtime> _runtime;
    std::unique_ptr<wasm::factory> _factory;
    std::unique_ptr<wasm::engine> _engine;
    wasm::probe _probe;
    cluster::transform_metadata _meta;
};
