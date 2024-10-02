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

#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/reactor.hh>

#include <gtest/gtest.h>

TEST_F(WasmTestFixture, CanRestartEngine) {
    load_wasm("identity");
    engine()->stop().get();
    // It still works after being restarted
    engine()->start().get();
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
}

TEST_F(WasmTestFixture, HandlesSetupPanic) {
    EXPECT_THROW(load_wasm("setup-panic"), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, HandlesTransformPanic) {
    load_wasm("transform-panic");
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, HandlesTransformErrors) {
    load_wasm("transform-error");
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
    engine()->stop().get();
    engine()->start().get();
    EXPECT_THROW(transform(make_tiny_batch()), wasm::wasm_exception);
}
