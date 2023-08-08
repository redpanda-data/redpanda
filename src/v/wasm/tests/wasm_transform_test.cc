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

#include "test_utils/fixture.h"
#include "test_utils/test.h"
#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

TEST_F_ASYNC(WasmTestFixture, IdentityFunction) {
    load_wasm("identity.wasm");
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
    ASSERT_EQ(transformed, batch);
}

TEST_F_ASYNC(WasmTestFixture, CanRestartEngine) {
    load_wasm("identity.wasm");
    engine()->stop().get();
    // Can be restarted without initialization
    engine()->start().get();
    engine()->stop().get();
    // It still works after being restarted
    engine()->start().get();
    engine()->initialize().get();
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
    ASSERT_EQ(transformed, batch);
}

// TODO(rockwood): Enable these tests after #12322 is merged and we can build
// this in CMake
/*
TEST_F_ASYNC(WasmTestFixture, wasm_test_fixture) {
    EXPECT_THROW(
      load_wasm("setup-panic.wasm"),
      wasm::wasm_exception);
}

TEST_F_ASYNC(WasmTestFixture, wasm_test_fixture) {
    load_wasm("transform-panic.wasm");
    EXPECT_THROW(
      transform(make_tiny_batch()),
      wasm::wasm_exception);
}

TEST_F_ASYNC(WasmTestFixture, wasm_test_fixture) {
    load_wasm("transform-error.wasm");
    EXPECT_THROW(
      transform(make_tiny_batch()),
      wasm::wasm_exception);
}
*/

TEST_F_ASYNC(WasmTestFixture, CanComputeMemoryUsage) {
    load_wasm("identity.wasm");
    ASSERT_GT(engine()->memory_usage_size_bytes(), 0);
}
