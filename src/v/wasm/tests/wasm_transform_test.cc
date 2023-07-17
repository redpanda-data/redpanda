/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "test_utils/fixture.h"
#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/testing/thread_test_case.hh>

#include <exception>

FIXTURE_TEST(test_wasm_transforms_work, wasm_test_fixture) {
    load_wasm("identity.wasm");
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    BOOST_CHECK_EQUAL(transformed.copy_records(), batch.copy_records());
    BOOST_CHECK_EQUAL(transformed, batch);
}

FIXTURE_TEST(test_wasm_engines_can_be_restarted, wasm_test_fixture) {
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
    BOOST_CHECK_EQUAL(transformed.copy_records(), batch.copy_records());
    BOOST_CHECK_EQUAL(transformed, batch);
}

FIXTURE_TEST(test_setup_panic, wasm_test_fixture) {
    BOOST_CHECK_EXCEPTION(
      load_wasm("setup-panic.wasm"),
      wasm::wasm_exception,
      [](const wasm::wasm_exception& ex) {
          std::cout << ex.error_code() << ":" << ex.what() << std::endl;
          return ex.error_code() == wasm::errc::user_code_failure;
      });
}

FIXTURE_TEST(test_transform_panic, wasm_test_fixture) {
    load_wasm("transform-panic.wasm");
    BOOST_CHECK_EXCEPTION(
      transform(make_tiny_batch()),
      wasm::wasm_exception,
      [](const wasm::wasm_exception& ex) {
          return ex.error_code() == wasm::errc::user_code_failure;
      });
}

FIXTURE_TEST(test_transform_error, wasm_test_fixture) {
    load_wasm("transform-error.wasm");
    BOOST_CHECK_EXCEPTION(
      transform(make_tiny_batch()),
      wasm::wasm_exception,
      [](const wasm::wasm_exception& ex) {
          return ex.error_code() == wasm::errc::user_code_failure;
      });
}
