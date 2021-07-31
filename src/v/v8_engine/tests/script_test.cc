/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "seastarx.h"
#include "utils/file_io.h"
#include "v8_engine/environment.h"
#include "v8_engine/executor.h"
#include "v8_engine/script.h"

#include <seastar/core/smp.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/test/unit_test.hpp>

class executor_wrapper_for_test {
public:
    executor_wrapper_for_test()
      : _executor(1, ss::smp::count) {}

    ~executor_wrapper_for_test() { _executor.stop().get(); }

    v8_engine::executor& get_executor() { return _executor; }

private:
    v8_engine::executor _executor;
};

v8_engine::enviroment env;

static constexpr size_t TIMEOUT_FOR_TEST_MS = 50;

SEASTAR_THREAD_TEST_CASE(to_upper_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    ss::temporary_buffer<char> js_code = read_fully_tmpbuf("to_upper.js").get();
    script.init("to_upper", std::move(js_code), executor_wrapper.get_executor())
      .get();

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    script.run(data.share(), executor_wrapper.get_executor()).get();
    boost::to_upper(raw_data);
    auto res = std::string(data.get_write(), data.size());
    BOOST_REQUIRE_EQUAL(raw_data, res);
}

SEASTAR_THREAD_TEST_CASE(sum_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    ss::temporary_buffer<char> js_code = read_fully_tmpbuf("sum.js").get();
    script.init("sum", std::move(js_code), executor_wrapper.get_executor())
      .get();

    struct sum_data {
        int32_t a;
        int32_t b;
        int32_t ans;
    };

    for (int i = 0; i < 100; ++i) {
        sum_data test_data = {.a = i, .b = i + 1, .ans = -1};
        ss::temporary_buffer<char> data(
          reinterpret_cast<char*>(&test_data), sizeof(test_data));
        script.run(data.share(), executor_wrapper.get_executor()).get();
        BOOST_REQUIRE_EQUAL(
          test_data.a + test_data.b,
          reinterpret_cast<const sum_data*>(data.get())->ans);
    }
}

SEASTAR_THREAD_TEST_CASE(exception_in_compile_script_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    std::string error_script = "2lkb34poup3q94h";
    ss::temporary_buffer<char> js_code(
      error_script.data(), error_script.size());
    BOOST_REQUIRE_EXCEPTION(
      script.init("sum", std::move(js_code), executor_wrapper.get_executor())
        .get(),
      v8_engine::script_exception,
      [](const v8_engine::script_exception& e) {
          return "Can not compile script:SyntaxError: Invalid or unexpected "
                 "token"
                 == std::string(e.what());
      });
}

SEASTAR_THREAD_TEST_CASE(exception_in_get_script_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    ss::temporary_buffer<char> js_code = read_fully_tmpbuf("sum.js").get();
    BOOST_REQUIRE_EXCEPTION(
      script.init("foo", std::move(js_code), executor_wrapper.get_executor())
        .get(),
      v8_engine::script_exception,
      [](const v8_engine::script_exception& e) {
          return "Can not get function(foo) from script"
                 == std::string(e.what());
      });
}

SEASTAR_THREAD_TEST_CASE(compile_timeout_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    ss::temporary_buffer<char> js_code
      = read_fully_tmpbuf("compile_timeout.js").get();
    BOOST_REQUIRE_EXCEPTION(
      script.init("foo", std::move(js_code), executor_wrapper.get_executor())
        .get(),
      v8_engine::script_exception,
      [](const v8_engine::script_exception& e) {
          return "Can not run script in first time(timeout)"
                 == std::string(e.what());
      });
}

SEASTAR_THREAD_TEST_CASE(run_timeout_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    ss::temporary_buffer<char> js_code
      = read_fully_tmpbuf("run_timeout.js").get();
    script
      .init("run_timeout", std::move(js_code), executor_wrapper.get_executor())
      .get();
    ss::temporary_buffer<char> empty_buf;

    BOOST_REQUIRE_EXCEPTION(
      script.run(empty_buf.share(), executor_wrapper.get_executor()).get();
      , v8_engine::script_exception, [](const v8_engine::script_exception& e) {
          return "Sript timeout" == std::string(e.what());
      });
}
