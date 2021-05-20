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
#include "v8_engine/environment.h"
#include "v8_engine/instance.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/test/unit_test.hpp>

#include <cstdint>

const std::string_view to_upper_case_js = "function to_upper(obj) {\
                                            let array = new Int8Array(obj);\
                                                for (let i = 0; i < obj.byteLength; i++) {\
                                                    if (array[i] >= 97 && array[i] <= 122) {\
      	                                                array[i] = array[i] - 32;\
                                                    } else {\
                                                        array[i] = array[i];\
                                                    }\
                                                }\
                                            }";

const std::string_view sum_js = "function sum(obj) {\
                                    let array = new Int32Array(obj);\
                                    array[2] = array[0] + array[1];\
                                }";

v8_engine::enviroment env;

SEASTAR_THREAD_TEST_CASE(to_upper_test) {
    v8_engine::instance inst(100);

    ss::temporary_buffer<char> js_code(
      to_upper_case_js.data(), to_upper_case_js.size());
    inst.init("to_upper", std::move(js_code)).get();

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    inst.run(data).get();
    boost::to_upper(raw_data);
    auto res = std::string(data.get_write(), data.size());
    BOOST_REQUIRE_EQUAL(raw_data, res);
}

SEASTAR_THREAD_TEST_CASE(sum_test) {
    v8_engine::instance inst(100);

    ss::temporary_buffer<char> js_code(sum_js.data(), sum_js.size());
    inst.init("sum", std::move(js_code)).get();

    struct sum_data {
        int32_t a;
        int32_t b;
        int32_t ans;
    };

    for (int i = 0; i < 100; ++i) {
        sum_data test_data = {.a = i, .b = i + 1, .ans = -1};
        ss::temporary_buffer<char> data(
          reinterpret_cast<char*>(&test_data), sizeof(test_data));
        inst.run(data).get();
        BOOST_REQUIRE_EQUAL(
          test_data.a + test_data.b,
          reinterpret_cast<const sum_data*>(data.get())->ans);
    }
}

SEASTAR_THREAD_TEST_CASE(exception_in_compile_script_test) {
    v8_engine::instance inst(100);

    std::string error_script = "2lkb34poup3q94h";
    ss::temporary_buffer<char> js_code(
      error_script.data(), error_script.size());
    BOOST_REQUIRE_EXCEPTION(
      inst.init("sum", std::move(js_code)).get(),
      v8_engine::instance_exception,
      [](const v8_engine::instance_exception& e) {
          return "Can not compile script:SyntaxError: Invalid or unexpected "
                 "token"
                 == std::string(e.what());
      });
}

SEASTAR_THREAD_TEST_CASE(exception_in_get_script_test) {
    v8_engine::instance inst(100);
    ss::temporary_buffer<char> js_code(sum_js.data(), sum_js.size());
    BOOST_REQUIRE_EXCEPTION(
      inst.init("foo", std::move(js_code)).get(),
      v8_engine::instance_exception,
      [](const v8_engine::instance_exception& e) {
          return "Can not get function(foo) from script"
                 == std::string(e.what());
      });
}
