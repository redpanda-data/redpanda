/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "model/record_utils.h"
#include "seastarx.h"
#include "utils/file_io.h"
#include "v8_engine/environment.h"
#include "v8_engine/executor.h"
#include "v8_engine/record_batch_wrapper.h"
#include "v8_engine/script.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/test/unit_test.hpp>

class executor_wrapper_for_test {
public:
    executor_wrapper_for_test()
      : _executor(ss::engine().alien(), 1, ss::smp::count) {}

    ~executor_wrapper_for_test() { _executor.stop().get(); }

    v8_engine::executor& get_executor() { return _executor; }

private:
    v8_engine::executor _executor;
};

v8_engine::enviroment env;

static constexpr size_t TIMEOUT_FOR_TEST_MS = 1000;

std::vector<ss::temporary_buffer<uint8_t>>
get_values(model::record_batch record_batch) {
    std::vector<ss::temporary_buffer<uint8_t>> res;
    record_batch.for_each_record([&res](model::record r) {
        auto value = iobuf_const_parser(r.value()).read_bytes(r.value_size());
        res.emplace_back(
          ss::temporary_buffer<uint8_t>(value.data(), value.size()));
    });
    return res;
}

SEASTAR_THREAD_TEST_CASE(to_upper_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    auto js_code = read_fully("to_upper.js").get();
    script.init("to_upper", std::move(js_code), executor_wrapper.get_executor())
      .get();

    ss::sstring raw_data = "qwerty";
    v8_engine::record_wrapper record;
    record.value = ss::temporary_buffer<char>(raw_data.data(), raw_data.size());
    std::list<v8_engine::record_wrapper> l;
    l.emplace_back(std::move(record));
    auto batch = v8_engine::convert_to_record_batch(
      model::record_batch_header(), l);

    script.run(batch, executor_wrapper.get_executor()).get();
    auto value = get_values(std::move(batch));
    boost::to_upper(raw_data);

    ss::sstring result(
      reinterpret_cast<char*>(value[0].get_write()), value[0].size());
    BOOST_REQUIRE_EQUAL(raw_data, result);
}

SEASTAR_THREAD_TEST_CASE(sum_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    auto js_code = read_fully("sum.js").get();
    script.init("sum", std::move(js_code), executor_wrapper.get_executor())
      .get();

    struct sum_data {
        int32_t a;
        int32_t b;
        int32_t ans;
    };

    std::list<v8_engine::record_wrapper> l;
    for (int i = 0; i < 10; ++i) {
        sum_data test_data = {.a = i, .b = i + 1, .ans = -1};
        ss::temporary_buffer<char> data(
          reinterpret_cast<char*>(&test_data), sizeof(test_data));

        v8_engine::record_wrapper record;
        record.value = std::move(data);
        l.emplace_back(std::move(record));
    }
    auto batch = v8_engine::convert_to_record_batch(
      model::record_batch_header(), l);

    script.run(batch, executor_wrapper.get_executor()).get();

    auto res = get_values(std::move(batch));
    for (auto& value : res) {
        const sum_data* data = reinterpret_cast<const sum_data*>(value.get());
        BOOST_REQUIRE_EQUAL(data->a + data->b, data->ans);
    }
}

SEASTAR_THREAD_TEST_CASE(exception_in_compile_script_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    std::string error_script = "2lkb34poup3q94h";
    iobuf js_code;
    js_code.append(error_script.c_str(), error_script.size());
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

    auto js_code = read_fully("sum.js").get();
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

    auto js_code = read_fully("compile_timeout.js").get();
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

    auto js_code = read_fully("run_timeout.js").get();
    script
      .init("run_timeout", std::move(js_code), executor_wrapper.get_executor())
      .get();
    ss::temporary_buffer<char> empty_buf;

    std::list<v8_engine::record_wrapper> l;
    v8_engine::record_wrapper record;
    record.value = std::move(empty_buf);
    l.emplace_back(std::move(record));
    auto batch = v8_engine::convert_to_record_batch(
      model::record_batch_header(), l);

    BOOST_REQUIRE_EXCEPTION(
      script.run(batch, executor_wrapper.get_executor()).get();
      , v8_engine::script_exception, [](const v8_engine::script_exception& e) {
          return "Sript timeout" == std::string(e.what());
      });
}
