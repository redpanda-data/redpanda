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

static constexpr size_t TIMEOUT_FOR_TEST_MS = 50;

model::record_batch get_record_batch(ss::temporary_buffer<char> value) {
    std::vector<model::record> records;
    iobuf serialize_record;

    iobuf raw_value;
    raw_value.append(std::move(value));

    static constexpr size_t zero_vint_size = vint::vint_size(0);
    auto size = sizeof(model::record_attributes::type)    // attributes
                + vint::vint_size(0)                      // timestamp delta
                + vint::vint_size(0)                      // offset_delta
                + vint::vint_size(0)                      // key size
                + 0                                       // key payload
                + vint::vint_size(raw_value.size_bytes()) // value size
                + raw_value.size_bytes() + zero_vint_size;

    model::record new_record(
      size, {}, 0, 0, -1, {}, raw_value.size_bytes(), std::move(raw_value), {});

    model::append_record_to_buffer(serialize_record, new_record);
    records.emplace_back(std::move(new_record));
    model::record_batch_header header;
    header.size_bytes = serialize_record.size_bytes()
                        + model::packed_record_batch_header_size;
    header.record_count = 1;
    header.crc = model::crc_record_batch(header, serialize_record);
    return model::record_batch(header, std::move(records));
}

ss::temporary_buffer<uint8_t> get_value(model::record_batch record_batch) {
    iobuf_const_parser parser(record_batch.data());
    auto record = model::parse_one_record_copy_from_buffer(parser);
    auto value
      = iobuf_const_parser(record.value()).read_bytes(record.value_size());
    return ss::temporary_buffer<uint8_t>(value.data(), value.size());
}

SEASTAR_THREAD_TEST_CASE(to_upper_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    iobuf js_code = read_fully("to_upper.js").get();
    script.init("to_upper", std::move(js_code), executor_wrapper.get_executor())
      .get();

    model::record r;

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    auto record_batch = get_record_batch(std::move(data));

    script.run(record_batch, executor_wrapper.get_executor()).get();

    auto res = get_value(std::move(record_batch));

    boost::to_upper(raw_data);

    ss::sstring result(reinterpret_cast<char*>(res.get_write()), res.size());
    BOOST_REQUIRE_EQUAL(raw_data, result);
}

SEASTAR_THREAD_TEST_CASE(sum_test) {
    executor_wrapper_for_test executor_wrapper;

    v8_engine::script script(100, TIMEOUT_FOR_TEST_MS);

    iobuf js_code = read_fully("sum.js").get();
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

        auto record_batch = get_record_batch(std::move(data));
        script.run(record_batch, executor_wrapper.get_executor()).get();
        auto res = get_value(std::move(record_batch));
        BOOST_REQUIRE_EQUAL(
          test_data.a + test_data.b,
          reinterpret_cast<const sum_data*>(res.get())->ans);
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

    iobuf js_code = read_fully("sum.js").get();
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

    iobuf js_code = read_fully("compile_timeout.js").get();
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

    iobuf js_code = read_fully("run_timeout.js").get();
    script
      .init("run_timeout", std::move(js_code), executor_wrapper.get_executor())
      .get();
    ss::temporary_buffer<char> empty_buf;
    auto record_batch = get_record_batch(std::move(empty_buf));

    BOOST_REQUIRE_EXCEPTION(
      script.run(record_batch, executor_wrapper.get_executor()).get();
      , v8_engine::script_exception, [](const v8_engine::script_exception& e) {
          return "Sript timeout" == std::string(e.what());
      });
}
